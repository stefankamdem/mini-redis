# server.py
from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
from collections import namedtuple
from io import BytesIO
import logging

logger = logging.getLogger(__name__)

class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))

class ProtocolHandler:
    def __init__(self):
        self.handlers = {
            '+': self.handle_simple_string,
            '-': self.handle_error,
            ':': self.handle_integer,
            '$': self.handle_string,
            '*': self.handle_array,
            '%': self.handle_dict,
        }

    def read_line_bytes(self, fh):
        line = fh.readline()
        if not line:
            raise Disconnect()
        if not line.endswith(b'\r\n'):
            raise CommandError('Invalid line ending')
        return line[:-2]  # bytes without CRLF

    def handle_request(self, fh):
        first = fh.read(1)
        if not first:
            raise Disconnect()
        first = first.decode('ascii')
        try:
            return self.handlers[first](fh)
        except KeyError:
            raise CommandError('bad request')

    def handle_simple_string(self, fh):
        return self.read_line_bytes(fh).decode('utf-8')

    def handle_error(self, fh):
        return Error(self.read_line_bytes(fh).decode('utf-8'))

    def handle_integer(self, fh):
        return int(self.read_line_bytes(fh).decode('ascii'))

    def handle_string(self, fh):
        length_line = self.read_line_bytes(fh).decode('ascii')
        length = int(length_line)
        if length == -1:
            return None
        data = fh.read(length + 2)
        if not data.endswith(b'\r\n'):
            raise CommandError('Invalid bulk string termination')
        return data[:-2]  # bytes (binary-safe)

    def handle_array(self, fh):
        num = int(self.read_line_bytes(fh).decode('ascii'))
        return [self.handle_request(fh) for _ in range(num)]

    def handle_dict(self, fh):
        num = int(self.read_line_bytes(fh).decode('ascii'))
        elements = [self.handle_request(fh) for _ in range(num * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, fh, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        fh.write(buf.getvalue())
        fh.flush()

    def _write(self, buf, data):
        # Normalise Python str -> bytes
        if isinstance(data, str):
            b = data.encode('utf-8')
            buf.write(b'$' + str(len(b)).encode('ascii') + b'\r\n' + b + b'\r\n')
        elif isinstance(data, bytes):
            buf.write(b'$' + str(len(data)).encode('ascii') + b'\r\n' + data + b'\r\n')
        elif isinstance(data, int):
            buf.write(b':' + str(data).encode('ascii') + b'\r\n')
        elif isinstance(data, Error):
            buf.write(b'-' + data.message.encode('utf-8') + b'\r\n')
        elif isinstance(data, (list, tuple)):
            buf.write(b'*' + str(len(data)).encode('ascii') + b'\r\n')
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%' + str(len(data)).encode('ascii') + b'\r\n')
            for k, v in data.items():
                self._write(buf, k)
                self._write(buf, v)
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            raise CommandError('unrecognized type: %s' % type(data))

class Server:
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer((host, port), self.connection_handler, spawn=self._pool)
        self._protocol = ProtocolHandler()
        self._kv = {}
        self._commands = self.get_commands()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset,
        }

    def connection_handler(self, conn, address):
        logger.info('Connection received: %s:%s' % address)
        socket_file = conn.makefile('rwb')
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                logger.info('Client disconnected: %s:%s' % address)
                break
            try:
                resp = self.get_response(data)
            except CommandError as exc:
                logger.exception('Command error')
                resp = Error(str(exc))
            self._protocol.write_response(socket_file, resp)

    def run(self):
        self._server.serve_forever()

    def get_response(self, data):
        if not isinstance(data, list):
            if isinstance(data, bytes):
                try:
                    s = data.decode('utf-8')
                except Exception:
                    raise CommandError('Request must be list or simple string.')
                data = s.split()
            elif isinstance(data, str):
                data = data.split()
            else:
                raise CommandError('Request must be list or simple string.')

        if not data:
            raise CommandError('Missing command')

        def norm(x):
            if isinstance(x, bytes):
                try:
                    return x.decode('utf-8')
                except Exception:
                    return x
            return x

        command = norm(data[0]).upper()
        args = [norm(x) for x in data[1:]]
        if command not in self._commands:
            raise CommandError('Unrecognized command: %s' % command)
        return self._commands[command](*args)

    # Command implementations
    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        if len(items) % 2 != 0:
            raise CommandError('MSET requires even number of arguments')
        pairs = list(zip(items[::2], items[1::2]))
        for k, v in pairs:
            self._kv[k] = v
        return len(pairs)

class Client:
    def __init__(self, host='127.0.0.1', port=31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp

    def get(self, key): return self.execute('GET', key)
    def set(self, key, value): return self.execute('SET', key, value)
    def delete(self, key): return self.execute('DELETE', key)
    def flush(self): return self.execute('FLUSH')
    def mget(self, *keys): return self.execute('MGET', *keys)
    def mset(self, *items): return self.execute('MSET', *items)

if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    Server().run()
