# test_client.py
from server import Client

def decode_obj(o):
    if isinstance(o, bytes):
        try:
            return o.decode('utf-8')
        except:
            return o
    if isinstance(o, list):
        return [decode_obj(x) for x in o]
    if isinstance(o, dict):
        return {decode_obj(k): decode_obj(v) for k, v in o.items()}
    return o

c = Client()
print("mset ->", c.mset('k1', 'v1', 'k2', ['v2-0', 1, 'v2-2'], 'k3', 'v3'))
print("get k2 ->", decode_obj(c.get('k2')))
print("mget k3,k1 ->", [decode_obj(x) for x in c.mget('k3', 'k1')])
print("delete k1 ->", c.delete('k1'))
print("flush ->", c.flush())
