# Mini-redis

A lightweight, simplified implementation of Redis built for learning and experimentation.
This project demonstrates core Redis concepts such as in-memory data storage, persistence, and basic network communication.

## Features
<ul>
  <li>In-memory key-value data store</li>
  <li>Basic commands: SET, GET, DEL, EXISTS</li>
  <li>Simple TCP server/lient architecture</li>
  <li>Thread-safe operations</li>
  <li>Easy to extend with new commands</li>
</ul>

## Installation

Clone the repository:
```bash
git clone https://github.com/stefankamdem/mini-redis.git
cd mini-redis
```

If the project uses Python:
```bash
pip install -r requirements
python main.py
```

## Usage
Run the server:
```bash
python server.py
```

## Example Output:
```pgsql
> SET name Stefan
OK
> GET name
Stefan
```

Then connect with a client or via telnet:
```bash
telnet localhost 6379
SET mykey hello
GET mykey
```

## Contributions
Contributions, issues, and feature requests are welcome!
Feel free to open a pull request or issue on GitHub.
