# baradb-redis

This repo is a Redis implementation based on [baradb](https://github.com/saint-yellow/baradb), a Bitcask K/V storage engine.

It provides a service on the top of Bitcask K/V storage engine. With the service, it could be easy to support Redis data types and implement Redis commands.

## Install

```shell 
$ go install github.com/saint-yellow/baradb-redis@latest 
```

This will add a binary file called baradb-redis to you $GOPATH/bin 

## Usage

It is similar to using Redis. Start the server and use a Redis client to connect to the server.

Suppose $GOPATH/bin/baradb-redis is in you $PATH, you can use it like this:

```
$ baradb-redis &
server running, ready to accept connections 

$ redis-cli -p 6378
127.0.0.1:6378> set name "saint-yellow"
OK
127.0.0.1:6378> get name
"saint-yellow"
127.0.0.1:6378>
```

## Roadmap 

1. Support `String`, `Hash`, `Set`, `ZSet`, `List`. 
2. Implement commands of data types mentioned above.

## Chagelog 

### v0.1.2 - 2023-07-11
Implemented commands: 
    - Generic: `DEL`, `TYPE`, `EXISTS`
    - String: `SET`, `GET`, `SETNX`, `STRLEN`
    - List: `LLEN`, `LPUSH`, `RPUSH`
    - Set: `SADD`, `SISMEMBER`, `SMEMBERS`, `SCARD`

## Contributing

PRs accepted. 

## License

MIT © Saint-Yellow 
