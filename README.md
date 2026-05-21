# zmq-fifo-buffer

A mutex-free, concurrent FIFO buffer server written in Go. Clients interact with the server over [ZeroMQ](https://zeromq.org/) using [Protocol Buffers](https://protobuf.dev/) for message serialisation. The buffer itself is implemented without any mutexes — all shared state is managed by a single dedicated goroutine that serialises access through Go channels.

## Repository structure

The repo root contains the following:
zmq-fifo-buffer/
  * common - Shared Protobuf schema and generated Go bindings
  * server - FIFO buffer server
  * client - Demo client application
  * Dockerfile.server
  * Dockerfile.client
  * docker-compose.yml

## How to build the different components

### `common` — shared Protobuf definitions

To regenerate the Go bindings after editing the `.proto` file:

```bash
# Install the protoc Go plugin (once)
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Change to 'common' directory
cd common

# Run from 'common' directory
protoc --proto_path=. --go_out=. --go_opt=module=github.com/lduseja9/zmq-fifo-buffer/common/proto/message.proto
```

---

### `server` — FIFO buffer server

The server binds a ZeroMQ **REP** socket and processes one request at a time in a request–reply loop.

**How the mutex-free design works**

`FifoBuffer` owns its internal slice exclusively inside a single goroutine started by `Start()`. All public methods (`Push`, `Pull`, `Size`) communicate with that goroutine through unbuffered channels, so callers block only until the goroutine processes their request. There are no mutexes, no locks, and no shared memory.

**Configuration**

   * It needs an environment variable `ZMQ_ENDPOINT`. THis is the address the server listen on.
   * The deffault value of `ZMQ_ENDPOINT` is  `tcp://0.0.0.0:5555`


#### Build the server


```bash
cd server
go build -o server .
```


---

### `client` — demo client

#### A simple client
A short-lived program that connects to the server and demonstrates all three operations:

1. Pushes 5 items (`item-0` … `item-4`)
2. Checks the queue size
3. Pulls 2 items
4. Checks the size again
5. Drains the remaining items until the server returns `Empty`

#### Build the simple client

```bash
cd client
go build -o client .
```

#### A Concurrent client
A short-lived program that runs 2 clients on different goroutines. One pushes values on the fifo buffer (aka producer 
thread) and the other pulls the values from the fifo buffer (aka consumer thread). Both the threads have appropriate 
amount of sleep time to show interleaving of values.

#### Build the concurrent client

```bash
cd client/concurrent-client
go build -o concurrent-client.exe .
```

---

## Running the clients 

#### Run the simple client
This should show simple push, pull, and size operations on the fifo buffer

```bash
cd client
./client
```

#### Run the concurrent client
This should demonstrate the fifo nature of the buffere despite the interleaving of pushes and pulls on the server by two clients

```bash
cd client/concurrent-client
./concurrent-client.exe
```

## Running the unit tests

All tests live in the `server` module. Run them from the `server` directory.

### Run all server tests

```bash
cd server
go test ./... -v
```

---

## Docker

Both Dockerfiles use a two-stage build: a `golang:1.24-alpine` builder stage compiles the binary, and an `alpine:3.19` runtime stage copies only the resulting binary, keeping the final image small.

The build context for both Dockerfiles is the **repo root**, because each component depends on the `common` module.

### Build the server image

```bash
docker build -f Dockerfile.server -t zmq-fifo-server:latest .
```

### Build the client image

```bash
docker build -f Dockerfile.client -t zmq-fifo-client:latest .
```

### Run the server image directly

```bash
docker run --rm -p 5555:5555 zmq-fifo-server:latest
```

---

## Docker Compose

`docker-compose.yml` defines both services and wires them together.

### Start both services

```bash
docker compose up
```

### Start only the server

```bash
docker compose up server
```

### Rebuild images before starting

```bash
docker compose up --build
```

### Stop and remove containers

```bash
docker compose down
```
