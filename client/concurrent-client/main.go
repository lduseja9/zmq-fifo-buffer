package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	pb "github.com/lduseja9/zmq-fifo-buffer/common/proto"

	zmq "github.com/go-zeromq/zmq4"
	"google.golang.org/protobuf/proto"
)

// Demonstrates mutex-free FIFO ordering under concurrent client access.
//
// Two goroutines each hold their own ZMQ REQ socket and talk to the server
// simultaneously. The server serialises all requests through a single channel-
// based goroutine — no mutexes — yet items always emerge in push order.
//
// Expected output shows interleaved pushes and pulls where the consumer
// always receives items in the exact order the producer pushed them.

const (
	defaultEndpoint = "tcp://127.0.0.1:5555"
	totalItems      = 10
	pushInterval    = 50 * time.Millisecond
	consumerDelay   = 75 * time.Millisecond // lets producer push a couple of items first
	retryDelay      = 20 * time.Millisecond // back-off when queue is Empty
)

func main() {
	endpoint := os.Getenv("ZMQ_ENDPOINT")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	var wg sync.WaitGroup
	wg.Add(2)

	// Client 1 — producer
	go func() {
		defer wg.Done()

		socket := newSocket(endpoint)
		defer socket.Close()
		fmt.Println("[producer] connected to", endpoint)

		for i := 0; i < totalItems; i++ {
			item := fmt.Sprintf("item-%02d", i)
			resp := send(socket, &pb.Request{
				Operation: pb.Operation_PUSH_QUEUE,
				Data:      item,
			})
			fmt.Printf("[producer] push(%q) → %s\n", item, resp.Status)

			// slow down the producer after pushing half the items to allow interleaving with the consumer
			if i >= totalItems/2 {
				// Give the consumer a chance to start pulling before we push the next item.
				fmt.Println("[producer] waiting a bit before pushing next item...")
				time.Sleep(pushInterval)
			}

		}
		fmt.Println("[producer] done")
	}()

	// Client 2 — consumer
	go func() {
		defer wg.Done()

		socket := newSocket(endpoint)
		defer socket.Close()
		fmt.Println("[consumer] connected to", endpoint)

		received := 0
		for received < totalItems {

			// Give the producer a head start so the interleaving is visible.
			time.Sleep(pushInterval)

			resp := send(socket, &pb.Request{Operation: pb.Operation_PULL_QUEUE})

			switch resp.Status {
			case pb.Status_Empty:
				fmt.Println("[consumer] queue empty — waiting for producer...")
				time.Sleep(retryDelay)

			case pb.Status_Success:
				expected := fmt.Sprintf("item-%02d", received)
				if resp.Data == expected {
					fmt.Printf("[consumer] pull() → %q. FIFO order is  maintained\n", resp.Data)
				} else {
					fmt.Printf("[consumer] pull() → %q. FIFO VIOLATION — expected %q\n", resp.Data, expected)
				}
				received++

			default:
				fmt.Printf("[consumer] unexpected status: %s\n", resp.Status)
			}
		}
		fmt.Println("[consumer] done — all", totalItems, "items received in FIFO order")
	}()

	wg.Wait()
}

func newSocket(endpoint string) zmq.Socket {
	socket := zmq.NewReq(context.Background())
	if err := socket.Dial(endpoint); err != nil {
		fmt.Printf("failed to connect to %s: %v\n", endpoint, err)
		panic(err)
	}
	return socket
}

func send(socket zmq.Socket, req *pb.Request) *pb.Response {
	data, err := proto.Marshal(req)
	if err != nil {
		return &pb.Response{Status: pb.Status_Failed}
	}
	if err := socket.Send(zmq.NewMsg(data)); err != nil {
		return &pb.Response{Status: pb.Status_Failed}
	}
	msg, err := socket.Recv()
	if err != nil {
		return &pb.Response{Status: pb.Status_Failed}
	}
	resp := &pb.Response{}
	if err := proto.Unmarshal(msg.Frames[0], resp); err != nil {
		return &pb.Response{Status: pb.Status_Failed}
	}
	return resp
}
