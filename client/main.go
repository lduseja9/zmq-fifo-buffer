package main

import (
	"context"
	"fmt"

	pb "github.com/lduseja9/zmq-fifo-buffer/common/proto"

	zmq "github.com/go-zeromq/zmq4"
	"google.golang.org/protobuf/proto"
)

const endpoint = "tcp://127.0.0.1:5555"

func main() {
	ctx := context.Background()
	// set up a ZeroMQ REP socket to listen for incoming messages
	socket := zmq.NewReq(ctx)
	defer socket.Close()

	if err := socket.Dial(endpoint); err != nil {
		fmt.Printf("Failed to connect to %s: %v\n", endpoint, err)
		panic(err)
	}
	fmt.Println("Connected to FIFO server at: ", endpoint)

	// Push 5 items.
	fmt.Println("Pushing 5 items......")
	for i := 0; i < 5; i++ {
		item := fmt.Sprintf("item-%d", i)
		resp := sendPush(socket, item)
		fmt.Printf("  push(%q) → %s\n", item, resp.Status)
	}
	fmt.Println()

	fmt.Println("Current size after 5 pushes....")
	sizeResponse := sendSize(socket)
	fmt.Printf("  size() → %d\n", sizeResponse.Size)
	fmt.Println()

	fmt.Println("Pulling 2 items....")
	for i := 0; i < 2; i++ {
		resp := sendPull(socket)
		fmt.Printf("  pull() → %q (status: %s)\n", resp.Data, resp.Status)
	}
	fmt.Println()

	fmt.Println("Current size after 2 pulls....")
	sizeResponse = sendSize(socket)
	fmt.Printf("  size() → %d\n", sizeResponse.Size)
	fmt.Println()

	fmt.Println("Pulling remaining items until queue is empty....")
	for {
		resp := sendPull(socket)
		fmt.Printf("  pull() → %q (status: %s)\n", resp.Data, resp.Status)
		if resp.Status == pb.Status_Empty {
			break
		}
	}
	fmt.Println()
}

func sendPush(socket zmq.Socket, item string) *pb.Response {
	// Send the item as a push message to the server
	return send(
		socket,
		&pb.Request{
			Operation: pb.Operation_PUSH_QUEUE,
			Data:      item,
		},
	)
}

func sendPull(socket zmq.Socket) *pb.Response {
	// Send a pull message to the server
	return send(
		socket,
		&pb.Request{
			Operation: pb.Operation_PULL_QUEUE,
		},
	)
}

func sendSize(socket zmq.Socket) *pb.Response {
	// Send a size message to the server
	return send(
		socket,
		&pb.Request{
			Operation: pb.Operation_SIZE_QUEUE,
		},
	)
}

func send(socket zmq.Socket, pushMessage *pb.Request) *pb.Response {
	wireMessage, err := proto.Marshal(pushMessage)
	if err != nil {
		fmt.Printf("Error marshaling message: %v\n", err)
		return &pb.Response{Status: pb.Status_Failed}
	}

	if err := socket.Send(zmq.NewMsg(wireMessage)); err != nil {
		fmt.Printf("Error sending message: %v\n", err)
		return &pb.Response{Status: pb.Status_Failed}
	}

	wireResponse, err := socket.Recv()
	if err != nil {
		fmt.Printf("Error receiving response: %v\n", err)
		return &pb.Response{Status: pb.Status_Failed}
	}

	pushResponse := &pb.Response{}
	if err := proto.Unmarshal(wireResponse.Frames[0], pushResponse); err != nil {
		fmt.Printf("Error unmarshaling response: %v\n", err)
		return &pb.Response{Status: pb.Status_Failed}
	}

	return pushResponse
}
