package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/lduseja9/zmq-fifo-buffer/common/proto"
	"github.com/lduseja9/zmq-fifo-buffer/server/interfaces"
	"github.com/lduseja9/zmq-fifo-buffer/server/storage"

	zmq "github.com/go-zeromq/zmq4"
	"google.golang.org/protobuf/proto"
)

const defaultEndpoint = "tcp://0.0.0.0:5555"

func main() {
	endpoint := os.Getenv("ZMQ_ENDPOINT")
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fifoBuffer := storage.NewFifoBuffer()
	defer fifoBuffer.Close()
	go fifoBuffer.Start()

	// set up a ZeroMQ REP socket to listen for incoming messages
	socket := zmq.NewRep(ctx)
	defer socket.Close()

	// Bind the socket to the specified endpoint
	if err := socket.Listen(endpoint); err != nil {
		fmt.Printf("Failed to listen on %s: %v\n", endpoint, err)
		panic(err)
	}
	fmt.Println("FIFO server listening on: ", endpoint)

	// Handle graceful shutdown on interrupt signals
	// SIGINT - triggered by Ctrl+C
	// SIGTERM - standard termination signal by OS or process managers like systemd, Docker or Kubernetes
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		fmt.Println("Shutting down server...")
		cancel()
	}()

	// Main loop to receive and respond to messages
	for {
		msg, err := socket.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				fmt.Println("Server stopped.")
				return
			default:
				fmt.Printf("Error receiving message: %v\n", err)
				continue
			}
		}

		fmt.Printf("Received message: %s\n", string(msg.Bytes()))

		wireRequest := &pb.Request{}
		if err := proto.Unmarshal(msg.Frames[0], wireRequest); err != nil {
			sendResponse(socket, &pb.Response{Status: pb.Status_Failed})
			continue
		}

		reponsePayload := requestHandler(fifoBuffer, wireRequest)
		sendResponse(socket, reponsePayload)
	}
}

func requestHandler(buf interfaces.PushPullBuffer, request *pb.Request) *pb.Response {
	fmt.Printf("Handling request: Operation=%s, Data=%q\n", request.Operation.String(), request.Data)

	switch request.Operation {
	case pb.Operation_PUSH_QUEUE:
		fmt.Printf("Pushing item to queue: %q\n", request.Data)
		buf.Push(request.Data)
		return &pb.Response{Status: pb.Status_Success}
	case pb.Operation_PULL_QUEUE:
		result := buf.Pull()
		if result.Ok {
			return &pb.Response{Status: pb.Status_Success, Data: result.Item}
		} else {
			return &pb.Response{Status: pb.Status_Empty}
		}
	case pb.Operation_SIZE_QUEUE:
		size := buf.Size()
		return &pb.Response{Status: pb.Status_Success, Size: int64(size)}
	default:
		fmt.Printf("Unknown operation: %s\n", request.Operation.String())
		return &pb.Response{Status: pb.Status_Failed}
	}
}

func sendResponse(buffSocket interfaces.BuffSocket, response *pb.Response) {
	wireResponse, err := proto.Marshal(response)
	if err != nil {
		fmt.Printf("Error marshaling response: %v\n", err)
		return
	}

	if err := buffSocket.Send(zmq.NewMsg(wireResponse)); err != nil {
		fmt.Printf("Error sending response: %v\n", err)
		return
	}
}
