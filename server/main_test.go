package main

import (
	"errors"
	"testing"

	pb "github.com/lduseja9/zmq-fifo-buffer/common/proto"
	"github.com/lduseja9/zmq-fifo-buffer/server/interfaces"

	zmq "github.com/go-zeromq/zmq4"
	"google.golang.org/protobuf/proto"
)

// --- Mocks ---

// mockBuffer implements the PushPullBuffer interface and records every interaction so
// tests can assert on what the handler actually called.
type mockBuffer struct {
	pushedItems []string
	pullResult  interfaces.PullResult
	sizeResult  int
}

func (m *mockBuffer) Push(item string) {
	m.pushedItems = append(m.pushedItems, item)
}

func (m *mockBuffer) Pull() interfaces.PullResult {
	return m.pullResult
}

func (m *mockBuffer) Size() int {
	return m.sizeResult
}

// mockSocket implements the BuffSocket interface and captures every message
// sent so tests can inspect the serialised response bytes.
type mockSocket struct {
	sent []zmq.Msg
	err  error // if non-nil, Send returns this error
}

func (m *mockSocket) Send(msg zmq.Msg) error {
	if m.err != nil {
		return m.err
	}
	m.sent = append(m.sent, msg)
	return nil
}

// sentResponse unmarshals the first captured message back into a Response.
func (m *mockSocket) sentResponse(t *testing.T) *pb.Response {
	t.Helper()
	if len(m.sent) == 0 {
		t.Fatal("mockSocket: no message was sent")
	}
	resp := &pb.Response{}
	if err := proto.Unmarshal(m.sent[0].Frames[0], resp); err != nil {
		t.Fatalf("mockSocket: failed to unmarshal response: %v", err)
	}
	return resp
}

// --- requestHandler tests ---

func TestRequestHandlerPushCallsBufferAndReturnsSuccess(t *testing.T) {
	buf := &mockBuffer{}
	resp := requestHandler(buf, &pb.Request{
		Operation: pb.Operation_PUSH_QUEUE,
		Data:      "hello",
	})

	if resp.Status != pb.Status_Success {
		t.Errorf("expected Success, got %v", resp.Status)
	}
	if len(buf.pushedItems) != 1 || buf.pushedItems[0] != "hello" {
		t.Errorf("expected Push(%q) to be called once, got %v", "hello", buf.pushedItems)
	}
}

func TestRequestHandlerPushResponseCarriesNoData(t *testing.T) {
	buf := &mockBuffer{}
	resp := requestHandler(buf, &pb.Request{
		Operation: pb.Operation_PUSH_QUEUE,
		Data:      "hello",
	})

	if resp.Data != "" {
		t.Errorf("push response should carry no data, got %q", resp.Data)
	}
	if resp.Size != 0 {
		t.Errorf("push response should carry no size, got %d", resp.Size)
	}
}

func TestRequestHandlerPullReturnsItemOnSuccess(t *testing.T) {
	buf := &mockBuffer{pullResult: interfaces.PullResult{Item: "world", Ok: true}}
	resp := requestHandler(buf, &pb.Request{Operation: pb.Operation_PULL_QUEUE})

	if resp.Status != pb.Status_Success {
		t.Errorf("expected Success, got %v", resp.Status)
	}
	if resp.Data != "world" {
		t.Errorf("expected %q, got %q", "world", resp.Data)
	}
}

func TestRequestHandlerPullReturnsEmptyStatusWhenBufferIsEmpty(t *testing.T) {
	buf := &mockBuffer{pullResult: interfaces.PullResult{Ok: false}}
	resp := requestHandler(buf, &pb.Request{Operation: pb.Operation_PULL_QUEUE})

	if resp.Status != pb.Status_Empty {
		t.Errorf("expected Empty, got %v", resp.Status)
	}
}

func TestRequestHandlerSizeReturnsCurrentCount(t *testing.T) {
	buf := &mockBuffer{sizeResult: 7}
	resp := requestHandler(buf, &pb.Request{Operation: pb.Operation_SIZE_QUEUE})

	if resp.Status != pb.Status_Success {
		t.Errorf("expected Success, got %v", resp.Status)
	}
	if resp.Size != 7 {
		t.Errorf("expected size 7, got %d", resp.Size)
	}
}

func TestRequestHandlerSizeOnEmptyBufferReturnsZero(t *testing.T) {
	buf := &mockBuffer{sizeResult: 0}
	resp := requestHandler(buf, &pb.Request{Operation: pb.Operation_SIZE_QUEUE})

	if resp.Status != pb.Status_Success {
		t.Errorf("expected Success, got %v", resp.Status)
	}
	if resp.Size != 0 {
		t.Errorf("expected size 0, got %d", resp.Size)
	}
}

func TestRequestHandlerUnknownOperationReturnsFailure(t *testing.T) {
	buf := &mockBuffer{}
	resp := requestHandler(buf, &pb.Request{Operation: pb.Operation(99)})

	if resp.Status != pb.Status_Failed {
		t.Errorf("expected Failed, got %v", resp.Status)
	}
}

func TestRequestHandlerUnknownOperationDoesNotTouchBuffer(t *testing.T) {
	buf := &mockBuffer{}
	requestHandler(buf, &pb.Request{Operation: pb.Operation(99)})

	if len(buf.pushedItems) != 0 {
		t.Errorf("expected no Push calls for unknown operation, got %v", buf.pushedItems)
	}
}

// --- sendResponse tests ---

func TestSendResponseMarshalsPushSuccessResponse(t *testing.T) {
	sender := &mockSocket{}
	sendResponse(sender, &pb.Response{Status: pb.Status_Success})

	resp := sender.sentResponse(t)
	if resp.Status != pb.Status_Success {
		t.Errorf("expected Success, got %v", resp.Status)
	}
}

func TestSendResponseMarshalsPullResponse(t *testing.T) {
	sender := &mockSocket{}
	sendResponse(sender, &pb.Response{Status: pb.Status_Success, Data: "item"})

	resp := sender.sentResponse(t)
	if resp.Status != pb.Status_Success {
		t.Errorf("expected Success, got %v", resp.Status)
	}
	if resp.Data != "item" {
		t.Errorf("expected %q, got %q", "item", resp.Data)
	}
}

func TestSendResponseMarshalsEmptyStatusResponse(t *testing.T) {
	sender := &mockSocket{}
	sendResponse(sender, &pb.Response{Status: pb.Status_Empty})

	resp := sender.sentResponse(t)
	if resp.Status != pb.Status_Empty {
		t.Errorf("expected Empty, got %v", resp.Status)
	}
}

func TestSendResponseMarshalsSizeResponse(t *testing.T) {
	sender := &mockSocket{}
	sendResponse(sender, &pb.Response{Status: pb.Status_Success, Size: 42})

	resp := sender.sentResponse(t)
	if resp.Size != 42 {
		t.Errorf("expected size 42, got %d", resp.Size)
	}
}

func TestSendResponseSendsExactlyOneMessage(t *testing.T) {
	sender := &mockSocket{}
	sendResponse(sender, &pb.Response{Status: pb.Status_Success})

	if len(sender.sent) != 1 {
		t.Errorf("expected exactly 1 message sent, got %d", len(sender.sent))
	}
}

func TestSendResponseSilentlyAbortsOnSendError(t *testing.T) {
	sender := &mockSocket{err: errors.New("socket closed")}
	// must not panic
	sendResponse(sender, &pb.Response{Status: pb.Status_Success})

	if len(sender.sent) != 0 {
		t.Errorf("expected no message recorded on send error, got %d", len(sender.sent))
	}
}
