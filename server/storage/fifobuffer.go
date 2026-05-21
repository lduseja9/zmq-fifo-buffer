package storage

import (
	"fmt"

	"github.com/lduseja9/zmq-fifo-buffer/server/interfaces"
)

type FifoBuffer struct {
	pushChannel chan string
	pullChannel chan chan interfaces.PullResult
	sizeChannel chan chan int
	stopChannel chan struct{}
	buffer      []string
}

func NewFifoBuffer() *FifoBuffer {
	return &FifoBuffer{
		pushChannel: make(chan string),
		pullChannel: make(chan chan interfaces.PullResult),
		sizeChannel: make(chan chan int),
		stopChannel: make(chan struct{}),
		buffer:      make([]string, 0),
	}
}

func (fb *FifoBuffer) Start() {
	for {
		select {
		case item := <-fb.pushChannel:
			fb.buffer = append(fb.buffer, item)
		case pullCh := <-fb.pullChannel:
			if len(fb.buffer) > 0 {
				pullCh <- interfaces.PullResult{Item: fb.buffer[0], Ok: true}
				fb.buffer = fb.buffer[1:]
			} else {
				pullCh <- interfaces.PullResult{Item: "", Ok: false}
			}
		case sizeCh := <-fb.sizeChannel:
			sizeCh <- len(fb.buffer)
		case <-fb.stopChannel:
			return
		}
	}
}

func (fb *FifoBuffer) Push(item string) {
	fb.pushChannel <- item
}

func (fb *FifoBuffer) Pull() interfaces.PullResult {
	pullCh := make(chan interfaces.PullResult)
	fb.pullChannel <- pullCh
	return <-pullCh
}

func (fb *FifoBuffer) Size() int {
	sizeCh := make(chan int)
	fb.sizeChannel <- sizeCh
	return <-sizeCh
}

func (fb *FifoBuffer) Clear() {
	fb.buffer = make([]string, 0)
}

func (fb *FifoBuffer) Stop() {
	fb.stopChannel <- struct{}{}
}

func (fb *FifoBuffer) Close() {
	fmt.Println("Closing FIFO buffer...")
	fb.Clear()
	fb.Stop()
	close(fb.pushChannel)
	close(fb.pullChannel)
	close(fb.sizeChannel)
	close(fb.stopChannel)
}
