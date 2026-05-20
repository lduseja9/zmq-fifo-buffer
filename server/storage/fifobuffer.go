package storage

import "fmt"

type PullResult struct {
	Item string
	Ok   bool
}

type FifoBuffer struct {
	pushChannel chan string
	pullChannel chan chan PullResult
	sizeChannel chan chan int
	stopChannel chan struct{}
	buffer      []string
}

func NewFifoBuffer() *FifoBuffer {
	return &FifoBuffer{
		pushChannel: make(chan string),
		pullChannel: make(chan chan PullResult),
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
				pullCh <- PullResult{Item: fb.buffer[0], Ok: true}
				fb.buffer = fb.buffer[1:]
			} else {
				pullCh <- PullResult{Item: "", Ok: false}
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

func (fb *FifoBuffer) Pull() PullResult {
	pullCh := make(chan PullResult)
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
