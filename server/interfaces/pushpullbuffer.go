package interfaces

type PullResult struct {
	Item string
	Ok   bool
}

// PushPullBuffer is the interface for the FIFO buffer, allowing Push, Pull, and Size operations
type PushPullBuffer interface {
	Push(item string)
	Pull() PullResult
	Size() int
}
