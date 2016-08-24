package indexer

type Semaphore chan bool

func NewSemaphore(n int) Semaphore {
	return make(Semaphore, n)
}

func (s Semaphore) acquire() {
	s <- true
}

func (s Semaphore) release() {
	<-s
}
