package naming

type Queue struct {
	data []any
	size int
	cap  int
	head int
}

func NewQueue() *Queue {
	return &Queue{
		data: make([]any, 16),
		size: 0,
		cap:  16,
		head: 0,
	}
}

func (q *Queue) Enqueue(elem any) {
	if q.size+1 > q.cap {
		newData := make([]any, q.cap*2)
		for i := 0; i < q.size; i++ {
			newData[i] = q.data[(q.head+i)%q.cap]
		}
		q.cap *= 2
		q.data = newData
		q.head = 0
	}
	q.data[(q.head+q.size)%q.cap] = elem
	q.size++
}

func (q *Queue) Peek() any {
	if q.size == 0 {
		return nil
	}
	return q.data[q.head]
}

func (q *Queue) Dequeue() any {
	if q.size == 0 {
		return nil
	}
	elem := q.data[q.head]
	q.head++
	q.size--
	if q.head >= q.cap {
		q.head -= q.cap
	}
	return elem
}

func (q *Queue) Empty() bool {
	return q.size == 0
}

type empty struct{}
type lockRequest struct {
	readonly bool
	granted  chan empty
}
type FIFORWMutex struct {
	rLock   chan chan empty
	wLock   chan chan empty
	rUnlock chan empty
	wUnlock chan empty
	quit    chan empty
}

func NewFIFORWMutex() *FIFORWMutex {
	lock := FIFORWMutex{
		rLock:   make(chan chan empty),
		wLock:   make(chan chan empty),
		rUnlock: make(chan empty),
		wUnlock: make(chan empty),
		quit:    make(chan empty),
	}
	go lock.scheduler()
	return &lock
}

func (lock *FIFORWMutex) RLock() {
	granted := make(chan empty)
	lock.rLock <- granted
	<-granted
}

func (lock *FIFORWMutex) RUnlock() {
	lock.rUnlock <- empty{}
}

func (lock *FIFORWMutex) Lock() {
	granted := make(chan empty)
	lock.wLock <- granted
	<-granted
}

func (lock *FIFORWMutex) Unlock() {
	lock.wUnlock <- empty{}
}

func (lock *FIFORWMutex) Destroy() {
	lock.quit <- empty{}
}

func (lock *FIFORWMutex) scheduler() {
	queue := NewQueue()
	nReading := 0
	writing := false

loop:
	for {
		select {
		case granted := <-lock.rLock:
			if queue.Empty() && !writing {
				// can read immediately without queuing
				nReading++
				granted <- empty{}
				continue loop
			}
			request := lockRequest{
				readonly: true,
				granted:  granted,
			}
			queue.Enqueue(request)

		case granted := <-lock.wLock:
			if queue.Empty() && nReading == 0 && !writing {
				// can write immediately without queuing
				writing = true
				granted <- empty{}
				continue loop
			}
			request := lockRequest{
				readonly: false,
				granted:  granted,
			}
			queue.Enqueue(request)

		case <-lock.rUnlock:
			nReading--
			if nReading == 0 && !queue.Empty() {
				// grant lock to the next request
				// it must be a write request
				request := queue.Dequeue().(lockRequest)
				writing = true
				request.granted <- empty{}
			}

		case <-lock.wUnlock:
			writing = false
			if !queue.Empty() {
				// grant lock to the next request
				request := queue.Dequeue().(lockRequest)
				if request.readonly {
					nReading++
				} else {
					writing = true
				}
				request.granted <- empty{}
			}
			if nReading > 0 {
				// try to grant as many rlocks as possible
				for !queue.Empty() {
					request := queue.Peek().(lockRequest)
					if request.readonly {
						queue.Dequeue()
						nReading++
						request.granted <- empty{}
					} else {
						break
					}
				}
			}
		case <-lock.quit:
			break loop
		}
	}
}
