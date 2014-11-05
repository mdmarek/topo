package main

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
)

type Topo struct {
	done <-chan int
	rng  *rand.Rand
}

type Mesg struct {
	Key  uint64
	Body interface{}
}

func New(seed int64) *Topo {
	done := make(chan int)
	rng := rand.New(rand.NewSource(seed))
	return &Topo{done: done, rng: rng}
}

func NewM(body interface{}) *Mesg {
	return &Mesg{0, body}
}

func NewPM(key uint64, body interface{}) *Mesg {
	return &Mesg{key, body}
}

// Merge merges the input channels into a single output channel.
func (topo *Topo) Merge(ins []<-chan *Mesg) <-chan *Mesg {
	var wg sync.WaitGroup
	out := make(chan *Mesg)

	fanin := func(in <-chan *Mesg) {
		defer wg.Done()
		for n := range in {
			select {
			case out <- n:
			case <-topo.done:
				return
			}
		}
	}

	wg.Add(len(ins))

	for i := 0; i < len(ins); i++ {
		go fanin(ins[i])
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Shuffle reads data from input channels, and sends messages to a randomly
// chosen output channel.
func (topo *Topo) Shuffle(nparts int, ins ...<-chan *Mesg) []<-chan *Mesg {
	var wg sync.WaitGroup
	var robin uint64
	wg.Add(len(ins))

	outs := make([]chan *Mesg, nparts)
	for i := 0; i < nparts; i++ {
		outs[i] = make(chan *Mesg)
	}

	for i := 0; i < len(ins); i++ {
		in := ins[i]
		go func() {
			defer wg.Done()
			for n := range in {
				robin = atomic.AddUint64(&robin, 1)
				select {
				case outs[robin%uint64(nparts)] <- n:
				case <-topo.done:
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		for i := 0; i < nparts; i++ {
			close(outs[i])
		}
	}()

	temp := make([]<-chan *Mesg, nparts)
	for i := 0; i < nparts; i++ {
		temp[i] = outs[i]
	}

	return temp
}

// Robin reads data from input channels, and sends messages round-robin to the
// output channels.
func (topo *Topo) Robin(nparts int, ins ...<-chan *Mesg) []<-chan *Mesg {
	var wg sync.WaitGroup
	wg.Add(len(ins))

	outs := make([]chan *Mesg, nparts)
	for i := 0; i < nparts; i++ {
		outs[i] = make(chan *Mesg)
	}

	for i := 0; i < len(ins); i++ {
		in := ins[i]
		go func() {
			defer wg.Done()
			for n := range in {
				select {
				case outs[topo.rng.Int()%nparts] <- n:
				case <-topo.done:
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		for i := 0; i < nparts; i++ {
			close(outs[i])
		}
	}()

	temp := make([]<-chan *Mesg, nparts)
	for i := 0; i < nparts; i++ {
		temp[i] = outs[i]
	}

	return temp
}

// Partition reads data from input channels, and uses the message partition
// key to consistently sends messages with the same key to the same output
// channel.
func (topo *Topo) Partition(nparts int, ins ...<-chan *Mesg) []<-chan *Mesg {
	var wg sync.WaitGroup
	wg.Add(len(ins))

	outs := make([]chan *Mesg, nparts)
	for i := 0; i < nparts; i++ {
		outs[i] = make(chan *Mesg)
	}

	for i := 0; i < len(ins); i++ {
		in := ins[i]
		go func() {
			defer wg.Done()
			for n := range in {
				select {
				case outs[n.Key%uint64(nparts)] <- n:
				case <-topo.done:
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		for i := 0; i < nparts; i++ {
			close(outs[i])
		}
	}()

	temp := make([]<-chan *Mesg, nparts)
	for i := 0; i < nparts; i++ {
		temp[i] = outs[i]
	}

	return temp
}

type Vertex struct {
	X int
}

func Worker(name int, wg *sync.WaitGroup, work <-chan *Mesg) {
	defer wg.Done()
	fmt.Printf("Worker %d starting...\n", name)
	for w := range work {
		fmt.Printf("Worker %d: %v\n", name, w.Body.(*Vertex).X)
	}
	fmt.Printf("Worker %d finished.\n", name)
}

func Kafka(topo *Topo) <-chan *Mesg {

	gen := func(start int, finish int) <-chan int {
		out := make(chan int)
		go func() {
			for i := start; i <= finish; i++ {
				out <- i
			}
			close(out)
		}()
		return out
	}

	inputs := func(done <-chan int, in <-chan int) <-chan *Mesg {
		out := make(chan *Mesg)
		go func() {
			defer close(out)
			for n := range in {
				select {
				case out <- NewPM(uint64(n), &Vertex{n}):
				case <-topo.done:
					return
				}
			}
		}()
		return out
	}

	c := gen(1, 20)

	partitions := make([]<-chan *Mesg, 2)
	partitions[0] = inputs(topo.done, c)
	partitions[1] = inputs(topo.done, c)

	return topo.Merge(partitions)
}

func main() {
	nworkers := 2

	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	topo := New(1120202)

	topic := Kafka(topo)
	outputs := topo.Partition(nworkers, topic)

	for i := 0; i < nworkers; i++ {
		go Worker(i, wg, outputs[i])
	}

	wg.Wait()
}
