package topo

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type topo struct {
	sig <-chan int
	rng *rand.Rand
}

// Topology represents a graph of communicating channel readers and writers.
type Topo interface {
	SigCom() <-chan int
	Merge(ins []<-chan Mesg) <-chan Mesg
	Robin(nparts int, ins ...<-chan Mesg) []<-chan Mesg
	Shuffle(nparts int, ins ...<-chan Mesg) []<-chan Mesg
	Partition(nparts int, ins ...<-chan Mesg) []<-chan Mesg
}

// Mesg represents a message routable by the topology. The Key() method
// is used to route the message in certain topologies. Body() is used
// to express something user specific.
type Mesg interface {
	Key() uint64
	Body() interface{}
}

// New creates a new topology, where seed is the seed used for
// random shuffle topologies.
func New(seed int64) Topo {
	sig := make(chan int)
	rng := rand.New(rand.NewSource(seed))
	return &topo{sig: sig, rng: rng}
}

// SigCom returns the topology's 'signal communication' channel, which
// contains a message when the topology signals that it is done and
// users should start their exit procedures.
func (topo *topo) SigCom() <-chan int {
	return topo.sig
}

// Merge merges the input channels into a single output channel.
func (topo *topo) Merge(ins []<-chan Mesg) <-chan Mesg {
	var wg sync.WaitGroup
	out := make(chan Mesg)

	fanin := func(in <-chan Mesg) {
		defer wg.Done()
		for n := range in {
			select {
			case out <- n:
			case <-topo.sig:
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
// chosen output channel. Number of output channels is set by nparts.
func (topo *topo) Shuffle(nparts int, ins ...<-chan Mesg) []<-chan Mesg {
	var wg sync.WaitGroup
	var robin uint64
	wg.Add(len(ins))

	outs := make([]chan Mesg, nparts)
	for i := 0; i < nparts; i++ {
		outs[i] = make(chan Mesg)
	}

	for i := 0; i < len(ins); i++ {
		in := ins[i]
		go func() {
			defer wg.Done()
			for n := range in {
				robin = atomic.AddUint64(&robin, 1)
				select {
				case outs[robin%uint64(nparts)] <- n:
				case <-topo.sig:
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

	temp := make([]<-chan Mesg, nparts)
	for i := 0; i < nparts; i++ {
		temp[i] = outs[i]
	}

	return temp
}

// Robin reads data from input channels, and sends messages round-robin to the
// output channels. Number of output channels is set by nparts.
func (topo *topo) Robin(nparts int, ins ...<-chan Mesg) []<-chan Mesg {
	var wg sync.WaitGroup
	wg.Add(len(ins))

	outs := make([]chan Mesg, nparts)
	for i := 0; i < nparts; i++ {
		outs[i] = make(chan Mesg)
	}

	for i := 0; i < len(ins); i++ {
		in := ins[i]
		go func() {
			defer wg.Done()
			for n := range in {
				select {
				case outs[topo.rng.Int()%nparts] <- n:
				case <-topo.sig:
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

	temp := make([]<-chan Mesg, nparts)
	for i := 0; i < nparts; i++ {
		temp[i] = outs[i]
	}

	return temp
}

// Partition reads data from input channels, and uses the message partition
// key to consistently sends messages with the same key to the same output
// channel. Number of output channels is set by nparts.
func (topo *topo) Partition(nparts int, ins ...<-chan Mesg) []<-chan Mesg {
	var wg sync.WaitGroup
	wg.Add(len(ins))

	outs := make([]chan Mesg, nparts)
	for i := 0; i < nparts; i++ {
		outs[i] = make(chan Mesg)
	}

	for i := 0; i < len(ins); i++ {
		in := ins[i]
		go func() {
			defer wg.Done()
			for n := range in {
				select {
				case outs[n.Key()%uint64(nparts)] <- n:
				case <-topo.sig:
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

	temp := make([]<-chan Mesg, nparts)
	for i := 0; i < nparts; i++ {
		temp[i] = outs[i]
	}

	return temp
}
