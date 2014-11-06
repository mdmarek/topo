package topo

import (
	"math/rand"
	"sync"
	"sync/atomic"
)

type topo struct {
	sig chan int
	rng *rand.Rand
}

// Topology represents a graph of communicating channel readers and writers.
type Topo interface {
	Exit()
	ExitChan() <-chan int
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

// Exit requests that the topology exits. This is done my closing the
// topology's exit channel, all intermediate stages read this channel
// in their select-statements and exit. The user defined sources
// must also read the exit channel in their select-statements
// and close their output channels and clean up when the exit
// channel closes.
func (topo *topo) Exit() {
	close(topo.sig)
}

// ExitChan returns the topology's 'exit' channel, which can be closed
// by calling the topology's Exit() method. Sources should use this
// channel in their select-statements because a closed channel is
// always considered available and will return the channels zero
// value.
func (topo *topo) ExitChan() <-chan int {
	return topo.sig
}

// Merge merges the input channels into a single output channel.
func (topo *topo) Merge(ins []<-chan Mesg) <-chan Mesg {
	var wg sync.WaitGroup
	out := make(chan Mesg)

	fanin := func(in <-chan Mesg) {
		defer wg.Done()
		// Notice that the for-loop will exit only if upstream
		// closes the input channel. This is intentional.
		// Normally "upstream" will have been created by one of
		// the other topology methods such as Shuffle() or
		// Robin() which should correctly close their output
		// channels, which would be this range's intput.
		for n := range in {
			select {
			case out <- n:
			case <-topo.sig:
				// This works because a closed channel is
				// always selectable. When someone asks
				// for the topology to exit, it will
				// close this channel, making it
				// selectable, and this goroutine
				// will exit.
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
			// Notice that the for-loop will exit only if upstream
			// closes the input channel. This is intentional.
			// Normally "upstream" will have been created by one of
			// the other topology methods such as Shuffle() or
			// Robin() which should correctly close their output
			// channels, which would be this range's intput.
			for n := range in {
				robin = atomic.AddUint64(&robin, 1)
				select {
				case outs[robin%uint64(nparts)] <- n:
				case <-topo.sig:
					// This works because a closed channel is
					// always selectable. When someone asks
					// for the topology to exit, it will
					// close this channel, making it
					// selectable, and this goroutine
					// will exit.
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
			// Notice that the for-loop will exit only if upstream
			// closes the input channel. This is intentional.
			// Normally "upstream" will have been created by one of
			// the other topology methods such as Shuffle() or
			// Robin() which should correctly close their output
			// channels, which would be this range's intput.
			for n := range in {
				select {
				case outs[topo.rng.Int()%nparts] <- n:
				case <-topo.sig:
					// This works because a closed channel is
					// always selectable. When someone asks
					// for the topology to exit, it will
					// close this channel, making it
					// selectable, and this goroutine
					// will exit.
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
			// Notice that the for-loop will exit only if upstream
			// closes the input channel. This is intentional.
			// Normally "upstream" will have been created by one of
			// the other topology methods such as Shuffle() or
			// Robin() which should correctly close their output
			// channels, which would be this range's intput.
			for n := range in {
				select {
				case outs[n.Key()%uint64(nparts)] <- n:
				case <-topo.sig:
					// This works because a closed channel is
					// always selectable. When someone asks
					// for the topology to exit, it will
					// close this channel, making it
					// selectable, and this goroutine
					// will exit.
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
