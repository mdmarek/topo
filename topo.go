// Copyright 2014 Marek Dolgos
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package topo

import (
	"errors"
	"sync"
)

type topo struct {
	csize int
	sig   chan bool
}

// Topology represents a graph of communicating channel-readers and channel-writers.
type Topo interface {
	ConfChanSize(csize int) error
	Exit()
	ExitChan() <-chan bool
	Merge(ins ...<-chan Mesg) <-chan Mesg
	Shuffle(nparts int, f func(<-chan Mesg, chan<- Mesg), ins ...<-chan Mesg) []<-chan Mesg
	Partition(nparts int, f func(<-chan Mesg, chan<- Mesg), ins ...<-chan Mesg) []<-chan Mesg
}

// Mesg represents a message routable by the topology. The Key() method
// is used to route the message in certain topologies. Body() is used
// to express something user specific.
type Mesg interface {
	Key() uint64
	Body() interface{}
}

// New creates a new topology.
func New() (Topo, error) {
	sig := make(chan bool)
	return &topo{sig: sig}, nil
}

// ConfChanSize configures the size of output channels create by calls to
// Partition. This method should be called before use of the topology.
func (topo *topo) ConfChanSize(csize int) error {
	if csize < 0 {
		return errors.New("topo: channel size must be non-negative")
	}
	topo.csize = csize
	return nil
}

// Exit requests that the topology exits. This is done my closing the
// topology's exit channel, all intermediate stages read this channel
// in their select-statements and exit. The user defined sources
// must also read the exit channel in their select-statements
// and close their output channels and clean up when the exit
// channel closes.
func (topo *topo) Exit() {
	select {
	case _, open := <-topo.sig:
		if open {
			close(topo.sig)
		} else {
			// Already closed, do nothing
		}
	default:
		close(topo.sig)
	}
}

// ExitChan returns the topology's 'exit' channel, which can be closed
// by calling the topology's Exit() method. Sources should use this
// channel in their select-statements because a closed channel is
// always considered available and will return the channels zero
// value.
func (topo *topo) ExitChan() <-chan bool {
	return topo.sig
}

// Merge merges the input channels into a single output channel and
// returns it for further plumbing.
func (topo *topo) Merge(ins ...<-chan Mesg) <-chan Mesg {
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

// Shuffle runs 'go f' n times and plumbs the toplogy to send messages from the 'ins' channels
// to some non-busy running f. n output channels are returned for further plumbing.
func (topo *topo) Shuffle(n int, f func(<-chan Mesg, chan<- Mesg), ins ...<-chan Mesg) []<-chan Mesg {
	var wg sync.WaitGroup
	wg.Add(n)

	in := topo.Merge(ins...)

	outs := make([]chan Mesg, n)
	for i := 0; i < n; i++ {
		out := make(chan Mesg, 0)
		go func() {
			defer wg.Done()
			f(in, out)
		}()
		outs[i] = out
	}

	go func() {
		wg.Wait()
		for i := 0; i < n; i++ {
			close(outs[i])
		}
	}()

	// This is done because there is no direct way
	// to cast "[]chan Mesg" to "[]<-chan Mesg"
	temp := make([]<-chan Mesg, n)
	for i := 0; i < n; i++ {
		temp[i] = outs[i]
	}

	return temp
}

// Partition runs 'go f' n times and plumbs the toplogy to send messages from the 'ins' channels
// to the same 'f' consistently. In other words, messages with the same key always go to the
// same running 'f'. n output channels are returned for further plumbing.
func (topo *topo) Partition(n int, f func(<-chan Mesg, chan<- Mesg), ins ...<-chan Mesg) []<-chan Mesg {

	// Output channels are closed when all function f's
	// have exited.
	var wgouts sync.WaitGroup
	wgouts.Add(n)

	// Parts channels are closed when all the input channels
	// have closed.
	var wgprts sync.WaitGroup
	wgprts.Add(len(ins))

	// What the hell is this code doing? In the case Partition
	// intermediate channels are need which will be passed
	// as the inputs to function _f_. In the case of
	// Shuffle, this was not needed because Shuffle can just
	// take from one merged input channel.
	prts := make([]chan Mesg, n)
	outs := make([]chan Mesg, n)
	for i := 0; i < n; i++ {
		prt := make(chan Mesg, topo.csize)
		out := make(chan Mesg, topo.csize)
		go func() {
			defer wgouts.Done()
			f(prt, out)
		}()
		prts[i] = prt
		outs[i] = out
	}

	for i := 0; i < len(ins); i++ {
		go func(in <-chan Mesg, exit <-chan bool) {
			defer wgprts.Done()
			// Notice that the for-loop will exit only if upstream
			// closes the input channel.
			for m := range in {
				select {
				case prts[m.Key()%uint64(n)] <- m:
				case <-exit:
					// This works because a closed channel is
					// always selectable. When someone asks
					// for the topology to exit, it will
					// close this channel, making it
					// selectable, and this goroutine
					// will exit.
					return
				}
			}
		}(ins[i], topo.sig)
	}

	go func() {
		wgouts.Wait()
		for i := 0; i < n; i++ {
			close(outs[i])
		}
	}()

	go func() {
		wgprts.Wait()
		for i := 0; i < n; i++ {
			close(prts[i])
		}
	}()

	// This is done because there is no direct way
	// to cast "[]chan Mesg" to "[]<-chan Mesg"
	temp := make([]<-chan Mesg, n)
	for i := 0; i < n; i++ {
		temp[i] = outs[i]
	}

	return temp
}
