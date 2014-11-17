TOPO
====

A library to create in process topologies of goroutines connected by channels.
Topo does boilerplate work as outlined in http://blog.golang.org/pipelines.
You receive correctly connected input and output channels, leaving the
message processing for you while handling the plumbing. Topo strives to be
simple, all interaction are via proper Go channels, no wrapping interfaces.

# Example Code

```go
package main

import (
	"fmt"
	"sync"

	"github.com/mdmarek/topo"
	"github.com/mdmarek/topo/topoutil"
)

const nworkers = 2

func worker(in <-chan topo.Mesg, out chan<- topo.Mesg) {
	... do something ...
}

func main() {
	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Create a new topo and source of streaming data from meetup.com.
	t, err := topo.New()
	if err != nil {
		fmt.Printf("Failed to create topo: %v\n", err)
		return
	}

	source, err := topoutil.NewMeetupSource(t)
	if err != nil {
		fmt.Printf("Failed to open source: %v\n", err)
		return
	}

	// Shuffles messages read from the source
	// to each worker.
	outputs := t.Shuffle(nworkers, worker, source)

	// Each output channel is read by one Sink, which
	// prints to stdout the messages it receives.
	for i := 0; i < nworkers; i++ {
		go topoutil.Sink(i, wg, outputs[i])
	}

	// Wait for the sinks to finish, if ever.
	wg.Wait()
}
```

# Messages

Topo creates channels of type `chan Mesg`, and a `Mesg` is defined as the
interface: 

```go
Mesg { 
	Key() uint64
	Body() interface{}
}
```

# Compositions

Topo works through three simple compositions of channels to form pipelines: 
`Merge`, `Shuffle`, and `Partition`.

`Merge` takes _n_ input channels and merges them into one output channel.  

`Shuffle` takes _n_ input channels and connects them to _m_ functions writing their output 
to _m_ output channels. Messages from the _n_ input channels are sent to the first
available function.

`Partition` takes _n_ input channels and connects them to _m_ functions writing their output
to _m_ output channels. Messages from the _n_ input channels are routed by taking the
message's key value modulo _m_.

# Sources

When writing a source of data for the topology it should use the topology's exit channel
in its select statement, otherwise a deadlock panic may occur. The basic structure is
as follows:

```go
func NewMySource(... params ..., t topo.Topo) (<-chan topo.Mesg, error) {

	...

	out := make(chan topo.Mesg)
	go func(exit <-chan bool) {
		defer close(out)
		for ... {
			select {
			case out <- produce():
			case <-exit:
				return
			}
		}
	}(t.ExitChan())

	...

	return out, nil
}
```

Keep in mind to pass the exit channel as a parameter to any started goroutiness rather
than as a closure.