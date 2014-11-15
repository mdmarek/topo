TOPO
====

A library to create in process topologies of goroutines connected by channels.
Topo does boilerplate work as outlined in http://blog.golang.org/pipelines.
You receive correctly connected input and output channels, leaving the
message processing for you while handling the plumbing.

# Example Code

```go
package main

import (
	"fmt"
	"sync"

	"github.com/mdmarek/topo"
	"github.com/mdmarek/topo/topoutil"
)

const seed = 12282
const nworkers = 2

func main() {
	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Create a new topo and source of streaming data from meetup.com.
	t := topo.New(seed)
	source, err := topoutil.NewMeetupSource(t)

	if err != nil {
		fmt.Printf("Failed to open source: %v\n", err)
		return
	}

	// Shuffles messages read from the source
	// to each output channel.
	outputs := t.Shuffle(nworkers, source)

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

Topo creates channels of type `chan topo.Mesg`, and a `Mesg` is defined as the
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

`Shuffle` takes _n_ input channels and connects them to _m_ output channels. Each
message from one of the _n_ input channels is sent to a random output channel.

`Partition` takes _n_ input channels and connects them to _m_ output channels. Each
message from one of the _n_ input channels is checked for a numeric key, this is
moduled by _m_, and the message is sent to the corresponding output channel.

# Sources

When writing a source of data for the topology it should use the topologies exit channel
in its select statement, otherwise a deadlock panic may occure. The basic structure is
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