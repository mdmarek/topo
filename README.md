TOPO
====

A library to creat in process topologies of goroutines connected by channels.
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

# Three Basic Compositions

Topo works through three simple compositions of channels to form pipelines: 
`Merge`, `Shuffle`, and `Partition`.

A shuffle takes n input channels and connects them to m output channels. Each
message from one of the n input channels is sent to a random output channel.

A partition takes n input channels and connects them to m output channels. Each
message from one of the n input channels is checked for a numeric key, this is
moduled by m, and the message is sent to the corresponding output channel.

A merge takes n input channels and merges them into one output channel.