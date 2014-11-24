/*
A library to create in process topologies of goroutines connected by channels.
Topo does boilerplate work as outlined in http://blog.golang.org/pipelines.
You receive correctly connected input and output channels, leaving the
message processing for you while handling the plumbing.

Example Code

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
*/
package topo
