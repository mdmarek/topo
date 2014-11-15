// A library to create in process topologies of goroutines connected by channels.
// Topo does boilerplate work as outlined in http://blog.golang.org/pipelines.
// You receive correctly connected input and output channels, leaving the
// message processing for you while handling the plumbing.
//
// Example Code
//
// 	package main
//
// 	import (
// 		"fmt"
// 		"sync"
//
// 		"github.com/mdmarek/topo"
// 		"github.com/mdmarek/topo/topoutil"
// 	)
//
// 	const seed = 12282
// 	const nworkers = 2
//
// 	func main() {
// 		wg := new(sync.WaitGroup)
// 		wg.Add(nworkers)
//
// 		// Create a new topo and source of streaming data from meetup.com.
// 		t := topo.New(seed)
// 		source, err := topoutil.NewMeetupSource(t)
//
// 		if err != nil {
// 			fmt.Printf("Failed to open source: %v\n", err)
// 			return
// 		}
//
// 		// Shuffles messages read from the source
// 		// to each output channel.
// 		outputs := t.Shuffle(nworkers, source)
//
// 		// Each output channel is read by one Sink, which
// 		// prints to stdout the messages it receives.
// 		for i := 0; i < nworkers; i++ {
// 			go topoutil.Sink(i, wg, outputs[i])
// 		}
//
// 		// Wait for the sinks to finish, if ever.
// 		wg.Wait()
// 	}
//