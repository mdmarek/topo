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

package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/mdmarek/topo"
	"github.com/mdmarek/topo/topoutil"
)

const nworkers = 2

func main() {
	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	t, err := topo.New()
	if err != nil {
		fmt.Printf("failed to create topology: %v\n", err)
	}

	source, err := topoutil.NewUsaGovSource(t)
	if err != nil {
		fmt.Printf("Failed to open source: %v\n", err)
		return
	}

	// Parse json messages concurrently then consistently send
	// messages with the same key to the same counter.
	usagovs := t.Shuffle(nworkers, Parser, source)
	outputs := t.Partition(nworkers, Counter, usagovs...)

	// Each output channel is read by one Sink, which
	// prints to stdout the messages it receives.
	for i := 0; i < nworkers; i++ {
		go topoutil.Sink(i, wg, outputs[i])
	}

	// Wait for the sinks to finish, if ever.
	wg.Wait()
}

// Parser parses messages from the input channel. The input body is expected to be a JOSN
// string and the output body will be of type UsaGov.
func Parser(in <-chan topo.Mesg, out chan<- topo.Mesg) {
	hash := fnv.New64()
	for m := range in {
		switch b := m.Body().(type) {
		case string:
			body := &UsaGov{}
			err := json.Unmarshal([]byte(b), &body)
			if err != nil {
				fmt.Printf("failed to unmarshal json: %v", b)
			}
			hash.Reset()
			hash.Write([]byte(body.Link))
			out <- &usagovmesg{key: hash.Sum64(), body: body}
		default:
			fmt.Printf("unknown message type: %T :: %v", b, b)
		}
	}
}

// Counter counts input message by link. The input body is expected to be of type UsaGov
// and the output body will be of type LinkCount.
func Counter(in <-chan topo.Mesg, out chan<- topo.Mesg) {
	counts := make(map[string]uint64)
	for m := range in {
		switch b := m.Body().(type) {
		case *UsaGov:
			link := b.Link
			if c, ok := counts[link]; ok {
				counts[link] = c + 1
			} else {
				counts[link] = 1
			}
			out <- &usagovmesg{body: &LinkCount{link: link, count: counts[link]}}
		default:
			fmt.Printf("unknown message type: %T :: %v", b, b)
		}
	}
}

type usagovmesg struct {
	key  uint64
	body interface{}
}

func (m *usagovmesg) Key() interface{} {
	return m.key
}

func (m *usagovmesg) Body() interface{} {
	return m.body
}

type LinkCount struct {
	link  string
	count uint64
}

type UsaGov struct {
	Ref  string `json:"r"`
	Tz   string `json:"tz"`
	Link string `json:"u"`
	City string `json:"cy"`
}
