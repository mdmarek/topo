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

	// Create a new topo and source of streaming data from meetup.com.
	t, err := topo.New()
	if err != nil {
		fmt.Printf("failed to create topology: %v\n", err)
	}

	source, err := topoutil.NewMeetupSource(t)
	if err != nil {
		fmt.Printf("failed to open source: %v\n", err)
		return
	}

	// Parse json messages concurrently then consistently send
	// messages with the same key to the same counter.
	meetups := t.Shuffle(nworkers, Parser, source)
	outputs := t.Partition(nworkers, Counter, meetups...)

	// Each output channel is read by one Sink, which
	// prints to stdout the messages it receives.
	for i := 0; i < nworkers; i++ {
		go topoutil.Sink(i, wg, outputs[i])
	}

	// Wait for the sinks to finish, if ever.
	wg.Wait()
}

// Parser parses messages from the input channel. The input body is expected to be a JOSN
// string and the output body will be of type Meetup.
func Parser(in <-chan topo.Mesg, out chan<- topo.Mesg) {
	hash := fnv.New64()
	for m := range in {
		switch b := m.Body().(type) {
		case string:
			body := &Meetup{}
			err := json.Unmarshal([]byte(b), &body)
			if err != nil {
				fmt.Printf("failed to unmarshal json: %v", b)
			}
			hash.Reset()
			hash.Write([]byte(body.Group.City))
			out <- &meetupmesg{key: hash.Sum64(), body: body}
		default:
			fmt.Printf("unknown message type: %T :: %v", b, b)
		}
	}
}

// Counter counts input message by city. The input body is expected to be of type Meetup
// and the output body will be of type CityCount.
func Counter(in <-chan topo.Mesg, out chan<- topo.Mesg) {
	counts := make(map[string]uint64)
	for m := range in {
		switch b := m.Body().(type) {
		case *Meetup:
			city := b.Group.City
			if c, ok := counts[city]; ok {
				counts[city] = c + 1
			} else {
				counts[city] = 1
			}
			out <- &meetupmesg{body: &CityCount{city: city, count: counts[city]}}
		default:
			fmt.Printf("unknown message type: %T :: %v", b, b)
		}
	}
}

type meetupmesg struct {
	key  uint64
	body interface{}
}

func (m *meetupmesg) Key() interface{} {
	return m.key
}

func (m *meetupmesg) Body() interface{} {
	return m.body
}

type CityCount struct {
	city  string
	count uint64
}

type Meetup struct {
	Member MeetupMember `json:"member"`
	Guests int          `json:"guests"`
	Group  MeetupGroup  `json:"group"`
}

type MeetupMember struct {
	Name  string `json:"member_name"`
	Photo string `json:"photo"`
	Id    int    `json:"member_id"`
}

type MeetupGroup struct {
	Name string `json:"group_name"`
	Id   int    `json:"group_id"`
	City string `json:"group_city"`
}
