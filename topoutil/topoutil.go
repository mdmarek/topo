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

package topoutil

import (
	"bufio"
	"fmt"
	"net/http"
	"sync"

	"github.com/mdmarek/topo"
)

type mesg struct {
	key  uint64
	body string
}

func (m *mesg) Key() uint64 {
	return m.key
}

func (m *mesg) Body() interface{} {
	return m.body
}

// Sink reads from the work chan and prints the body of each message. When
// the work chan is closed it joins the wait group.
func Sink(name int, wg *sync.WaitGroup, work <-chan topo.Mesg) {
	defer wg.Done()
	fmt.Printf("Sink %d starting...\n", name)
	for w := range work {
		switch b := w.Body().(type) {
		default:
			fmt.Printf("Sink %d: unknown body type: %T :: %v\n", name, b, b)
		case string:
			fmt.Printf("Sink %d: %v\n", name, b)
		}

	}
	fmt.Printf("Sink %d finished.\n", name)
}

// NewMeetup creates a channel of messags sourced from meetup.com's public stream.
// More info at: http://www.meetup.com/meetup_api/docs/stream/2/rsvps/
func NewMeetupSource(t topo.Topo) (<-chan topo.Mesg, error) {
	return NewChunkedHttpSource("http://stream.meetup.com/2/rsvps", t, "meetup")
}

// NewUsaGov creates a channel of messags sourced from USA.gov's public stream of bit.ly clicks.
// More info at: http://www.usa.gov/About/developer-resources/1usagov.shtml
func NewUsaGovSource(t topo.Topo) (<-chan topo.Mesg, error) {
	return NewChunkedHttpSource("http://developer.usa.gov/1usagov", t, "usagov")
}

// NewChunkedHttpSource creates a channel of messages sourced from a chunked HTTP connection
// which sends each message delimited by a newline. Paremter 'url' is the source, and 'name'
// is included in errors printed.
func NewChunkedHttpSource(url string, t topo.Topo, name string) (<-chan topo.Mesg, error) {
	if resp, err := http.Get(url); err != nil {
		return nil, err
	} else {
		out := make(chan topo.Mesg)
		go func(exit <-chan int) {
			defer close(out)
			// Scanner by default will split on newlines, if the chunked HTTP source
			// delimits by newline then this scanner will work.
			scanner := bufio.NewScanner(resp.Body)
			for {
				scanner.Scan()
				err = scanner.Err()
				if err != nil {
					fmt.Printf("error: source: %v: %v\n", name, err)
					return
				}
				// Read the text body of the scan, since there
				// was no error.
				body := scanner.Text()
				select {
				case out <- &mesg{0, body}:
				case <-exit:
					return
				}
			}
		}(t.ExitChan())
		return out, nil
	}
}
