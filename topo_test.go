package topo

import (
	"strconv"
	"sync"
	"testing"
)

type tmesg struct {
	key  uint64
	body string
}

func (m *tmesg) Key() uint64 {
	return m.key
}

func (m *tmesg) Body() interface{} {
	return m.body
}

type sink struct {
	sunk    []interface{}
	unknown []interface{}
}

// tester reads messages from the work channel and adds those
// of type 'string' to the 'sunk' slice, and anything else
// to the 'unknown' slice.
func tester(s *sink, name int, wg *sync.WaitGroup, work <-chan Mesg) {
	defer wg.Done()
	for w := range work {
		switch b := w.Body().(type) {
		default:
			s.unknown = append(s.unknown, b)
		case string:
			s.sunk = append(s.sunk, b)
		}

	}
}

// newSink creates a new sink.
func newSink() *sink {
	return &sink{sunk: make([]interface{}, 0), unknown: make([]interface{}, 0)}
}

// newNumberSource sends messages of consecutive numbers, starting at 0
// and going up to, but not including, count. The numbers are converted
// to strings for the message body.
func newNumberSource(count uint64, topo Topo) <-chan Mesg {
	out := make(chan Mesg)
	go func(exit <-chan int) {
		defer close(out)
		for i := 0; i < 10; i++ {
			select {
			case out <- &tmesg{key: uint64(i), body: strconv.Itoa(i)}:
			case <-exit:
				return
			}
		}
	}(topo.ExitChan())
	return out
}

// TestShuffle tests that the shuffle topology sends all the messages
// and between all sinks, all sent messages should be received.
func TestShuffle(t *testing.T) {
	const (
		diffexpected = 1
		count        = 10
		expected     = ((count - 1) * count) / 2 // Expected sum of 0 to count, inclusive
		nworkers     = 2
	)

	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Set up the topology with:
	//    1. One source of consecutive numbers starting at 0;
	//    2. Two sinks for those numbers, each should
	//       receive a random subset.
	topo := New(123)
	source := newNumberSource(count, topo)
	outputs := topo.Shuffle(nworkers, source)

	// Start the sinks.
	sinks := make([]*sink, nworkers)
	for i := 0; i < nworkers; i++ {
		sinks[i] = newSink()
		go tester(sinks[i], i, wg, outputs[i])
	}

	wg.Wait()

	// We test that all numbers were received, we can
	// do this by summing the received numbers and
	// checking that the total is as expected.
	var total float64
	for i := 0; i < nworkers; i++ {
		for j, nstr := range sinks[i].sunk {
			if n, err := strconv.ParseFloat(nstr.(string), 64); err != nil {
				t.Errorf("Failed to parse value at sinks[%d].sunk[%d]: %v\n", i, j, err)
			} else {
				total += n
			}
		}
	}

	if total != expected {
		t.Errorf("Expected total sum for enumeration of 0 to %v is: %v, but was: %v\n", count, expected, total)
	}
}
