package topo

import (
	"math"
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
	known   []interface{}
	unknown []interface{}
}

// tester reads messages from the work channel and adds those
// of type 'string' to the 'known' slice, and anything else
// to the 'unknown' slice.
func tester(s *sink, name int, wg *sync.WaitGroup, work <-chan Mesg) {
	defer wg.Done()
	for w := range work {
		switch b := w.Body().(type) {
		default:
			s.unknown = append(s.unknown, b)
		case string:
			s.known = append(s.known, b)
		}

	}
}

// newSink creates a new sink.
func newSink() *sink {
	return &sink{known: make([]interface{}, 0), unknown: make([]interface{}, 0)}
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

// TestRobin tests that the round-robin topology send inputs in alternating
// fasion to two consumers. The numbers 0 to 10 are sent, meaning that
// each of the conumers should have all odd numbers and the other
// all even numbers, with the absolute difference between
// corresponding collected entries being 1.
func TestRobin(t *testing.T) {
	const (
		diffexpected = 1
		count        = 10
		nworkers     = 2
	)

	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Set up the topology with:
	//    1. One source of consecutive numbers starting at 0;
	//    2. Two sinks for those numbers, each should
	//       receive every other number.
	topo := New(123)
	source := newNumberSource(count, topo)
	outputs := topo.Robin(nworkers, source)

	// Start the sinks.
	sinks := make([]*sink, nworkers)
	for i := 0; i < nworkers; i++ {
		sinks[i] = newSink()
		go tester(sinks[i], i, wg, outputs[i])
	}

	wg.Wait()

	// Check that the absolute difference between
	// corresponding numbers is the expected diff.
	var n1 float64
	var n2 float64
	var err error
	for i := 0; i < count/2; i++ {
		n1, err = strconv.ParseFloat(sinks[0].known[i].(string), 64)
		if err != nil {
			t.Errorf("Error parsing number from received message body: %v\n", err)
		}

		n2, err = strconv.ParseFloat(sinks[1].known[i].(string), 64)
		if err != nil {
			t.Errorf("Error parsing number from received message body: %v\n", err)
		}

		if diff := math.Abs(n2 - n1); diff != diffexpected {
			t.Errorf("Absolute value of difference between sink[0].known[%d] and sink[1].known[%d] should of been %d, but was Abs(%.0f - %.0f) = %.0f.", i, i, diffexpected, n2, n1, diff)
		}
	}
}
