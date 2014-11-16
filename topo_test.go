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
func tester(s *sink, wg *sync.WaitGroup, work <-chan Mesg) {
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

// newNumberSource sends messages of consecutive numbers, starting at first
// and going up to, but not including, last. The numbers are converted to
// strings for the message body, and are also set as the message key.
func newNumberSource(first, last int, topo Topo) <-chan Mesg {
	out := make(chan Mesg)
	go func(exit <-chan bool) {
		defer close(out)
		for i := first; i < last; i++ {
			select {
			case out <- &tmesg{key: uint64(i), body: strconv.Itoa(i)}:
			case <-exit:
				return
			}
		}
	}(topo.ExitChan())
	return out
}

// TestMerge tests that merge topology combines multiple sources into
// a single output, all messages sent should be received on this
// single output.
func TestMerge(t *testing.T) {
	const (
		diffexpected = 1
		first        = 0
		mid          = 10
		last         = 20
		expected     = ((last - 1) * last) / 2
	)

	wg := new(sync.WaitGroup)
	wg.Add(1)

	// Set up the topology with:
	//    1. Two sources, one ranging over [first,mid)
	//       and the second over [mid,last)
	//    2. Merge the two sources into one output
	//       channel.
	topo := New(123)
	source1 := newNumberSource(first, mid, topo)
	source2 := newNumberSource(mid, last, topo)
	output := topo.Merge(source1, source2)

	sink := newSink()
	go tester(sink, wg, output)

	wg.Wait()

	// Sum the numbers, it should be the sum of first to last.
	var total int
	for i, m := range sink.sunk {
		if n, err := strconv.Atoi(m.(string)); err != nil {
			t.Errorf("Failed to parse value at sink.sunk[%d]: %v\n", i, err)
		} else {
			total += n
		}
	}

	if total != expected {
		t.Errorf("Expected total sum for enumeration of [%v,%v) is: %v, but was: %v\n", first, last, expected, total)
	}
}

// TestShuffle tests that the shuffle topology sends all the messages
// and between all sinks, all sent messages should be received.
func TestShuffle(t *testing.T) {
	const (
		diffexpected = 1
		first        = 0
		last         = 10
		expected     = ((last - 1) * last) / 2
		nworkers     = 2
	)

	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Set up the topology with:
	//    1. One source of consecutive numbers starting at 0;
	//    2. Two sinks for those numbers, each should
	//       receive a random subset.
	topo := New(123)
	source := newNumberSource(first, last, topo)
	outputs := topo.Shuffle(nworkers, source)

	// Start the sinks.
	sinks := make([]*sink, nworkers)
	for i := 0; i < nworkers; i++ {
		sinks[i] = newSink()
		go tester(sinks[i], wg, outputs[i])
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
		t.Errorf("Expected total sum for enumeration of [%v,%v) is: %v, but was: %v\n", first, last, expected, total)
	}
}

// TestPartition tests that the partition topology sends messages based
// on the message key, in this case the same integer encoded into the
// body.
func TestPartition(t *testing.T) {
	const (
		diffexpected = 1
		first        = 0
		last         = 10
		expected     = ((last - 1) * last) / 2
		nworkers     = 2
	)

	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Set up the topology with:
	//    1. One source of consecutive numbers starting at 0;
	//    2. Two sinks for those numbers, sink 0 should
	//       receive the evens, sink 1 should receive
	//       the odds.
	topo := New(123)
	source := newNumberSource(first, last, topo)
	outputs := topo.Partition(nworkers, source)

	// Start the sinks.
	sinks := make([]*sink, nworkers)
	for i := 0; i < nworkers; i++ {
		sinks[i] = newSink()
		go tester(sinks[i], wg, outputs[i])
	}

	wg.Wait()

	// We test that all numbers were received, we can
	// do this by summing the received numbers and
	// checking that the total is as expected.
	var total int

	// Sink 0 is expected to get the even numbers: 0, 2, 4, 8, 10
	for _, m := range sinks[0].sunk {
		n, err := strconv.Atoi(m.(string))
		if err != nil {
			t.Errorf("Expected parsable number: %v\n", err)
		}

		if rem := n % 2; rem != 0 {
			t.Errorf("Expected only number n such that: n %% 2 == 0, but n = %d, and %d %% 2 == %d, not 0\n", n, n, rem)
		}

		total += n
	}

	// Sink 1 is expected to get the odd numbers: 1, 3, 5, 7, 9
	for _, m := range sinks[1].sunk {
		n, err := strconv.Atoi(m.(string))
		if err != nil {
			t.Errorf("Expected parsable number: %v\n", err)
		}

		if rem := (n - 1) % 2; rem != 0 {
			t.Errorf("Expected only number n such that (n-1) %% 2 == 0, but n = %d, and %d %% 2 == %d, not 0\n", n, n-1, rem)
		}

		total += n
	}

	if total != expected {
		t.Errorf("Expected total sum for enumeration of [%v,%v) is: %v, but was: %v\n", first, last, expected, total)
	}
}

// TestExit tests that reading only a partial number of messages and then
// calling Exit should correctly close all channels and goroutines and
// no deadlock panic should occur at the end of the test.
func TestExit(t *testing.T) {
	const (
		diffexpected = 1
		first        = 0
		last         = 10
		expected     = ((last - 1) * last) / 2
		nworkers     = 2
	)

	wg := new(sync.WaitGroup)
	wg.Add(nworkers)

	// Set up the topology with:
	//    1. One source of consecutive numbers starting at 0;
	//    2. Two sinks for those numbers, each should
	//       receive either the odd or even subset.
	topo := New(123)
	source := newNumberSource(first, last, topo)
	outputs := topo.Partition(nworkers, source)

	go func() {
		m := <-outputs[0]
		if m.Key() != 0 {
			t.Errorf("Expected message from outputs[%v] was not %v but rather: %v\n", 0, 0, m.Key())
		}
		wg.Done()
	}()
	go func() {
		m := <-outputs[1]
		if m.Key() != 1 {
			t.Errorf("Expected message from outputs[%v] was not %v but rather: %v\n", 1, 1, m.Key())
		}
		wg.Done()
	}()

	// Reading only a partial number of the total messages should
	// NOT cause a deadlock panic at the end of the test.

	wg.Wait()
	topo.Exit()
}

// BenchmarkShuffle1Deep tests a shuffle topology when there is only
// 1 shuffle hop to get to the sink. The intention is to see how
// much performance is affected by simply introducing more
// channel hops irregardless of work done between channels.
func BenchmarkShuffle1Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs := topo.Shuffle(nworkers, source)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs[i])
		}

		wg.Wait()
	}
}

// BenchmarkShuffle1Deep tests a shuffle topology when there are
// 2 shuffle hops to get to the sink.
func BenchmarkShuffle2Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs1 := topo.Shuffle(nworkers, source)
		outputs2 := topo.Shuffle(nworkers, outputs1...)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs2[i])
		}

		wg.Wait()
	}
}

// BenchmarkShuffle1Deep tests a shuffle topology when there are
// 3 shuffle hops to get to the sink.
func BenchmarkShuffle3Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs1 := topo.Shuffle(nworkers, source)
		outputs2 := topo.Shuffle(nworkers, outputs1...)
		outputs3 := topo.Shuffle(nworkers, outputs2...)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs3[i])
		}

		wg.Wait()
	}
}

// BenchmarkShuffle1Deep tests a shuffle topology when there are
// 4 shuffle hops to get to the sink.
func BenchmarkShuffle4Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs1 := topo.Shuffle(nworkers, source)
		outputs2 := topo.Shuffle(nworkers, outputs1...)
		outputs3 := topo.Shuffle(nworkers, outputs2...)
		outputs4 := topo.Shuffle(nworkers, outputs3...)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs4[i])
		}

		wg.Wait()
	}
}

// BenchmarkPartition1Deep tests a shuffle topology when there is only
// 1 partition hop to get to the sink. The intention is to see how
// much performance is affected by simply introducing more
// channel hops irregardless of work done between channels.
func BenchmarkPartition1Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs := topo.Partition(nworkers, source)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs[i])
		}

		wg.Wait()
	}
}

// BenchmarkPartition1Deep tests a shuffle topology when there are
// 2 partition hops to get to the sink.
func BenchmarkPartition2Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs1 := topo.Partition(nworkers, source)
		outputs2 := topo.Partition(nworkers, outputs1...)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs2[i])
		}

		wg.Wait()
	}
}

// BenchmarkPartition1Deep tests a shuffle topology when there are
// 3 partition hops to get to the sink.
func BenchmarkPartition3Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs1 := topo.Partition(nworkers, source)
		outputs2 := topo.Partition(nworkers, outputs1...)
		outputs3 := topo.Partition(nworkers, outputs2...)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs3[i])
		}

		wg.Wait()
	}
}

// BenchmarkPartition1Deep tests a shuffle topology when there are
// 4 partition hops to get to the sink.
func BenchmarkPartition4Deep(b *testing.B) {

	const nworkers = 1

	for i := 0; i < b.N; i++ {
		wg := new(sync.WaitGroup)
		wg.Add(nworkers)

		topo := New(123)
		source := newNumberSource(0, 1000, topo)
		outputs1 := topo.Partition(nworkers, source)
		outputs2 := topo.Partition(nworkers, outputs1...)
		outputs3 := topo.Partition(nworkers, outputs2...)
		outputs4 := topo.Partition(nworkers, outputs3...)

		// Start the sinks.
		sinks := make([]*sink, nworkers)
		for i := 0; i < nworkers; i++ {
			sinks[i] = newSink()
			go tester(sinks[i], wg, outputs4[i])
		}

		wg.Wait()
	}
}
