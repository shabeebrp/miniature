package main

//
// a MapReduce pseudo-application that sometimes crashes,
// and sometimes takes a long time,
// to test MapReduce's ability to recover.
//
// go build -buildmode=plugin crash.go
//

import (
	crand "crypto/rand"
	"mapreduce/worker"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if rr.Int64() < 330 {
		// crash!
		os.Exit(1)
	} else if rr.Int64() < 660 {
		// delay for a while.
		maxms := big.NewInt(10 * 1000)
		ms, _ := crand.Int(crand.Reader, maxms)
		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
	}
}

func Map(filename string, contents string) []worker.KeyValue {
	maybeCrash()

	kva := []worker.KeyValue{}
	kva = append(kva, worker.KeyValue{"a", filename})
	kva = append(kva, worker.KeyValue{"b", strconv.Itoa(len(filename))})
	kva = append(kva, worker.KeyValue{"c", strconv.Itoa(len(contents))})
	kva = append(kva, worker.KeyValue{"d", "xyzzy"})
	return kva
}

func Reduce(key string, values []string) string {
	maybeCrash()

	// sort values to ensure deterministic output.
	vv := make([]string, len(values))
	copy(vv, values)
	sort.Strings(vv)

	val := strings.Join(vv, " ")
	return val
}
