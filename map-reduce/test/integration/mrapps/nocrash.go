package main

//
// same as crash.go but doesn't actually crash.
//
// go build -buildmode=plugin nocrash.go
//

import (
	crand "crypto/rand"
	"mapreduce/worker"
	"math/big"
	"os"
	"sort"
	"strconv"
	"strings"
)

func maybeCrash() {
	max := big.NewInt(1000)
	rr, _ := crand.Int(crand.Reader, max)
	if false && rr.Int64() < 500 {
		// crash!
		os.Exit(1)
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
