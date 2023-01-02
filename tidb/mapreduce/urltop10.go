package main

import (
	"bytes"
	"container/heap"
	"fmt"
	"strconv"
	"strings"
)

// URLTop10 .
func URLTop10(nWorkers int) RoundsArgs {
	// YOUR CODE HERE :)
	// And don't forget to document your idea.
	var args RoundsArgs
	args = append(args, RoundArgs{
		MapFunc:    URLCountMap,
		ReduceFunc: URLCountReduce,
		NReduce:    nWorkers,
	})
	args = append(args, RoundArgs{
		MapFunc:    URLTop10Map,
		ReduceFunc: URLTop10Reduce,
		NReduce:    1,
	})
	return args
}

// URLCountMap map file to {Key: fileNum, Value: url1 3}, {Key: fileNum, Value: url4 2}...
func URLCountMap(filename string, contents string) []KeyValue {
	tmpKv := make(map[string]int, 1024)
	urlKindCnt := 0
	lines := strings.Split(contents, "\n")
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if len(l) == 0 {
			continue
		}
		if _, ok := tmpKv[l]; !ok {
			urlKindCnt++
		}
		tmpKv[l]++
	}

	workersNum := GetMRCluster().NWorkers()
	kvs := make([]KeyValue, 0, urlKindCnt)
	var buf bytes.Buffer
	for k, v := range tmpKv {
		buf.WriteString(k + " " + strconv.Itoa(v))
		kvs = append(kvs, KeyValue{
			Key:   strconv.Itoa(ihash(k) % workersNum),
			Value: buf.String(),
		})
		buf.Reset()
	}
	return kvs
}

// URLCountReduce do a small part of topK
func URLCountReduce(key string, values []string) string {
	kvs := make(map[string]int, 1024)

	for _, v := range values {
		if len(v) == 0 {
			continue
		}
		tmp := strings.Split(v, " ")
		val, err := strconv.Atoi(tmp[1])
		if err != nil {
			panic(err)
		}
		kvs[tmp[0]] += val
	}

	// topK
	h := &IHeap{}
	heap.Init(h)
	for k, v := range kvs {
		heap.Push(h, Item{k, v})
		if h.Len() > 10 {
			heap.Pop(h)
		}
	}
	us, cs := TopN(kvs, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s %d\n", us[i], cs[i])
	}
	return buf.String()
}

func URLTop10Map(filename string, contents string) []KeyValue {
	lines := strings.Split(contents, "\n")
	kvs := make([]KeyValue, 0, len(lines))
	for _, l := range lines {
		kvs = append(kvs, KeyValue{"", l})
	}
	return kvs
}

func URLTop10Reduce(key string, values []string) string {
	kvs := make(map[string]int, len(values))
	var err error
	for _, v := range values {
		line := strings.Split(v, " ")
		if len(line) != 2 {
			continue
		}
		kvs[line[0]], err = strconv.Atoi(line[1])
		if err != nil {
			panic(err)
		}
	}

	// topK
	h := &IHeap{}
	heap.Init(h)
	for k, v := range kvs {
		heap.Push(h, Item{k, v})
		if h.Len() > 10 {
			heap.Pop(h)
		}
	}
	us, cs := TopN(kvs, 10)
	buf := new(bytes.Buffer)
	for i := range us {
		fmt.Fprintf(buf, "%s: %d\n", us[i], cs[i])
	}
	return buf.String()
}

// Item is the element of IHeap
type Item struct {
	url         string
	occurrences int
}

// TopK implementation
func (i *IHeap) TopK(kvs map[string]int, n int) ([]string, []int) {
	h := &IHeap{}
	heap.Init(h)
	for k, v := range kvs {
		heap.Push(h, Item{k, v})
		if h.Len() > n {
			heap.Pop(h)
		}
	}
	urls := make([]string, 0, n)
	cnts := make([]int, 0, n)
	for i, v := range *h {
		urls[i] = v.url
		cnts[i] = v.occurrences
	}
	return urls, cnts
}

// IHeap topK algorithm
type IHeap []Item

// Len 计算长度
func (h IHeap) Len() int { return len(h) }

// Less 用于比较
func (h IHeap) Less(i, j int) bool { return h[i].occurrences > h[j].occurrences }

// Swap 用于交换
func (h IHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push 放入
func (h *IHeap) Push(x interface{}) {
	*h = append(*h, x.(Item))
}

// Pop 取出
func (h *IHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
