package main

import (
	"encoding/csv"
	"github.com/pingcap/tidb/util/mvmap"
	"io"
	"os"
	"strconv"
	"unsafe"
)

const blockSize = 1200

// Join accepts a join query of two relations, and returns the sum of
// relation0.col0 in the final result.
// Input arguments:
//   f0: file name of the given relation0
//   f1: file name of the given relation1
//   offset0: offsets of which columns the given relation0 should be joined
//   offset1: offsets of which columns the given relation1 should be joined
// Output arguments:
//   sum: sum of relation0.col0 in the final result
func Join(f0, f1 string, offset0, offset1 []int) uint64 {

	htConsumerCh := make(chan [][]string, 1)
	joinConsumerCh := make(chan [][]string, 1)

	go readCSVFiles(f0, htConsumerCh)
	ht := constructHashTable(htConsumerCh, offset0)
	go readCSVFiles(f1, joinConsumerCh)
	return probeHt(ht, joinConsumerCh, offset1)
}

func readCSVFiles(f string, ch chan [][]string) {
	defer close(ch)
	csvFile, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	defer csvFile.Close()

	csvReader := csv.NewReader(csvFile)
	block := make([][]string, 0, blockSize)
	cnt := 0
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		block = append(block, row)
		cnt++
		if cnt == blockSize {
			ch <- block
			cnt = 0
			block = make([][]string, 0, blockSize)
		}
	}
	if len(block) > 0 {
		ch <- block
	}
}

func constructHashTable(htConsumerCh chan [][]string, offset []int) (ht *mvmap.MVMap) {
	var keyBuffer []byte
	valBuffer := make([]byte, 8)
	ht = mvmap.NewMVMap()
	for block := range htConsumerCh {
		for _, row := range block {
			for i, off := range offset {
				if i > 0 {
					keyBuffer = append(keyBuffer, '-')
				}
				keyBuffer = append(keyBuffer, []byte(row[off])...)
			}
			v, err := strconv.ParseUint(row[0], 10, 64)
			if err != nil {
				panic(err)
			}
			*(*int64)(unsafe.Pointer(&valBuffer[0])) = int64(v)
			ht.Put(keyBuffer, valBuffer)
			keyBuffer = keyBuffer[:0]
		}
	}
	return
}

func probeHt(ht *mvmap.MVMap, joinConsumerCh chan [][]string, offset []int) uint64 {
	var hashKey []byte
	var vals [][]byte
	var sum uint64 = 0
	for block := range joinConsumerCh {
		for _, row := range block {
			for i, off := range offset {
				if i > 0 {
					hashKey = append(hashKey, '-')
				}
				hashKey = append(hashKey, []byte(row[off])...)
			}
			vals = ht.Get(hashKey, vals)
			hashKey = hashKey[:0]
			for _, val := range vals {
				v := *(*int64)(unsafe.Pointer(&val[0]))
				sum += uint64(v)
			}
			vals = vals[:0]
		}
	}
	return sum
}
