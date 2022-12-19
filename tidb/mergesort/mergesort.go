package main

import (
	"runtime"
	"sort"
	"sync"
)

var tmp []int64

func merge(src []int64, start int64, m int64, end int64) {
	tmpPos := start
	pos1 := start
	pos2 := m + 1
	for pos1 <= m && pos2 <= end {
		if src[pos1] < src[pos2] {
			tmp[tmpPos] = src[pos1]
			pos1++
		} else {
			tmp[tmpPos] = src[pos2]
			pos2++
		}
		tmpPos++
	}
	for pos1 <= m {
		tmp[tmpPos] = src[pos1]
		tmpPos++
		pos1++
	}
	for pos2 <= end {
		tmp[tmpPos] = src[pos2]
		tmpPos++
		pos2++
	}
	for i := start; i <= end; i++ {
		src[i] = tmp[i]
		tmpPos++
	}
}

func split(src []int64, start int64, end int64) {
	if start >= end {
		return
	}
	m := (end-start)/2 + start
	split(src, start, m)
	split(src, m+1, end)
	merge(src, start, m, end)
}

// MergeSort performs the merge sort algorithm.
// Please supplement this function to accomplish the homework.
func MergeSort(src []int64) {
	// split and merge in every block
	n := runtime.NumCPU()
	l := len(src)
	tmp = make([]int64, l)
	if l < 3*n {
		sort.SliceStable(src, func(i, j int) bool {
			return src[i] < src[j]
		})
		return
	}

	pos := make([]int64, n)
	wg := sync.WaitGroup{}
	step := l / len(pos)
	if l%len(pos) != 0 {
		step++
	}
	for i := 0; i < len(pos); i++ {
		wg.Add(1)
		pos[i] = int64(i * step)
		go func(idx int) {
			if idx*step+step-1 >= l {
				split(src, int64(idx*step), int64(l-1))
			} else {
				split(src, int64(idx*step), int64(idx*step+step-1))
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	// merge large blocks
	for len(pos) > 1 {
		var tmpPos []int64
		i := 0
		for i+1 < len(pos) {
			wg.Add(1)
			go func(idx int) {
				if 2*pos[idx+1]-pos[idx]-1 >= int64(l) {
					merge(src, pos[idx], pos[idx+1]-1, int64(l-1))
				} else {
					merge(src, pos[idx], pos[idx+1]-1, 2*pos[idx+1]-pos[idx]-1)
				}
				wg.Done()
			}(i)
			if i%2 == 0 {
				tmpPos = append(tmpPos, pos[i])
			}
			i += 2
		}
		wg.Wait()
		if i < len(pos) {
			tmpPos = append(tmpPos, pos[i])
		}
		pos = tmpPos
	}
}
