package gcl

import (
	"math/rand"
	"sync"
	"testing"
)

func TestLazyList(t *testing.T) {
	const (
		loopCnt = 1000
		thCnt   = 10
		kRange  = 20
	)

	list := NewLazyList[int64, int64](
		func(a, b int64) bool { return a < b },
		func(a, b int64) bool { return a == b },
	)

	type result struct {
		insOk      int
		insMiss    int
		lookupOk   int
		lookupMiss int
		remOk      int
		remMiss    int
	}

	var results [thCnt]result

	var wg sync.WaitGroup
	for i := 0; i < thCnt; i++ {
		wg.Add(1)
		go func(ii int) {
			defer wg.Done()
			for j := 0; j < loopCnt; j++ {
				ikey := rand.Int63() % kRange
				ok := list.Add(ikey, int64(ii))
				if ok {
					results[ii].insOk += 1
				} else {
					results[ii].insMiss += 1
				}

				ival, lkok := list.Lookup(ikey)
				if lkok {
					results[ii].lookupOk += 1
				} else {
					results[ii].lookupMiss += 1
				}

				if lkok && ival != int64(ii) {
					delOk := list.Remove(ikey)
					if delOk {
						results[ii].remOk += 1
					} else {
						results[ii].remMiss += 1
					}
				}
			}
		}(i)
	}
	wg.Wait()

	cnt := 0
	for x := int64(0); x < kRange; x++ {
		_, ok := list.Lookup(x)
		if ok {
			cnt += 1
		}
	}

	insCnt := 0
	remCnt := 0
	for _, r := range results {
		insCnt += r.insOk
		remCnt += r.remOk
	}

	if remCnt+cnt != insCnt {
		t.Errorf("counting error %d %d %d", remCnt, cnt, insCnt)
	}
}
