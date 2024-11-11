package gcl

import (
	"math/rand"
	"sync"
	"testing"
)

func TestRandomLv(t *testing.T) {
	var cnt [numLevel]int
	N := 1000000
	for i := 0; i < N; i++ {
		lv := randomLv()
		cnt[lv] += 1
	}
	if cnt[0] <= N/2 {
		t.Errorf("cnt[0] = %d, want > %d", cnt[0], N/2)
	}
}

func TestLazySkipList(t *testing.T) {
	// increase the following to stress
	const (
		loopCnt = 1000
		thCnt   = 10
		kRange  = 10000
	)

	list := NewSkipList[int64, int64](
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
