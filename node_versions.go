package iavl

import (
	"errors"
	"fmt"
)

// not Thread-Safe
type NodeVersions struct {
	nums []int
	changes map[int64]int  // version -> num, changes will be merged to nums when commit

	maxVersions int
	maxNodes int

	firstVersion   int64
	nextVersion    int64
	nextVersionIdx int

	totalNodes int
}

func NewNodeVersions(maxVersions int, maxNodes int, lastVersion int64) *NodeVersions {
	return &NodeVersions{
		nums: make([]int, maxVersions, maxVersions),
		changes: make(map[int64]int, 32),
		maxVersions: maxVersions,
		maxNodes:maxNodes,

		firstVersion:   lastVersion,
		nextVersion:    lastVersion+1,
		nextVersionIdx: 0,
		totalNodes:     0,
	}
}

func (nv *NodeVersions) Inc1(version int64) {
	nv.changes[version]++
}

func (nv *NodeVersions) Inc(version int64, times int) {
	nv.changes[version] += times
}

func (nv *NodeVersions) Dec1(version int64) {
	nv.changes[version]--
}

func (nv *NodeVersions) Dec(version int64, times int) {
	nv.changes[version] -= times
}

func (nv *NodeVersions) Update(fromVersion, toVersion int64) {
	nv.changes[fromVersion]--
	nv.changes[toVersion]++
}

func (nv *NodeVersions) Commit(newVersion int64) (maxPruneVersion int64, err error) {
	if newVersion != nv.nextVersion {
		return 0, fmt.Errorf("expect version %d, got %d", nv.nextVersion, newVersion)
	}

	for version, num := range nv.changes {
		if version > nv.nextVersion {
			// should not happen
			return 0, fmt.Errorf("some changes happen on a future version %d, latest version is %d", version, newVersion)
		} else if version == nv.nextVersion {
			continue
			// skip it first, will handle this version later to avoid losing the original num of version (nv.nextVersion - nv.maxVersions)
		}
		if num == 0 {
			continue
		}

		versionIdx := nv.getIndex(version)
		if versionIdx < 0 {
			// skip the versions we do not need
			continue
		}

		nv.nums[versionIdx] += num
		nv.totalNodes += num

		if version < nv.firstVersion {
			// some old version may be loaded in this round.
			nv.firstVersion = version
		}
	}

	maxPruneVersion, _ = nv.prune()
	nv.nums[nv.nextVersionIdx] = nv.changes[nv.nextVersion]
	nv.changes = make(map[int64]int, len(nv.changes))
	nv.nextVersion++
	return maxPruneVersion, nil
}

func (nv *NodeVersions) prune() (maxPruneVersion int64, prunedNum int) {
	if nv.totalNodes <= nv.maxNodes {
		if minVersionNum := nv.nums[nv.nextVersionIdx]; minVersionNum == 0 {
			return -1, 0
		} else {
			return nv.nextVersion - int64(nv.maxVersions), minVersionNum
		}
	}
	toPruneNum := nv.totalNodes - nv.maxNodes
	i:= nv.getIndex(nv.firstVersion)  // start from the idx of firstVersion to skip some zero nums.
	for {
		if nv.nums[i] > 0 {
			prunedNum += nv.nums[i]
			nv.nums[i] = 0
			if prunedNum >= toPruneNum { // the actual prune num would be equal or more than toPruneNum
				break
			}
		}
		if i < nv.maxVersions {
			i++
		} else {
			i =	0
		}
	}
	maxPruneVersion = nv.getVersion(i)
	nv.firstVersion = maxPruneVersion + 1
	return maxPruneVersion, prunedNum
}

func (nv *NodeVersions) getIndex(version int64) int {
	if version < nv.nextVersion- int64(nv.maxVersions) {
		return -1
	}
	idx := nv.nextVersionIdx - int(nv.nextVersion- version)
	if idx < 0 {
		idx += nv.maxVersions
	}
	return idx
}

func (nv *NodeVersions) getVersion(idx int) int64 {
	if idx >= nv.nextVersionIdx {
		return nv.nextVersion - int64(nv.maxVersions) + int64(idx - nv.nextVersionIdx)
	}
	return nv.nextVersion - int64(nv.nextVersionIdx - idx)
}

type Int64Ring struct {
	buf  []int64
	tail int
	cap  int
}

func NewInt64Ring(cap int) *Int64Ring {
	return &Int64Ring{
		buf:  make([]int64, 0, cap),
		tail: 0,
		cap:  cap,
	}
}

func (q *Int64Ring) Push(v int64) {
	if len(q.buf) < q.cap {
		q.buf = append(q.buf, v)
		q.tail++
	} else {
		q.buf[q.tail] = v
		q.tail = (q.tail + 1) % q.cap
	}
}

func (q *Int64Ring) Get(idx int) (int64, error) {
	if len(q.buf) < q.cap {
		if idx >= len(q.buf) {
			return 0, errors.New("index out of range")
		}
		return q.buf[idx], nil
	}
	return q.buf[(q.tail+idx)%q.cap], nil
}

func (q *Int64Ring) Set(idx int, v int64) error {
	if len(q.buf) < q.cap {
		if idx >= len(q.buf) {
			return errors.New("index out of range")
		}
		q.buf[idx] = v
	} else {
		q.buf[(q.tail+idx)%q.cap] = v
	}
	return nil
}


