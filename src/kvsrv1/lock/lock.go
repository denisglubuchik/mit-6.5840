package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	lockname string
	ownerID string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	ownerID := kvtest.RandValue(8)
	lk := &Lock{ck: ck, lockname: lockname, ownerID: ownerID}
	return lk
}

func (lk *Lock) Acquire() {
	key := lk.lockname
	for {
		val, ver, _ := lk.ck.Get(key)

		if val == "" {
			ok := lk.ck.Put(key, lk.ownerID, ver)
			if ok == rpc.OK {
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	key := lk.lockname
	val, ver, err := lk.ck.Get(key)
	if err == rpc.ErrNoKey {
		return
	}
	if lk.ownerID == val {
		ok := lk.ck.Put(key, "", ver)
		if ok == "" {
			return
		}
	}
}
