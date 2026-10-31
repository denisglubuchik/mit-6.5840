package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Tvalue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	storage map[string]Tvalue
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.storage = make(map[string]Tvalue)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	res, ok := kv.storage[key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = res.Value
	reply.Version = res.Version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key

	res, ok := kv.storage[key]
	if !ok {
		if args.Version == 0 {
			val := Tvalue{Value: args.Value, Version: args.Version + 1}
			kv.storage[key] = val
			return
		}
		reply.Err = rpc.ErrNoKey
		return
	}

	if res.Version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}
	val := Tvalue{Value: args.Value, Version: args.Version + 1}
	kv.storage[key] = val
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
