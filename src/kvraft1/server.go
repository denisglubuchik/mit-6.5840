package kvraft

import (
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type Tvalue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	mu      sync.Mutex
	storage map[string]Tvalue
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply

		key := args.Key
		res, ok := kv.storage[key]
		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Value = res.Value
		reply.Version = res.Version
		return reply

	case rpc.PutArgs:
		var reply rpc.PutReply

		key := args.Key
		res, ok := kv.storage[key]
		if !ok {
			if args.Version == 0 {
				val := Tvalue{Value: args.Value, Version: args.Version + 1}
				kv.storage[key] = val
				return reply
			}
			reply.Err = rpc.ErrNoKey
			return reply
		}

		if res.Version != args.Version {
			reply.Err = rpc.ErrVersion
			return reply
		}
		val := Tvalue{Value: args.Value, Version: args.Version + 1}
		kv.storage[key] = val

		return reply

	default:
		panic(fmt.Sprintf("DoOp: unexpected request type %T", req))
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:      me,
		storage: make(map[string]Tvalue),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
