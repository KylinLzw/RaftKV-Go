/*
1. 状态机：用于存储 Raft 层传来的指令，存储数据
	MemoryKVStateMachine： 表示状态机的数据结构
2. 对外接口：
	NewMemoryKVStateMachine ：初始化状态机接口
	Get ：获取数据的接口
	Put ：存储数据的接口
	Append ：修改数据的接口
*/

package kvraft

// MemoryKVStateMachine ：状态机数据结构
// 状态机，在项目中使用基于内存的map存储
type MemoryKVStateMachine struct {
	KV map[string]string
}

// NewMemoryKVStateMachine ：初始化状态机接口
// 用于初始化状态机
func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string),
	}
}

// Get ：获取数据的接口
// 根据 Key 获取状态机存储的 Value
func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := mkv.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

// Put ：存储数据的接口
// 根据 Key 存储 Value
func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	mkv.KV[key] = value
	return OK
}

// Append ：修改数据的接口
// 根据 Key 修改状态机存储的 Value，采用追加的方式
func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	mkv.KV[key] += value
	return OK
}
