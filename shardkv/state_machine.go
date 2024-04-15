package shardkv

/*

 */

type MemoryKVStateMachine struct {
	KV     map[string]string
	Status ShardStatus
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV:     make(map[string]string),
		Status: Normal,
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	MyLog(-1, DMachine, "\033[35mApply Get to State_machine\033[0m")

	if value, ok := mkv.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	MyLog(-1, DMachine, "\033[35mApply Put to State_machine\033[0m")

	mkv.KV[key] = value
	return OK
}

func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	MyLog(-1, DMachine, "\033[35mApply Append to State_machine\033[0m")

	mkv.KV[key] += value
	return OK
}

func (mkv *MemoryKVStateMachine) copyData() map[string]string {
	newKV := make(map[string]string)
	for k, v := range mkv.KV {
		newKV[k] = v
	}
	return newKV
}
