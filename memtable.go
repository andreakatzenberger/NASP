package memtable

import (
	"fmt"
	"skiplist"
)

type Memtable struct {
	structure *skiplist.SkipList
	threshold float32
	size      int
	maxSize   int
}

func CreateMemtable(maxHeight int, threshold float32, maxSize int) *Memtable {
	return &Memtable{
		structure: skiplist.CreateSkipList(maxHeight),
		threshold: threshold,
		size:      0,
		maxSize:   maxSize,
	}
}

func (m *Memtable) Add(key string, value []byte) {
	percentage := (m.maxSize / m.size) * 100
	if float32(percentage) >= m.threshold {
		//m.Flush()
	} else {
		if m.structure.Find(key) != nil {
			m.structure.Add(key, value)
			m.size++
		}
	}
}

func (m *Memtable) Find(key string) []byte {
	elem := m.structure.Find(key)
	if elem == nil {
		fmt.Println("Element sa zadacim kljucem ne postoji u memtableu.")
		return nil
	} else {
		return skiplist.GetValue(elem)
	}
}

func (m *Memtable) Delete(key string) {
	m.structure.Delete(key)
}

//func (m *Memtable) Flush() ? {
//	data := m.structure.GetAll()
//	m.size = 0
//	m.structure.Empty()
//	return data
//}
