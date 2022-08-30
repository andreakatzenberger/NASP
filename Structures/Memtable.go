package Structures

import (
	"fmt"
)

type Memtable struct {
	structure *SkipList
	threshold float32
	size      int
	maxSize   int
}

//kreira nov, prazan memtable
func CreateMemtable(maxHeight int, threshold float32, maxSize int) *Memtable {
	return &Memtable{
		structure: CreateSkipList(maxHeight),
		threshold: threshold,
		size:      0,
		maxSize:   maxSize,
	}
}

//dodaje element u memtable
func (m *Memtable) Add(key string, value []byte) {
	percentage := (m.maxSize / m.size) * 100
	if float32(percentage) >= m.threshold { //proverava popunjenost memtablea
		//m.Flush()
	} else {
		if m.structure.Find(key) != nil {
			m.structure.Add(key, value)
			m.size++
		}
	}
}

//trazi element u memtableu
func (m *Memtable) Find(key string) []byte {
	elem := m.structure.Find(key)
	if elem == nil {
		fmt.Println("Element sa zadacim kljucem ne postoji u memtableu.")
		return nil
	} else {
		return GetValue(elem)
	}
}

//brise element iz memtablea
func (m *Memtable) Delete(key string) {
	m.structure.Delete(key)
}

//func (m *Memtable) Flush() ? {
//	data := m.structure.GetAll()
//	m.size = 0
//	m.structure.Empty()
//	return data
//}
