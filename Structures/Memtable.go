package Structures

type Memtable struct {
	structure *SkipList
	threshold float32
	maxSize   int
}

//kreira nov, prazan memtable
func CreateMemtable(maxHeight int, threshold float32, maxSize int) *Memtable {
	return &Memtable{
		structure: CreateSkipList(maxHeight),
		threshold: threshold,
		maxSize:   maxSize,
	}
}

//dodaje element u memtable
func (m Memtable) Add(key string, value []byte) bool {
	if m.structure.size == 0 {
		m.structure.Add(key, value)
		return false
	} else {
		percentage := (float64(m.structure.size) / float64(m.maxSize)) * 100
		if float32(percentage) >= m.threshold { //proverava popunjenost memtablea
			m.Flush()
			m.structure.Add(key, value)
			return true
		} else {
			m.structure.Add(key, value)
			return false
		}
	}

}

//trazi element u memtableu
func (m *Memtable) Find(key string) []byte {
	elem := m.structure.Find(key)
	if elem == nil {
		return nil
	} else {
		return elem.value
	}
}

//brise element iz memtablea
func (m *Memtable) Delete(key string) bool {
	return m.structure.Delete(key)
}

func (m *Memtable) Flush() {
	allRecords := m.structure.SLNodeToRecord()
	PutToSSTable(allRecords)
	m.structure.Empty()
}
