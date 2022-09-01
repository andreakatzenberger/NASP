package Structures

import (
	"fmt"
	"math/rand"
	"time"
)

type SkipListNode struct {
	key       string
	value     []byte
	timestamp int64
	tombstone byte
	next      []*SkipListNode
}

//kreira novi element pomocu prosledjenih vrednosti
func createNode(key string, value []byte, timestamp int64, level int) *SkipListNode {
	return &SkipListNode{
		key:       key,
		value:     value,
		timestamp: timestamp,
		tombstone: 0,
		next:      make([]*SkipListNode, level),
	}
}

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

//kreira novu, praznu skiplistu
func CreateSkipList(maxHeight int) *SkipList {
	head := createNode("", nil, 0, maxHeight+1)
	return &SkipList{
		maxHeight: maxHeight,
		height:    1,
		size:      0,
		head:      head,
	}
}

//trazi element sa zadatim kljucem
func (s *SkipList) Find(key string) *SkipListNode {
	curr := s.head
	for i := s.height; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].key < key {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]
	if curr != nil {
		if curr.key == key && curr.tombstone == 0 {
			return curr //ako postoji vraca element
		}
	}
	return nil //ako ne postoji vraca nil
}

//brise element sa zadatim kljucem menjajuci vrednost tombstonea na true (1)
func (s *SkipList) Delete(key string) bool {
	elem := s.Find(key)
	if elem == nil {
		foundElem := GetFromSSTable(key)
		if foundElem == nil {
			return false
		} else {
			//level := s.roll()
			//now := time.Now()
			//newDeleted := createNode(key, foundElem, now.Unix(), level+1)
			//newDeleted.tombstone = 1
			//s.Add(newDeleted.key, newDeleted.value)
			level := s.roll()
			if level > s.maxHeight {
				level = s.maxHeight
			}
			newNode := createNode(key, foundElem, time.Now().Unix(), level+1)
			newNode.tombstone = 1
			s.addLevels(newNode, level)
			s.size++
			return true
		}
	} else {
		elem.tombstone = 1
		now := time.Now()
		elem.timestamp = now.Unix()
		return true
	}
}

//dodaje zadati element na zadati broj nivoa
func (s *SkipList) addLevels(node *SkipListNode, level int) {
	curr := s.head
	if s.size == 0 { //ako se dodaje prvi element
		for i := 0; i <= level; i++ {
			curr.next[i] = node
		}
	} else {
		for i := level; i >= 0; i-- {
			if curr.next[i] == nil {
				curr.next[i] = node
			} else {
				for curr.next[i].key < node.key {
					curr = curr.next[i]
					if curr.next[i] == nil {
						break
					}
				}
				node.next[i] = curr.next[i]
				curr.next[i] = node
			}
			curr = s.head
		}
	}
}

//dodaje element sa zadatim kljucem i vrednoscu
func (s *SkipList) Add(key string, value []byte) bool {
	elem := s.Find(key)
	if elem == nil { //ako element nije vec u listi dodaje se
		level := s.roll()
		if level > s.maxHeight {
			level = s.maxHeight
		}
		now := time.Now()
		newNode := createNode(key, value, now.Unix(), level+1)
		s.addLevels(newNode, level)
		s.size++
		return true
	} else { //ako element jeste vec u listi menjaju mu se vrednost
		now := time.Now()
		elem.timestamp = now.Unix()
		elem.value = value
		elem.tombstone = 0
		return false
	}
}

//ispisuje sve nivoe skipliste
func (s *SkipList) Print() {
	for i := s.height; i >= 0; i-- {
		curr := s.head
		fmt.Print("[")
		for curr.next[i] != nil {
			if curr.next[i].tombstone == 0 {
				fmt.Print("Kljuc: ", curr.next[i].key, " vrednost: ", curr.next[i].value, ", ")
			}
			curr = curr.next[i]
		}
		fmt.Print("]\n")
	}
}

//vraca random broj za odredjivanje broja nivoa u kojima ce se element nalaziti
func (s *SkipList) roll() int {
	level := 0
	for ; rand.Int31n(2) == 1; level++ {
		if level > s.height {
			s.height = level
			return level
		}
	}
	return level
}

//prazni skip listu
func (s *SkipList) Empty() {
	head := createNode("", nil, 0, s.maxHeight+1)
	s.size = 0
	s.height = 1
	s.head = head
}

//vraca listu svih elementa skip liste
func (s *SkipList) GetAll() []SkipListNode {
	curr := s.head
	allElements := []SkipListNode{}
	for i := s.height; i >= 0; i-- {
		for curr.next[i] != nil {
			curr = curr.next[i]
			allElements = append(allElements, *curr)
		}
	}
	return allElements
}

//listu svih elemenata liste pretvara u listu zapisa
func (s *SkipList) SLNodeToRecord() []Record {
	allRecords := []Record{}
	allNodes := s.GetAll()
	for i := 0; i < len(allNodes); i++ {
		newRecord := CreateRecord(allNodes[i].key, allNodes[i].value, allNodes[i].tombstone)
		allRecords = append(allRecords, *newRecord)
	}
	return allRecords
}
