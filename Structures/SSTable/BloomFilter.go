package SSTable

import (
	"Projekat/Handling"
	"Projekat/Structures"
	"encoding/gob"
	"fmt"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"os"
	"strconv"
	"time"
)

type BloomFilter struct {
	M         uint
	K         uint
	P         float64
	Set       []byte
	hFunc     []hash.Hash32
	TimeConst uint
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h, ts
}

func CopyHashFunctions(k uint, tc uint) []hash.Hash32 {
	var h []hash.Hash32
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(tc+1)))
	}
	return h
}

func HashIt(hashF hash.Hash32, record string, m uint) uint32 {
	_, err := hashF.Write([]byte(record))
	Handling.PanicError(err)

	i := hashF.Sum32() % uint32(m)
	hashF.Reset()
	return i
}

func CreateBloomFilter(expectedElements uint, falsePositiveRate float64) *BloomFilter {
	filter := BloomFilter{}
	filter.M = CalculateM(int(expectedElements), falsePositiveRate)
	filter.K = CalculateK(int(expectedElements), filter.M)
	filter.hFunc, filter.TimeConst = CreateHashFunctions(filter.K)
	filter.Set = make([]byte, filter.M)
	filter.P = falsePositiveRate
	return &filter
}

func (filter *BloomFilter) Add(record Structures.Record) {
	for _, hashF := range filter.hFunc {
		i := HashIt(hashF, record.Key, filter.M)
		filter.Set[i] = 1
	}
}

func (filter *BloomFilter) Search(record string) bool {
	for _, hashF := range filter.hFunc {
		i := HashIt(hashF, record, filter.M)
		if filter.Set[i] != 1 {
			return false
		}
	}
	return true
}

func (filter *BloomFilter) WriteRecordsToBloomFilter(records *[]Structures.Record) BloomFilter {
	for _, record := range *records {
		filter.Add(record)
	}
	return *filter
}

func WriteBloomFilter(fileName string, filter *BloomFilter) {
	file, err := os.Create(fileName)
	Handling.PanicError(err)
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err = encoder.Encode(filter)
	Handling.PanicError(err)
}

func ReadBloomFilter(fileName string) (filter *BloomFilter) {
	file, err := os.Open(fileName)
	Handling.PanicError(err)
	defer file.Close()

	decoder := gob.NewDecoder(file)
	filter = new(BloomFilter)
	_, err = file.Seek(0, 0)
	Handling.ReturnError(err)

	for {
		err = decoder.Decode(filter)
		if err != nil {
			break
		}
	}
	filter.hFunc = CopyHashFunctions(filter.K, filter.TimeConst)
	return
}

func CheckKeyInFilterFile(recordKey string, filePath string) bool {
	filter := ReadBloomFilter(filePath)
	found := filter.Search(recordKey)
	return found
}

func TestBloomFilter() {

	filter := CreateBloomFilter(100, 0.05)

	for i := 0; i < 100; i++ {
		bytes := []byte{1, 2}
		record := Structures.CreateRecord(strconv.Itoa(i), bytes, 0)
		filter.Add(*record)
	}

	WriteBloomFilter("testBloomFilter.gob", filter)
	fmt.Println(filter.Search("99"))
	fmt.Println(filter.Search("124"))

	newFiler := ReadBloomFilter("testBloomFilter.gob")
	fmt.Println(newFiler.Search("99"))
	fmt.Println(newFiler.Search("Mi nismo andjeli"))

}
