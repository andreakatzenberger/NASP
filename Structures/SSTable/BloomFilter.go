package SSTable

import (
	"Projekat/Handling"
	"Projekat/Structures"
	"encoding/gob"
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"time"
)

type BloomFilter struct {
	M         uint
	K         uint
	Set       []byte
	hFunc     []hash.Hash32
	TimeConst uint
}

//Izračunaj veličinu bit seta M
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

//Izračunaj optimalan broj hash funkcija k
func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

//Kreiraj hash funkciju
func CreateHashFunctions(k uint, ts uint) ([]hash.Hash32, uint) {
	var h []hash.Hash32
	if ts == 0 {
		ts = uint(time.Now().Unix())
	}
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h, ts
}

//Hashiranje, koristimo za dodavanje i pretragu
func HashIt(hashF hash.Hash32, record string, m uint) uint32 {
	_, err := hashF.Write([]byte(record))
	Handling.PanicError(err)

	i := hashF.Sum32() % uint32(m)
	hashF.Reset()
	return i
}

//Kreiranje bloom filtera, navodimo veličinu i falsepositive rate
func CreateBloomFilter(expectedElements uint, falsePositiveRate float64) *BloomFilter {
	filter := BloomFilter{}
	filter.M = CalculateM(int(expectedElements), falsePositiveRate)
	filter.K = CalculateK(int(expectedElements), filter.M)
	filter.hFunc, filter.TimeConst = CreateHashFunctions(filter.K, filter.TimeConst)
	filter.Set = make([]byte, filter.M)
	return &filter
}

//Dodavanje ključa u bloom filter
func (filter *BloomFilter) Add(record Structures.Record) {
	for _, hashF := range filter.hFunc {
		i := HashIt(hashF, record.Key, filter.M)
		filter.Set[i] = 1
	}
}

//Pretraga ključa u bloom filteru
func (filter *BloomFilter) Search(record string) bool {
	for _, hashF := range filter.hFunc {
		i := HashIt(hashF, record, filter.M)
		if filter.Set[i] != 1 {
			return false
		}
	}
	return true
}

//Upis svih ključeva u bloom filter
func (filter *BloomFilter) WriteRecordsToBloomFilter(records *[]Structures.Record) BloomFilter {
	for _, record := range *records {
		filter.Add(record)
	}
	return *filter
}

//Zapis bloom filtera u fajl
func WriteBloomFilter(filePath string, filter *BloomFilter) {
	file := Handling.CreateFile(filePath)
	defer file.Close()

	encoder := gob.NewEncoder(file)
	err := encoder.Encode(filter)
	Handling.PanicError(err)
}

//Čitanje bloom filtera iz fajla
func ReadBloomFilter(filePath string) BloomFilter {
	file := Handling.OpenFile(filePath)
	defer file.Close()

	decoder := gob.NewDecoder(file)
	filter := BloomFilter{}
	err := decoder.Decode(&filter)
	Handling.PanicError(err)

	filter.hFunc, _ = CreateHashFunctions(filter.K, filter.TimeConst)
	err = file.Close()
	if err != nil {
		Handling.PanicError(err)
	}
	return filter
}

//Pretraga ključa u bloom filter fajlu
func CheckKeyInFilterFile(recordKey string, filePath string) bool {
	filter := ReadBloomFilter(filePath)
	found := filter.Search(recordKey)
	return found
}
