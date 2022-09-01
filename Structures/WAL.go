package Structures

import (
	"encoding/binary"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value 0 - append 1 - deleted
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
*/

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WAL struct {
	SegmentSize int64
	files       []string
}

// kreiranje WAL-a/ucitavanje podataka ukoliko postoje
func Innit(SegmentSize int64, files []string) *WAL {
	allFiles, err := ioutil.ReadDir("./Data/WAL")
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range allFiles {
		files = append(files, f.Name())
	}
	if len(files) == 0 {
		file, err2 := os.Create("./Data/WAL/wal_1.log")
		if err2 != nil {
			panic(err)
		}
		file.Close()
		files = append(files, "wal_1.log")
	}
	return &WAL{
		SegmentSize: SegmentSize,
		files:       files,
	}
}

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// kreiranje niza bajtova
func (w *WAL) Insert(key string, value []byte, tmbStn string) bool {
	activeFile := w.files[len(w.files)-1]
	r, err := os.Stat("./data/WAL/" + activeFile)
	if err != nil {
		panic(err)
	}

	if r.Size() > w.SegmentSize {
		activeFile = "wal_" + strconv.Itoa(len(w.files)+1) + ".log"
		file, err2 := os.Create("./data/WAL/" + activeFile)
		if err2 != nil {
			panic(err)
		}
		file.Close()
		w.files = append(w.files, activeFile)
	}

	var crc = CRC32(value)
	var now = time.Now()
	var timestamp = now.Unix()

	bytes := make([]byte, 37)
	binary.BigEndian.PutUint32(bytes[:], crc)
	binary.BigEndian.PutUint64(bytes[4:], uint64(timestamp))
	var tombStone = []byte{0}
	if tmbStn == "d" {
		tombStone = []byte{1}
	}
	bytes[20] = tombStone[0]
	binary.BigEndian.PutUint64(bytes[21:], uint64(len([]byte(key))))
	binary.BigEndian.PutUint64(bytes[29:], uint64(len(value)))

	bytes = append(bytes, []byte(key)...)
	bytes = append(bytes, value...)
	file, err := os.OpenFile("./data/WAL/"+activeFile, os.O_RDWR, 0777)
	defer file.Close()
	if err != nil {
		panic(err.Error())
	}
	Append(file, bytes)
	return true
}

// dodavanje novog podatka (niza bajtova) u WAL
func Append(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	mmapf, err := mmap.MapRegion(file, int(currentLen)+len(data), mmap.RDWR, 0, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	mmapf.Flush()
	return nil
}

// brisanje svih segmenta WAL-a
func (w *WAL) Delete() {
	lastSegment := w.files[len(w.files)-1]
	for _, seg := range w.files {
		if seg != lastSegment {
			err := os.Remove("./Data/WAL/" + seg)

			if err != nil {
				panic(err)
			}
		}
	}
	err := os.Rename("./Data/WAL/"+lastSegment, "./Data/WAL/wal_1.log")
	if err != nil {
		panic(err)
	}

	w.files = nil
	w.files = append(w.files, "wal_1.log")
}

func (w *WAL) Read() map[string][]byte {
	retMap := make(map[string][]byte)
	for _, activeFile := range w.files {
		file, err := os.OpenFile("./Data/WAL/"+activeFile, os.O_RDWR, 0777)
		defer file.Close()
		for {
			if err != nil {
				panic(err.Error())
			}
			crc := make([]byte, 4)
			file.Read(crc)

			c := binary.BigEndian.Uint32(crc)
			if c == 0 {
				break
			}

			time := make([]byte, 16)
			file.Read(time)

			tmbStone := make([]byte, 1)
			file.Read(tmbStone)

			keySiz := make([]byte, 8)
			file.Read(keySiz)
			n := binary.BigEndian.Uint64(keySiz)

			valueSiz := make([]byte, 8)
			file.Read(valueSiz)
			m := binary.BigEndian.Uint64(valueSiz)

			key := make([]byte, n)
			file.Read(key)
			sKey := string(key)

			value := make([]byte, m)
			file.Read(value)
			if (CRC32(value)) != c {
				continue
			}
			if tmbStone[0] == 0 {
				retMap[sKey] = value
			} else if tmbStone[0] == 1 {
				delete(retMap, sKey)
			}
		}
	}
	return retMap
}
