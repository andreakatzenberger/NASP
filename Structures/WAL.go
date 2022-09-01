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

// Izmeni
// const (
//
// T_SIZE = 8
// C_SIZE = 4
//
// CRC_SIZE       = T_SIZE + C_SIZE
// TOMBSTONE_SIZE = CRC_SIZE + 1
// KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
// VALUE_SIZE     = KEY_SIZE + T_SIZE
//
// DEFAULT_SEGMENT_SIZE = 100
//
// )
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WAL struct {
	SegmentSize int64
	filesSlice  []string
}

func Innit(SegmentSize int64, filesSlice []string) *WAL {
	files, err := ioutil.ReadDir("./Data/WAL")
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range files {
		filesSlice = append(filesSlice, f.Name())
	}
	if len(filesSlice) == 0 {
		file, err2 := os.Create("./Data/WAL/wal_1.log")
		if err2 != nil {
			panic(err)
		}
		file.Close()
		filesSlice = append(filesSlice, "wal_1.log")
	}
	return &WAL{
		SegmentSize: SegmentSize,
		filesSlice:  filesSlice,
	}
}

func writeWall(file *os.File, data []byte) error {
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

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (w *WAL) Insert(key string, value []byte, tmbStn string) bool {
	activeFile := w.filesSlice[len(w.filesSlice)-1]
	r, err := os.Stat("./Data/WAL/" + activeFile)
	if err != nil {
		panic(err)
	}
	if r.Size() > w.SegmentSize {
		activeFile = "wal_" + strconv.Itoa(len(w.filesSlice)+1) + ".log"
		file, err2 := os.Create("./Data/WAL/" + activeFile)
		if err2 != nil {
			panic(err)
		}
		file.Close()
		w.filesSlice = append(w.filesSlice, activeFile)
	}

	var keySize = uint64(len([]byte(key)))
	var valueSize = uint64(len(value))
	var now = time.Now()
	var timestamp = now.Unix()
	var crc = CRC32(value)

	fileBytes := make([]byte, 37)
	binary.BigEndian.PutUint32(fileBytes[:], crc)
	binary.BigEndian.PutUint64(fileBytes[4:], uint64(timestamp))

	var tombStone = []byte{0}
	if tmbStn == "d" {
		tombStone = []byte{1}
	}
	fileBytes[20] = tombStone[0]

	binary.BigEndian.PutUint64(fileBytes[21:], keySize)
	binary.BigEndian.PutUint64(fileBytes[29:], valueSize)
	fileBytes = append(fileBytes, []byte(key)...)
	fileBytes = append(fileBytes, value...)

	file, err := os.OpenFile("./Data/WAL/"+activeFile, os.O_RDWR, 0777)
	//file, err := os.OpenFile("./Data/WAL/"+activeFile, os.O_APPEND, 0777)
	defer file.Close()
	if err != nil {
		panic(err.Error())
	}
	writeWall(file, fileBytes)
	return true
}

func (w *WAL) DeleteSegments() {
	lastSegment := w.filesSlice[len(w.filesSlice)-1]
	for _, seg := range w.filesSlice {
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

	w.filesSlice = nil
	w.filesSlice = append(w.filesSlice, "wal_1.log")
}

func (w *WAL) Read() map[string][]byte {
	retMap := make(map[string][]byte)
	for _, activeFile := range w.filesSlice {
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
