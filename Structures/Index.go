package Structures

import (
	error "../Handling"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

type IndexTableEntry struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

//Veličina index entry-a
func (indexEntry *IndexTableEntry) GetSize() uint64 {
	return 8 + indexEntry.KeySize + 8
}

//Kreiraj index entry pomoću zapisa
func CreateIndexEntry(record *Record, offSet uint64) IndexTableEntry {
	indexEntry := IndexTableEntry{KeySize: uint64(len(record.Key)), Key: record.Key, Offset: offSet}
	return indexEntry
}

//Serijalizacija index entry-a i zapis u  fajl
func (indexEntry *IndexTableEntry) WriteEntryToIndexFile(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, indexEntry.KeySize)
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, []byte(indexEntry.Key))
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, indexEntry.Offset)
	error.PanicError(err)
}

//Deserijalizacija index entry-a iz fajla
func (indexEntry *IndexTableEntry) ReadEntryFromIndexFile(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &indexEntry.KeySize)
	if error.EOFError(err) == true {
		return true
	}

	keyByteSlice := make([]byte, indexEntry.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if error.EOFError(err) == true {
		return true
	}
	indexEntry.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &indexEntry.Offset)
	if error.EOFError(err) == true {
		return true
	}

	return false
}

//Pronadji offset za ključ iz Data fajla
func getOffsetInDataTableForKey(key string, filePath string, offset uint64, intervalSize uint64) (uint64, bool) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
		return 0, false
	}
	defer file.Close()
	reader := bufio.NewReader(file)

	_, err = file.Seek(int64(offset), 0)
	if err != nil {
		return 0, false
	}

	tmpIndexEntry := IndexTableEntry{}
	for i := uint64(0); i < intervalSize; i++ {
		eof := tmpIndexEntry.ReadEntryFromIndexFile(reader)
		if eof {
			return 0, false
		}

		if tmpIndexEntry.Key == key {
			return tmpIndexEntry.Offset, true
		}
	}
	return 0, false
}

//Ispiši index entry
func (indexEntry *IndexTableEntry) Print(i int) {
	fmt.Println("Entry", i)
	fmt.Println("Key size:", indexEntry.KeySize)
	fmt.Println("Key:", indexEntry.Key)
	fmt.Println("Offset:", indexEntry.Offset)
	fmt.Println()
}

//Ispiši index entry iz index fajla
func PrintIndexFile(indexFilePath string) {
	file, err := os.Open(indexFilePath)
	error.PanicError(err)

	defer file.Close()
	reader := bufio.NewReader(file)

	i := 1
	indexEntry := IndexTableEntry{}
	for {
		eof := indexEntry.ReadEntryFromIndexFile(reader)
		if eof {
			break
		}
		indexEntry.Print(i)
		i++
	}
}
