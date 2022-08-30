package SSTable

import (
	error "Projekat/Handling"
	record "Projekat/Structures"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type SummaryTableHeader struct {
	MinKeySize  uint64
	MinKey      string
	MaxKeySize  uint64
	MaxKey      string
	EntriesSize uint64
}

type SummaryTableEntry struct {
	KeySize uint64
	Key     string
	Offset  uint64
}

func CreateSummaryHeader(records []record.Record) *SummaryTableHeader {
	summaryHeader := SummaryTableHeader{}
	summaryHeader.MinKey = records[0].Key
	summaryHeader.MinKeySize = uint64(len(summaryHeader.MinKey))
	summaryHeader.MaxKey = records[len(records)-1].Key
	summaryHeader.MaxKeySize = uint64(len(summaryHeader.MaxKey))
	return &summaryHeader
}

func (summaryHeader *SummaryTableHeader) Print() {
	fmt.Println("Min key size:", summaryHeader.MinKeySize)
	fmt.Println("Min key:", summaryHeader.MinKey)
	fmt.Println("Max key size:", summaryHeader.MaxKeySize)
	fmt.Println("Max key:", summaryHeader.MaxKey)
	fmt.Println("Summary entries size:", summaryHeader.EntriesSize)
}

func (summaryEntry *SummaryTableEntry) Print() {
	fmt.Println("Key size:", summaryEntry.KeySize)
	fmt.Println("Key:", summaryEntry.Key)
	fmt.Println("Offset:", summaryEntry.Offset)
}

func (summaryEntry *SummaryTableEntry) GetSize() uint64 {
	return 8 + summaryEntry.KeySize + 8
}

func (summaryHeader *SummaryTableHeader) WriteHeaderToSummaryFile(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryHeader.MinKeySize)
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryHeader.MinKey))
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, summaryHeader.MaxKeySize)
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryHeader.MaxKey))
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, summaryHeader.EntriesSize)
	error.PanicError(err)
}

func (summaryEntry *SummaryTableEntry) WriteEntryToSummaryFile(writer *bufio.Writer) {
	err := binary.Write(writer, binary.LittleEndian, summaryEntry.KeySize)
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, []byte(summaryEntry.Key))
	error.PanicError(err)

	err = binary.Write(writer, binary.LittleEndian, summaryEntry.Offset)
	error.PanicError(err)
}

func (summaryHeader *SummaryTableHeader) ReadHeaderFromSummaryFile(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &summaryHeader.MinKeySize)
	if error.EOFError(err) == true {
		return true
	}

	minKeyByteSlice := make([]byte, summaryHeader.MinKeySize)
	err = binary.Read(reader, binary.LittleEndian, &minKeyByteSlice)
	if error.EOFError(err) == true {
		return true
	}
	summaryHeader.MinKey = string(minKeyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryHeader.MaxKeySize)
	if error.EOFError(err) == true {
		return true
	}

	maxKeyByteSlice := make([]byte, summaryHeader.MaxKeySize)
	err = binary.Read(reader, binary.LittleEndian, &maxKeyByteSlice)
	if error.EOFError(err) == true {
		return true
	}
	summaryHeader.MaxKey = string(maxKeyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryHeader.EntriesSize)
	if error.EOFError(err) == true {
		return true
	}

	return false
}

func (summaryEntry *SummaryTableEntry) ReadEntryFromSummaryFile(reader *bufio.Reader) bool {
	err := binary.Read(reader, binary.LittleEndian, &summaryEntry.KeySize)
	if error.EOFError(err) == true {
		return true
	}

	keyByteSlice := make([]byte, summaryEntry.KeySize)
	err = binary.Read(reader, binary.LittleEndian, &keyByteSlice)
	if error.EOFError(err) == true {
		return true
	}
	summaryEntry.Key = string(keyByteSlice)

	err = binary.Read(reader, binary.LittleEndian, &summaryEntry.Offset)
	if error.EOFError(err) == true {
		return true
	}

	return false
}

func CreateSummaryHeaderAndEntries(records []record.Record) (summaryHeader *SummaryTableHeader, summaryEntries []SummaryTableEntry) {
	summaryHeader = CreateSummaryHeader(records)
	summaryEntries = make([]SummaryTableEntry, 0)
	return summaryHeader, summaryEntries
}

func getOffsetInIndexTableForKey(key string, filePath string) (uint64, bool) {
	file, err := os.Open(filePath)
	error.PanicError(err)

	defer file.Close()
	reader := bufio.NewReader(file)

	summaryHeader := SummaryTableHeader{}
	eof := summaryHeader.ReadHeaderFromSummaryFile(reader)
	if eof {
		return 0, false
	}

	if summaryHeader.MinKey > key {
		return 0, false
	}

	if summaryHeader.MaxKey < key {
		return 0, false
	}

	buf := make([]byte, summaryHeader.EntriesSize)
	_, err = io.ReadFull(reader, buf)
	error.PanicError(err)

	reader = bufio.NewReader(bytes.NewBuffer(buf))
	prevSummaryEntry := SummaryTableEntry{}
	nextSummaryEntry := SummaryTableEntry{}

	for {
		prevSummaryEntry = nextSummaryEntry
		eof = nextSummaryEntry.ReadEntryFromSummaryFile(reader)
		if eof {
			return prevSummaryEntry.Offset, true
		}

		if prevSummaryEntry.Key <= key && key < nextSummaryEntry.Key {
			break
		}
	}

	return prevSummaryEntry.Offset, true
}

func PrintSummaryFile(filePath string) {
	file, err := os.Open(filePath)
	error.PanicError(err)

	defer file.Close()
	reader := bufio.NewReader(file)

	summaryHeader := SummaryTableHeader{}
	eof := summaryHeader.ReadHeaderFromSummaryFile(reader)
	if eof {
		return
	}

	summaryHeader.Print()

	fmt.Println()
	i := 1
	summaryEntry := SummaryTableEntry{}
	for {
		eof := summaryEntry.ReadEntryFromSummaryFile(reader)
		if eof {
			return
		}

		fmt.Println("Entry", i)
		summaryEntry.Print()
		fmt.Println()
		i++
	}
}
