package SSTable

import (
	error "Projekat/Handling"
	record "Projekat/Structures"
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

//Serijalizacija zapisa i upisati u fajl
func WriteRecordToDataFile(record *record.Record, writer *bufio.Writer) {
	recordByteSlice := record.EncodeRecord()

	err := binary.Write(writer, binary.LittleEndian, recordByteSlice)
	error.ReturnError(err)
}

//Serijalizacija zapisa iz fajla
func ReadRecordFromDataFile(record *record.Record, reader *bufio.Reader) bool {
	eof := record.DecodeRecord(reader)
	return eof
}

//GetRecordInDataTableForOffset
func GetRecordInDataTableForOffset(filePath string, offset uint64) (*record.Record, bool) {
	file, err := os.Open(filePath)
	error.PanicError(err)

	defer file.Close()
	reader := bufio.NewReader(file)

	file.Seek(int64(offset), 0)

	foundRecord := record.Record{}
	eof := ReadRecordFromDataFile(&foundRecord, reader)
	if eof {
		return &record.Record{}, false
	}

	return &foundRecord, true
}

//Ispiši zapise iz data fajla
func PrintDataFile(dataFilePath string) {
	file, err := os.Open(dataFilePath)
	error.PanicError(err)
	defer file.Close()
	reader := bufio.NewReader(file)

	i := 1
	recordToPrint := record.Record{}
	for {
		eof := recordToPrint.DecodeRecord(reader)
		if eof {
			break
		}

		fmt.Println("Record", i)
		fmt.Println("-----------------------")
		recordToPrint.Print()
		fmt.Println()
		i++
	}
}
