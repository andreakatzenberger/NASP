package Structures

import (
	file "Projekat/Handling"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
)

const indexInterval = 10

type SSTable struct {
	DataFilePath    string
	IndexFilePath   string
	SummaryFilePath string
	FilterFilePath  string
	TocFilePath     string
	IndexNumber     string
}

type OffSets struct {
	dataFileOffSet  uint64
	indexFileOffSet uint64
}

func InitializeOffSets() (offSets *OffSets) {
	return &OffSets{0, 0}
}

func SSTableConstructor(index string) *SSTable {
	sstable := SSTable{}
	sstable.DataFilePath, sstable.IndexFilePath, sstable.SummaryFilePath,
		sstable.FilterFilePath, sstable.TocFilePath = file.CreateFilePathsByIndex(index)
	sstable.IndexNumber = index

	return &sstable
}

func (sstable *SSTable) CheckIfSSTableExist() bool {
	indexes := GetAllSSTableIndexes()
	for _, index := range indexes {
		if sstable.IndexNumber == index {
			return true
		}
	}
	return false
}

func (sstable *SSTable) WriteRecordsToSSTable(records []Record) bool {

	CreateFilterFile(sstable.FilterFilePath, records)

	dataFile, indexFile, summaryFile := file.CreateFiles(sstable.DataFilePath, sstable.IndexFilePath, sstable.SummaryFilePath)
	dataFileWriter, indexFileWriter, summaryFileWriter := file.CreateFileWriters(dataFile, indexFile, summaryFile)
	summaryHeader, summaryEntries := CreateSummaryHeaderAndEntries(records)
	offSets := InitializeOffSets()

	for index, record := range records {
		WriteRecordToDataFile(&record, dataFileWriter)

		indexEntry := CreateIndexEntry(&record, offSets.dataFileOffSet)
		indexEntry.WriteEntryToIndexFile(indexFileWriter)
		offSets.dataFileOffSet += record.GetSize()

		if (index == len(records)-1) || (index%indexInterval == 0) {
			summaryEntry := SummaryTableEntry{KeySize: indexEntry.KeySize, Key: indexEntry.Key, Offset: offSets.indexFileOffSet}
			summaryEntries = append(summaryEntries, summaryEntry)
			summaryHeader.EntriesSize += summaryEntry.GetSize()
		}
		offSets.indexFileOffSet += indexEntry.GetSize()
	}

	summaryHeader.WriteHeaderToSummaryFile(summaryFileWriter)
	for _, summaryEntry := range summaryEntries {
		summaryEntry.WriteEntryToSummaryFile(summaryFileWriter)
	}

	sstable.WriteFileNamesToToc()

	file.FlushAndCloseFiles(dataFileWriter, indexFileWriter, summaryFileWriter, dataFile, indexFile, summaryFile)
	return true
}

func CreateFilterFile(bloomFilterFilePath string, records []Record) {

	filter := CreateBloomFilter(uint(len(records)), 0.05)
	filter.WriteRecordsToBloomFilter(&records)
	WriteBloomFilter(bloomFilterFilePath, filter)
}

func (sstable *SSTable) GetRecordInSStableForKey(key string) (*Record, bool) {

	found := CheckKeyInFilterFile(key, sstable.FilterFilePath)
	if !found {
		return &Record{}, false
	}
	offsetIndexTable, found := getOffsetInIndexTableForKey(key, sstable.SummaryFilePath)
	if !found {
		return &Record{}, false
	}
	offsetDataTable, found := getOffsetInDataTableForKey(key, sstable.IndexFilePath, offsetIndexTable, indexInterval)
	if !found {
		return &Record{}, false
	}
	foundRecord, found := GetRecordInDataTableForOffset(sstable.DataFilePath, offsetDataTable)
	if !found {
		return &Record{}, false
	}
	return foundRecord, true
}

func GetAllSSTableIndexes() (indexs []string) {
	dataPaths, _ := ioutil.ReadDir("Data/Data")
	indexes := make([]string, 0)
	for _, path := range dataPaths {
		splitByLine := strings.Split(path.Name(), "-")
		index := splitByLine[3]
		indexes = append(indexes, index)
	}
	return indexes
}

func (sstable *SSTable) DeleteSSTableFiles() {
	file.DeleteFile(sstable.DataFilePath)
	file.DeleteFile(sstable.TocFilePath)
	file.DeleteFile(sstable.FilterFilePath)
	file.DeleteFile(sstable.SummaryFilePath)
	file.DeleteFile(sstable.IndexFilePath)
}

func increaseByOne(index string) string {
	indexInt, _ := strconv.Atoi(index)
	newIndex := indexInt + 1
	indexString := strconv.Itoa(newIndex)
	return indexString
}

func PutToSSTable(records []Record) {
	index := file.GetLastIndexFromDirectory()
	index = increaseByOne(index)

	sstable := SSTableConstructor(index)
	success := sstable.WriteRecordsToSSTable(records)
	if success == true {
		fmt.Println("Uspesno napravljen SSTable!")
	} else {
		fmt.Println("SSTable nije uspeno napravljen.")
	}
}

func GetFromSSTable(key string) []byte {
	indexes := GetAllSSTableIndexes()
	record := Record{}
	exist := false

	for _, index := range indexes {
		sstable := SSTableConstructor(index)
		newRecord, found := sstable.GetRecordInSStableForKey(key)

		if found == true {
			if newRecord.Timestamp > record.Timestamp {
				record = *newRecord
				exist = true
			}
		}
	}
	if exist == true {
		if record.Tombstone != 1 {
			return record.Value
		} else {
			return nil
		}
	} else {
		return nil
	}
}

func DeleteSSTable(index string) {

	sstable := SSTableConstructor(index)
	exist := sstable.CheckIfSSTableExist()
	if exist == true {
		sstable.DeleteSSTableFiles()
	}
}
