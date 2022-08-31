package SSTable

import (
	file "Projekat/Handling"
	record "Projekat/Structures"
	"io/ioutil"
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

func (sstable *SSTable) WriteRecordsToSSTable(records []record.Record) bool {

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

func CreateFilterFile(bloomFilterFilePath string, records []record.Record) {

	filter := CreateBloomFilter(uint(len(records)), 0.05)
	filter.WriteRecordsToBloomFilter(&records)
	WriteBloomFilter(bloomFilterFilePath, filter)
}

func (sstable *SSTable) GetRecordInSStableForKey(key string) (*record.Record, bool) {

	found := CheckKeyInFilterFile(key, sstable.FilterFilePath)
	if !found {
		return &record.Record{}, false
	}
	offsetIndexTable, found := getOffsetInIndexTableForKey(key, sstable.SummaryFilePath)
	if !found {
		return &record.Record{}, false
	}
	offsetDataTable, found := getOffsetInDataTableForKey(key, sstable.IndexFilePath, offsetIndexTable, indexInterval)
	if !found {
		return &record.Record{}, false
	}
	foundRecord, found := GetRecordInDataTableForOffset(sstable.DataFilePath, offsetDataTable)
	if !found {
		return &record.Record{}, false
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
