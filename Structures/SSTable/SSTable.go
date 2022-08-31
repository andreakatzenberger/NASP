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
}

type OffSets struct {
	dataFileOffSet  uint64
	indexFileOffSet uint64
}

func InitializeOffSets() (offSets *OffSets) {
	return &OffSets{0, 0}
}

func CreateSSTable(index uint) *SSTable {

	sstable := SSTable{}
	sstable.DataFilePath, sstable.IndexFilePath, sstable.SummaryFilePath,
		sstable.FilterFilePath, sstable.TocFilePath = file.CreateFilePathsByIndex(index)

	return &sstable
}
func (sstable *SSTable) WriteRecordsToSSTable(records []record.Record) {

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

func GetSSTableIndexes() (indexs []string) {
	dataPaths, _ := ioutil.ReadDir("Data/Data")
	indexes := make([]string, 0)
	for _, path := range dataPaths {
		splitByLine := strings.Split(path.Name(), "_")
		splitByPoint := strings.Split(splitByLine[1], ".")
		index := splitByPoint[0]
		indexes = append(indexes, index)
	}
	return indexes
}
