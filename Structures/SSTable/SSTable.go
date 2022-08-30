package SSTable

import (
	file "Projekat/Handling"
	record "Projekat/Structures"
)

const indexInterval = 10

type SSTable struct {
	DataFilePath    string
	IndexFilePath   string
	SummaryFilePath string
	FilterFilePath  string
}

type OffSets struct {
	dataFileOffSet  uint64
	indexFileOffSet uint64
}

func InitializeOffSets() (offSets *OffSets) {
	return &OffSets{0, 0}
}

func CreateSSTable(dataFilePath, indexFilePath, summaryFilePath, bloomFilterFilePath string, records []record.Record) *SSTable {

	CreateFilterFile(bloomFilterFilePath, records)

	dataFile, indexFile, summaryFile := file.CreateFiles(dataFilePath, indexFilePath, summaryFilePath)
	dataFileWriter, indexFileWriter, summaryFileWriter := file.CreateFileWriters(dataFile, indexFile, summaryFile)
	summaryHeader, summaryEntries := CreateSummaryHeaderAndEntries(records)
	offSets := InitializeOffSets()

	for index, record := range records {
		WriteRecordToDataFile(&record, dataFileWriter)

		indexEntry := CreateIndexTableByRecord(&record, offSets.dataFileOffSet)
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

	file.FlushAndCloseFiles(dataFileWriter, indexFileWriter, summaryFileWriter, dataFile, indexFile, summaryFile)
	return &SSTable{dataFilePath, indexFilePath, summaryFilePath, bloomFilterFilePath}
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
