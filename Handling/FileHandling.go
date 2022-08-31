package Handling

import (
	"bufio"
	"io/ioutil"
	"os"
	"strings"
)

func CreateFile(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0777)
	PanicError(err)
	return file
}

func OpenFile(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0777)
	PanicError(err)
	return file
}

func DeleteFile(filePath string) {
	err := os.Remove(filePath)
	PanicError(err)
}

func CreateFiles(dataFilePath, indexFilePath, summaryFilePath string) (dataFile, indexFile, summaryFile *os.File) {
	dataFile = CreateFile(dataFilePath)
	indexFile = CreateFile(indexFilePath)
	summaryFile = CreateFile(summaryFilePath)

	return dataFile, indexFile, summaryFile
}

func CreateFileWriters(dataFile, indexFile, summaryFile *os.File) (dataFileWriter, indexFileWriter, summaryFileWriter *bufio.Writer) {
	dataFileWriter = bufio.NewWriter(dataFile)
	indexFileWriter = bufio.NewWriter(indexFile)
	summaryFileWriter = bufio.NewWriter(summaryFile)

	return dataFileWriter, indexFileWriter, summaryFileWriter
}

func FlushAndCloseFiles(dataFW, indexFW, summaryFW *bufio.Writer, dataF, indexF, summaryF *os.File) {
	dataFW.Flush()
	indexFW.Flush()
	summaryFW.Flush()
	dataF.Close()
	indexF.Close()
	summaryF.Close()
}

func GetLastIndexFromDirectory() string {
	directory, _ := ioutil.ReadDir("Data/Data")
	directorySize := len(directory)
	if directorySize > 0 {
		filePath := directory[directorySize-1]
		splitByLine := strings.Split(filePath.Name(), "-")
		index := splitByLine[3]
		return index
	}
	return "0"
}

func CreateFilePathsByIndex(index string) (dataFilePath, indexFilePath, summaryFilePath, filterFilePath, tocFilePath string) {
	dataFilePath = "Data/Data/user-table-data-" + index + "-Data.gob"
	indexFilePath = "Data/Index/user-table-data-" + index + "-Index.gob"
	summaryFilePath = "Data/Summary/user-table-data-" + index + "-Sumarry.gob"
	filterFilePath = "Data/BloomFilter/user-table-data-" + index + "-BloomFilter.gob"
	tocFilePath = "Data/TOC/user-table-data-" + index + "-TOC.txt"

	return dataFilePath, indexFilePath, summaryFilePath, filterFilePath, tocFilePath
}
