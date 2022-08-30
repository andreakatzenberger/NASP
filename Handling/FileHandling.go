package Handling

import (
	"bufio"
	"io/ioutil"
	"os"
	"strconv"
)

func CreateFile(filePath string) *os.File {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0777)
	PanicError(err)
	return file
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

func GetIndexSizeFromDirectory() (index uint) {
	files, _ := ioutil.ReadDir("Data/Data")
	return uint(len(files) + 1)
}

func CreateFilePathsByLevel(index uint) (dataFilePath, indexFilePath, summaryFilePath, filterFilePath, tocFilePath string) {
	dataFilePath = "Data/Data/data_" + strconv.Itoa(int(index)) + ".gob"
	indexFilePath = "Data/Index/index_" + strconv.Itoa(int(index)) + ".gob"
	summaryFilePath = "Data/Summary/summary_" + strconv.Itoa(int(index)) + ".gob"
	filterFilePath = "Data/BloomFilter/bloomfilter_" + strconv.Itoa(int(index)) + ".gob"
	tocFilePath = "Data/TOC/toc_" + strconv.Itoa(int(index)) + ".txt"

	return dataFilePath, indexFilePath, summaryFilePath, filterFilePath, tocFilePath
}
