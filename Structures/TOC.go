package Structures

import (
	error "Projekat/Handling"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func (sstable *SSTable) WriteFileNamesToToc() {
	file, err := os.OpenFile(sstable.TocFilePath, os.O_CREATE|os.O_WRONLY, 0777)
	error.ReturnError(err)

	defer file.Close()

	_, err = file.WriteString(sstable.FilterFilePath + "\n")
	error.ReturnError(err)

	_, err = file.WriteString(sstable.SummaryFilePath + "\n")
	error.ReturnError(err)

	_, err = file.WriteString(sstable.IndexFilePath + "\n")
	error.ReturnError(err)

	_, err = file.WriteString(sstable.DataFilePath + "\n")
	error.ReturnError(err)

	file.Close()
}

func ReadFileNamesFromToc(index string) (filterPath, summaryPath, IndexPath, DataPath string) {
	file, err := ioutil.ReadFile("Data/TOC/user-table-data-" + index + "-TOC.txt")
	if err != nil {
		log.Fatal(err)
	}

	fileNames := string(file)
	fileNamesArray := strings.Split(fileNames, "\n")

	return fileNamesArray[0], fileNamesArray[1], fileNamesArray[2], fileNamesArray[3]
}
