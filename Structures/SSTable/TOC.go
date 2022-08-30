package SSTable

import (
	error "Projekat/Handling"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

func WriteFileNamesToToc(index string) {
	file, err := os.OpenFile("Data/TOC/TOC_"+index+".txt", os.O_RDONLY, 0777)
	error.ReturnError(err)

	defer file.Close()

	_, err = file.WriteString("Data/TOC/bloomfilter_" + index + ".gob\n")
	error.ReturnError(err)

	_, err = file.WriteString("Data/Summary/summary_" + index + ".gob\n")
	error.ReturnError(err)

	_, err = file.WriteString("Data/Index/index_" + index + ".gob\n")
	error.ReturnError(err)

	_, err = file.WriteString("Data/Data/data_" + index + ".gob\n")
	error.ReturnError(err)

	file.Close()
}

func ReadFileNamesFromToc(index string) (filterPath, summaryPath, IndexPath, DataPath string) {
	file, err := ioutil.ReadFile("Data/TOC/TOC_" + index + ".txt")
	if err != nil {
		log.Fatal(err)
	}

	fileNames := string(file)
	fileNamesArray := strings.Split(fileNames, "\n")

	return fileNamesArray[0], fileNamesArray[1], fileNamesArray[2], fileNamesArray[3]
}
