package main

import (
	file "Projekat/Handling"
	record "Projekat/Structures"
	"Projekat/Structures/SSTable"
	"fmt"
	"strconv"
)

func increaseByOne(index string) string {
	indexInt, _ := strconv.Atoi(index)
	newIndex := indexInt + 1
	indexString := strconv.Itoa(newIndex)
	return indexString
}

func CreateRecords(first, last int) *[]record.Record {
	records := []record.Record{}

	for i := first; i < last; i++ {
		bytes := []byte{1, 2}
		record := record.CreateRecord(strconv.Itoa(i), bytes, 0)
		records = append(records, *record)
	}
	return &records
}

func PutToSSTable(records []record.Record) {
	index := file.GetLastIndexFromDirectory()
	index = increaseByOne(index)

	sstable := SSTable.SSTableConstructor(index)
	success := sstable.WriteRecordsToSSTable(records)
	if success == true {
		fmt.Println("Records are successfully writen to SSTable")
	} else {
		fmt.Println("Records are unsuccessfully writen to SSTable")
	}
}

func GetFromSSTable(key string) {
	indexes := SSTable.GetAllSSTableIndexes()
	record := record.Record{}
	exist := false

	for _, index := range indexes {
		sstable := SSTable.SSTableConstructor(index)
		newRecord, found := sstable.GetRecordInSStableForKey(key)

		if found == true {
			if newRecord.Timestamp > record.Timestamp {
				record = *newRecord
				exist = true
			}
		}
	}
	if exist == true {
		fmt.Print("Record key ", record.Key, " found, ")
		if record.Tombstone != 1 {
			fmt.Println("with", record.Value, " value.")
		} else {
			fmt.Println("but it is deleted.")
		}
	} else {
		fmt.Println("Record is not found")
	}
}

func DeleteSSTable(index string) {

	sstable := SSTable.SSTableConstructor(index)
	exist := sstable.CheckIfSSTableExist()
	if exist == true {
		sstable.DeleteSSTableFiles()
	}
}

func main() {

	//Zapisi u fajlu:
	//Index number 1 - 0:99
	//Index number 2 - 100:199
	//Index number 3 - 500:600
	//Index number 4 - 0:99

	// Kreiraj zapise
	records := CreateRecords(0, 100)

	// Upisi zapise u SSTable
	PutToSSTable(*records)

	// Dobavi zapis preko kljuca iz SSTable-a
	GetFromSSTable("50")

	// Obavi SSTable preko njegovog indexa
	DeleteSSTable("1")

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//app := Structures.CreateApp()
	//fmt.Println("KORISNICKI MENI")
	//for {
	//	fmt.Println("1) PUT\n" +
	//		"2) GET\n" +
	//		"3) DELETE\n" +
	//		"4) Exit")
	//	var option string
	//	fmt.Println("Izaberite odgovarajucu opciju: ")
	//	fmt.Scanln(&option)
	//	if option == "1" {
	//		var key string
	//		var value string
	//		fmt.Println("Unesite kljuc: ")
	//		fmt.Scanln(&key)
	//		fmt.Println("Unesite vrednost: ")
	//		fmt.Scanln(&value)
	//		success := app.Put(key, []byte(value))
	//		if success == true {
	//			fmt.Println("Element je uspesno unet.")
	//		} else {
	//			fmt.Println("Element nije uspesno unet.")
	//		}
	//	} else if option == "2" {
	//		var key string
	//		fmt.Println("Unesite kljuc: ")
	//		fmt.Scanln(&key)
	//		value := app.Get(key)
	//		if value == nil {
	//			fmt.Println("Element sa unetim kljucem ne postoji.")
	//		} else {
	//			fmt.Println("Vrednost elementa sa unetim kljucem je: ", value)
	//		}
	//	} else if option == "3" {
	//		var key string
	//		fmt.Println("Unesite kljuc: ")
	//		fmt.Scanln(&key)
	//		success := app.Delete(key)
	//		if success == true {
	//			fmt.Println("Element je uspesno obrisan.")
	//		} else {
	//			fmt.Println("Element nije uspesno obrisan.")
	//		}
	//	} else if option == "4" {
	//		break
	//	} else {
	//		fmt.Println("Uneli ste nepostojecu opciju! Pokusajte ponovo!")
	//	}
	//}
}
