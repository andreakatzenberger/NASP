package main

import (
	"Projekat/Handling"
	record "Projekat/Structures"
	"Projekat/Structures/SSTable"
	"fmt"
	"strconv"
	"time"
)

func main() {
	fmt.Println()
	records := []record.Record{}

	start := time.Now()

	for i := 0; i < 100; i++ {
		bytes := []byte{1, 2}
		record := record.CreateRecord(strconv.Itoa(i), bytes, 0)
		records = append(records, *record)
	}

	dataFilePath := "Data/Data/Data.gob"
	indexFilePath := "Data/Index/index.gob"
	summaryFilePath := "Data/Summary/summary.gob"
	filterFilePath := "Data/Filter/filter.gob"

	sstable := SSTable.SSTable{}
	sstable.DataFilePath = "Data/Data/Data.gob"
	sstable.IndexFilePath = "Data/Index/index.gob"
	sstable.SummaryFilePath = "Data/Summary/summary.gob"
	sstable.FilterFilePath = "Data/Filter/filter.gob"

	//SSTableProba.TestBloomFilter()
	SSTable.CreateSSTable(dataFilePath, indexFilePath, summaryFilePath, filterFilePath, records)
	_, found := sstable.GetRecordInSStableForKey("1")
	fmt.Println(found)
	end := time.Since(start)
	fmt.Println(end)
	SSTable.WriteFileNamesToToc("1")
	_, _, _, a := SSTable.ReadFileNamesFromToc("1")
	fmt.Println(a)
	fmt.Println(Handling.LevelSizeInDirectory())
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
