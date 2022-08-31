package main

import (
	record "Projekat/Structures"
	"Projekat/Structures/SSTable"
	"fmt"
	"strconv"
)

func main() {
	//fmt.Println()
	//records := []record.Record{}
	//
	//for i := 100; i < 200; i++ {
	//	bytes := []byte{1, 2}
	//	record := record.CreateRecord(strconv.Itoa(i), bytes, 0)
	//	records = append(records, *record)
	//}
	//
	//index := file.GetIndexSizeFromDirectory()
	//sstable := SSTable.CreateSSTable(index)
	//sstable.WriteRecordsToSSTable(records)

	indexes := SSTable.GetSSTableIndexes()
	record1 := record.Record{}

	for _, index := range indexes {
		indexUint, _ := strconv.ParseUint(index, 10, 64)
		sstable := SSTable.CreateSSTable(uint(indexUint))
		record2, found := sstable.GetRecordInSStableForKey("700")

		if found == true {
			if record2.Timestamp > record1.Timestamp {
				record1 = *record2
			}
		}
	}
	if record1.Tombstone != 1 {
		fmt.Println("Found record", record1.Key, "with", record1.Value, "value.", record1.Timestamp)
	}

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
