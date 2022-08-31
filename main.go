package main

import (
	"Projekat/Structures"
	"fmt"
)

//func CreateRecords(first, last int) *[]record.Record {
//	records := []record.Record{}
//
//	for i := first; i < last; i++ {
//		bytes := []byte{1, 2}
//		record := record.CreateRecord(strconv.Itoa(i), bytes, 0)
//		records = append(records, *record)
//	}
//	return &records
//}

func main() {

	//Zapisi u fajlu:
	//Index number 1 - 0:99
	//Index number 2 - 100:199
	//Index number 3 - 500:600
	//Index number 4 - 0:99

	// Kreiraj zapise
	//records := CreateRecords(0, 100)

	// Upisi zapise u SSTable
	//PutToSSTable(*records)

	// Dobavi zapis preko kljuca iz SSTable-a
	//GetFromSSTable("50")

	// Obavi SSTable preko njegovog indexa
	//DeleteSSTable("1")

	/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	app := Structures.CreateApp()
	fmt.Println("KORISNICKI MENI")
	for {
		fmt.Println("1) PUT\n" +
			"2) GET\n" +
			"3) DELETE\n" +
			"4) Exit")
		var option string
		fmt.Println("Izaberite odgovarajucu opciju: ")
		fmt.Scanln(&option)
		if option == "1" {
			var key string
			var value string
			fmt.Println("Unesite kljuc: ")
			fmt.Scanln(&key)
			fmt.Println("Unesite vrednost: ")
			fmt.Scanln(&value)
			success := app.Put(key, []byte(value))
			if success == true {
				fmt.Println("Element je uspesno unet.")
			} else {
				fmt.Println("Element nije uspesno unet.")
			}
		} else if option == "2" {
			var key string
			fmt.Println("Unesite kljuc: ")
			fmt.Scanln(&key)
			value := app.Get(key)
			if value == nil {
				fmt.Println("Element sa unetim kljucem ne postoji.")
			} else {
				fmt.Println("Vrednost elementa sa unetim kljucem je: ", value)
			}
		} else if option == "3" {
			var key string
			fmt.Println("Unesite kljuc: ")
			fmt.Scanln(&key)
			success := app.Delete(key)
			if success == true {
				fmt.Println("Element je uspesno obrisan.")
			} else {
				fmt.Println("Element nije uspesno obrisan.")
			}
		} else if option == "4" {
			break
		} else {
			fmt.Println("Uneli ste nepostojecu opciju! Pokusajte ponovo!")
		}
	}

	//sl := record.CreateSkipList(10)
	//sl.Add("1", []byte("1"))
	//sl.Add("2", []byte("2"))
	//sl.Add("3", []byte("3"))
	//sl.Add("4", []byte("4"))
	//sl.Add("5", []byte("5"))
	//sl.Add("6", []byte("6"))
	//sl.Add("7", []byte("7"))
	//sl.Add("8", []byte("8"))
	//sl.Print()
	//
	//allRecords := sl.SLNodeToRecord()
	//for i := 0; i < len(allRecords); i++ {
	//	allRecords[i].Print()
	//}

}
