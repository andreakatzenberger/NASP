package app

import (
	"memtable"
)

type DefaulConfig struct {
	//dodati za wal
	
	//za skiplistu
	MaxHeight int

	//za memtable
	Threshold float32
	MaxSize   int
	
	//dodati za sstable
}

//ucitava defaultne konfiguracije
func LoadDefaultConfig() *DefaulConfig {
	return &DefaulConfig{
		MaxHeight: 5,
		Threshold: 80,
		MaxSize:   10,
	}
}

type App struct {
	//wal
	memtable memtable.Memtable
	//sstable
}

func CreateApp() *App {
	config := LoadDefaultConfig()
	return &App{
		//wal
		memtable: *memtable.CreateMemtable(config.MaxHeight, config.Threshold, config.MaxSize),
		//sstable
	}
}

func (app *App) Put(key string, value []byte) bool {
	app.memtable.Add(key, value)
	return true
}

func (app *App) Get(key string) []byte {
	if app.memtable.Find(key) == nil {
		return nil
	} else {
		return app.memtable.Find(key)
	}
}

func (app *App) Delete(key string) bool {
	app.memtable.Delete(key)
	return true
}
