package Structures

type DefaultConfig struct {
	//dodati za wal

	//za skiplistu
	MaxHeight int

	//za memtable
	Threshold float32
	MaxSize   int
}

//ucitava defaultne konfiguracije
func LoadDefaultConfig() *DefaultConfig {
	return &DefaultConfig{
		MaxHeight: 5,
		Threshold: 80,
		MaxSize:   5,
	}
}

type App struct {
	//wal
	memtable Memtable
}

//kreira sistem
func CreateApp() *App {
	config := LoadDefaultConfig()
	return &App{
		//wal
		memtable: *CreateMemtable(config.MaxHeight, config.Threshold, config.MaxSize),
		//sstable
	}
}

//ubacuje element
//prima kljuc tipa string i vrednost tipa bit array, a vraca bool
func (app *App) Put(key string, value []byte) bool {
	app.memtable.Add(key, value)
	return true
}

//trazi element
//prima kljuc tipa string, a vraca vrednost tipa bit array
func (app *App) Get(key string) []byte {
	if app.memtable.Find(key) == nil {
		return GetFromSSTable(key)
		//return nil
	} else {
		return app.memtable.Find(key)
	}
}

//brise element
//prima kljuc tipa string, vraca bool
func (app *App) Delete(key string) bool {
	app.memtable.Delete(key)
	return true
}
