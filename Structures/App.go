package Structures

type DefaultConfig struct {
	//za wal
	SegmentSize int64
	filesSlice  []string

	//za skiplistu
	MaxHeight int

	//za memtable
	Threshold float32
	MaxSize   int
}

// ucitava defaultne konfiguracije
func LoadDefaultConfig() *DefaultConfig {
	return &DefaultConfig{
		SegmentSize: 3,
		filesSlice:  nil,
		MaxHeight:   5,
		Threshold:   80,
		MaxSize:     6,
	}
}

type App struct {
	wal      WAL
	memtable Memtable
}

// kreira sistem
func CreateApp() *App {
	config := LoadDefaultConfig()
	return &App{
		wal:      *Innit(config.SegmentSize, config.filesSlice),
		memtable: *CreateMemtable(config.MaxHeight, config.Threshold, config.MaxSize),
	}
}

// restauriranje podataka u memtable
func (app *App) RestoreFromWAL() {
	data := app.wal.Read()
	for key, value := range data {
		app.memtable.Add(key, value)
	}
}

// ubacuje element
// prima kljuc tipa string i vrednost tipa bit array, a vraca bool
func (app *App) Put(key string, value []byte) bool {
	insert := app.wal.Insert(key, value, "i")
	if !insert {
		return false
	}
	filled := app.memtable.Add(key, value)
	if filled {
		app.wal.Delete()
	}
	return true
}

// trazi element
// prima kljuc tipa string, a vraca vrednost tipa bit array
func (app *App) Get(key string) []byte {
	if app.memtable.Find(key) == nil {
		return GetFromSSTable(key)
	} else {
		return app.memtable.Find(key)
	}
}

// brise element
// prima kljuc tipa string, vraca bool
func (app *App) Delete(key string) bool {
	insert := app.wal.Insert(key, make([]byte, 0), "d")
	if !insert {
		return false
	}
	return app.memtable.Delete(key)
}
