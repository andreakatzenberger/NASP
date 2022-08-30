package Handling

import "io"

func ReturnError(err error) {
	if err != nil {
		return
	}
}

func PanicError(err error) {
	if err != nil {
		panic(err)
	}
}

func EOFError(err error) bool {
	if err != nil {
		if err == io.EOF {
			return true
		}
		panic(err)
	}
	return false
}
