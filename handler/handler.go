package handler

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

func Handler(input, output, suffix string, chanSize int) error {
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	wg1.Add(1)
	lineCh := make(chan []byte, chanSize)
	resultCh := make(chan *Result, chanSize)
	rw := NewResultWriter(resultCh, output, chanSize)
	rw.Run(wg1.Done)
	dc := NewDataClassfier(lineCh, resultCh, "张三", runtime.GOMAXPROCS(0)*2)
	dc.Run()
	_, err := os.Stat(input)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("input dir [%s] not exist", input)
		}
	}
	err = filepath.WalkDir(input, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, suffix) {
			wg2.Add(1)
			go LineReader(path, lineCh, wg2.Done)
		}
		return nil
	})
	wg2.Wait()
	close(lineCh)
	wg1.Wait()
	return err
}

func LineReader(path string, ch chan []byte, wgDone func()) {
	defer wgDone()
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		ch <- []byte(scan.Text())
	}
}

type Result struct {
	key  string
	data []byte
}
