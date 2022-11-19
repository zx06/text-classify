package handler

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func Handler(input, output, suffix string, chanSize int) error {
	var wg sync.WaitGroup
	var wg2 sync.WaitGroup
	wg.Add(1)
	lineCh := make(chan []byte, chanSize)
	resultCh := make(chan *Result, chanSize)
	rw := NewResultWriter(resultCh, output, chanSize)
	rw.Run(&wg)
	dc := NewDataClassfier(lineCh, resultCh, "张三", 8)
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
			go LineReader(path, lineCh,&wg2)
		}
		return nil
	})
	wg2.Wait()
	close(lineCh)
	wg.Wait()
	return err
}

func LineReader(path string, ch chan []byte,wg *sync.WaitGroup) error {
	defer wg.Done()
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		ch <- []byte(scan.Text())
	}
	f.Close()
	return nil
}



type Result struct {
	key  string
	data []byte
}



type DataClassfier struct {
	name        string
	dataCh      chan []byte
	resultCh    chan *Result
	concurrency int
}

func NewDataClassfier(dataCh chan []byte, resultCh chan *Result, name string, concurrency int) *DataClassfier {
	return &DataClassfier{
		name:        name,
		dataCh:      dataCh,
		resultCh:    resultCh,
		concurrency: concurrency,
	}
}

type DataType struct {
	Name      string
	Timestamp int64
}

func (c *DataClassfier) Run() {
	go func() {
		var wg sync.WaitGroup
		wg.Add(c.concurrency)
		for i := 0; i < c.concurrency; i++ {
			go func() {
				defer wg.Done()
				for line := range c.dataCh {
					var data DataType
					err := json.Unmarshal(line, &data)
					if err != nil {
						// todo: 需要处理
						continue
					}
					if data.Name == c.name {
						// todo: 这里会按本地时区解析，如需指定市区需调整时区
						key := time.UnixMilli(data.Timestamp).Format("20060102")
						c.resultCh <- &Result{
							key:  key,
							data: line,
						}
					}
				}
			}()
		}
		wg.Wait()
		close(c.resultCh)
	}()
}
