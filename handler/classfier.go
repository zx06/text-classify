package handler

import (
	"encoding/json"
	"sync"
	"time"
)

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
							ts:   data.Timestamp,
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
