package handler

import (
	"os"
	"path/filepath"
	"sync"
)

type ResultWriter struct {
	chanSize int
	mc       map[string]chan []byte
	resultCh chan *Result
	base     string
}

func NewResultWriter(ch chan *Result, base string, chanSize int) *ResultWriter {
	return &ResultWriter{
		chanSize: chanSize,
		mc:       make(map[string]chan []byte),
		resultCh: ch,
		base:     base,
	}
}

func (w *ResultWriter) Run(wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		for r := range w.resultCh {
			c, ok := w.mc[r.key]
			// key不存在时新建chan并启动新协程处理写入
			if !ok {
				c = make(chan []byte, w.chanSize)
				w.mc[r.key] = c
				path := filepath.Join(w.base, r.key)
				// 创建写协程
				go w.writer(c, path)
			}
			// 写入数据到对应chan
			c <- r.data
		}
		// 结果chan处理完后关闭所有写入子chan
		for _, v := range w.mc {
			close(v)
		}
	}()
}

func (w *ResultWriter) writer(dataChan chan []byte, path string) {
	f, err := os.Create(path)
	if err != nil {
		// todo: 错误处理
		panic(err)
	}
	defer f.Close()
	for b := range dataChan {
		b = append(b, '\n')
		_, err = f.Write(b)
		if err != nil {
			panic(err)
		}
	}
}
