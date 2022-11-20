package handler

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// FileWriter 这里只做流式并发写入，未包含排序
type FileWriter struct {
	chanSize int
	mc       map[string]chan []byte
	resultCh chan *Result
	base     string
}

func NewFileWriter(ch chan *Result, base string, chanSize int) *FileWriter {
	return &FileWriter{
		chanSize: chanSize,
		mc:       make(map[string]chan []byte),
		resultCh: ch,
		base:     base,
	}
}

func (w *FileWriter) Run(wgDone func()) {
	go func() {
		defer wgDone()
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

func (w *FileWriter) writer(dataChan chan []byte, path string) {
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

// SortedFileWriter 这里会先收集数据，然后单个文件数据中做排序，内存占用会较多
type SortedFileWriter struct {
	resultCh chan *Result
	data     map[string]Results
	chanSize int
	base     string
}

func NewSortedFileWriter(ch chan *Result, base string, chanSize int) *SortedFileWriter {
	return &SortedFileWriter{
		resultCh: ch,
		data:     make(map[string]Results),
		chanSize: chanSize,
		base:     base,
	}
}
func (w *SortedFileWriter) Run(wgDone func()) {
	go func() {
		defer wgDone()
		// 此处只是单纯的将chan中的数据按key分组丢到map中
		for r := range w.resultCh {
			c, ok := w.data[r.key]
			if !ok {
				c = Results{}
				w.data[r.key] = c
			}
			w.data[r.key] = append(c, r)
		}
		var writerWg sync.WaitGroup
		for k, v := range w.data {
			writerWg.Add(1)
			t0 := time.Now()
			// 排序
			sort.Sort(v)
			fmt.Printf("sort [%s] cost %s\n", k, time.Since(t0))
			// 并发的将排序后的数据写道文件中
			go w.writer(v, filepath.Join(w.base, k), writerWg.Done)
		}
		writerWg.Wait()

	}()
}

func (w *SortedFileWriter) writer(data Results, path string, wgDone func()) {
	defer wgDone()
	f, err := os.Create(path)
	if err != nil {
		// todo: 错误处理
		panic(err)
	}
	defer f.Close()
	for _, v := range data {
		b := append(v.data, '\n')
		_, err = f.Write(b)
		if err != nil {
			panic(err)
		}
	}
}
