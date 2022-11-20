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
	// 全局的wait
	var wg1 sync.WaitGroup
	// 读取数据的wait, 用于在文件读取完后关闭channel
	var wg2 sync.WaitGroup
	wg1.Add(1)
	dataCh := make(chan []byte, chanSize)
	resultCh := make(chan *Result, chanSize)
	rw := NewSortedFileWriter(resultCh, output, chanSize)
	rw.Run(wg1.Done)
	dc := NewDataClassfier(dataCh, resultCh, "张三", runtime.GOMAXPROCS(0)*2)
	dc.Run()
	// 输入目录的一些检查
	inputInfo, err := os.Stat(input)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("input dir [%s] not exist", input)
		}
	}
	if !inputInfo.IsDir() {
		return fmt.Errorf("input path [%s] is not dir", input)
	}
	// 如果输出路径不存在则自动创建
	err = os.MkdirAll(output, 0644)
	if err != nil {
		return err
	}
	// 遍历输入文件夹, 这里只遍历了一层, 如果存在多层目录可改成递归形式
	err = filepath.WalkDir(input, func(path string, d fs.DirEntry, err error) error {
		// 跳过文件夹
		if d.IsDir() {
			return nil
		}
		// 只读取符合条件拓展名,这里是.txt
		if strings.HasSuffix(path, suffix) {
			wg2.Add(1)
			// 并发读取文件
			go LineReader(path, dataCh, wg2.Done)
		}
		return nil
	})
	// 等待数据读取完成
	wg2.Wait()
	// 关闭原始数据chan
	close(dataCh)
	// 等待所有任务完成
	wg1.Wait()
	return err
}

// LineReader 按行读取文件并发送到chan中
func LineReader(path string, ch chan []byte, wgDone func()) {
	defer wgDone()
	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		// todo:
		ch <- []byte(scan.Text())
	}
}

type Result struct {
	ts   int64
	key  string
	data []byte
}

type Results []*Result

func (r Results) Len() int {
	return len(r)
}
func (r Results) Less(i, j int) bool {
	return r[i].ts < r[j].ts
}
func (r Results) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}
