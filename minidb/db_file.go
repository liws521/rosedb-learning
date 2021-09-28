package minidb

import (
	"fmt"
	"os"
)

const FileName = "minidb.data"
const MergeFileName = "minidb.data.merge"

// DBFile 数据文件定义
type DBFile struct {
	File *os.File
	Offset int64
}


func newInternal(fileName string) (*DBFile, error) {
	// os.O_CREATE 如果文件不存在将创建一个新文件
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0644)
	// err返回给上层处理
	if err != nil {
		return nil, err
	}
	// os.Stat返回一个FileInfo接口类型的变量
	// 能调用Name(), Size(), Mode(), ModTime()修改时间, IsDir(), Sys()等方法
	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	// Offset指向新内容待写的位置
	return &DBFile{Offset: stat.Size(), File: file}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	// os.PathSeparator, 操作系统指定的路径分隔符, win下为'\\'
	fileName := path + string(os.PathSeparator) + FileName
	fmt.Println(fileName)
	return newInternal(fileName)
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	// 先读固定10B大小的header, 从中解码出Key和Value该读多大
	buf := make([]byte, entryHeaderSize)
	// 因为err在返回值列表声明了, 所以这里直接赋值使用即可
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	if e, err = Decode(buf); err != nil {
		return
	}

	offset += entryHeaderSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}
	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// Write 写入Entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}

// NewMergeDBFile 新建一个合并时的数据文件
func NewMergeDBFile(path string) (*DBFile, error) {
	fileName := path + string(os.PathSeparator) + MergeFileName
	return newInternal(fileName)
}