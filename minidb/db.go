package minidb

import (
	"io"
	"os"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile *DBFile	// 数据文件
	dirPath string	// 数据目录
	mu sync.RWMutex
}

// Open 开启一个数据库实例
func Open(dirPath string) (*MiniDB, error) {
	// 如果数据库目录不存在, 则新建一个
	// 把err传入os.IsNotExist(), 如果文件/目录不存在则返回ture
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		// 创建目录及必要的上级目录, const os.ModePerm FileMode = 0777
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	// 上面这个对if init; condition {} 应用的很到位, err局部变量就在这里用, 就在if中声明

	// 加载数据文件
	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	// 创建一个MiniDB实例
	db := &MiniDB{
		dbFile: dbFile,
		indexes: make(map[string]int64),
		dirPath: dirPath,
	}

	// 加载索引
	db.loadIndexesFromFile(dbFile)
	return db, nil
}

func (db *MiniDB) loadIndexesFromFile(dbFile *DBFile) {
	if dbFile == nil {
		return
	}
	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			// 读取完毕
			if err == io.EOF {
				break
			}
			return
		}

		// 设置索引状态
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			// 删除内存中的key
			delete(db.indexes, string(e.Key))
		}
		offset += e.GetSize()
	}
	return
}

// Put 写入数据
func (db *MiniDB) Put(key, value []byte) (err error) {
	if len(key) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// 找到文件应该写入的位置
	offset := db.dbFile.Offset
	// 封装成Entry
	entry := NewEntry(key, value, PUT)
	// 追加到数据文件中
	err = db.dbFile.Write(entry)
	// 写到内存
	db.indexes[string(key)] = offset
	return
}

// Get 读取数据
func (db *MiniDB) Get(key []byte) (val []byte, err error) {
	if len(key) == 0 {
		return
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 从内存当中取出索引信息
	offset, ok := db.indexes[string(key)]
	// key 不存在
	if !ok {
		return
	}
	// 从磁盘中读取数据
	var e *Entry
	e, err = db.dbFile.Read(offset)
	if err != nil && err != io.EOF {
		return
	}
	if e != nil {
		val = e.Value
	}
	return
}

// Del 删除数据
func (db *MiniDB) Del(key []byte) (err error) {
	if len(key) == 0 {
		return
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// 从内存中取出索引信息
	_, ok := db.indexes[string(key)]
	// key 不存在, 忽略
	if !ok {
		return
	}
	// 封装成Entry并写入
	e := NewEntry(key, nil, DEL)
	err = db.dbFile.Write(e)
	if err != nil {
		return
	}
	// 删除内存中的key
	delete(db.indexes, string(key))
	return
}

// Merge 合并数据文件, 在rosedb中是Reclaim方法
func (db *MiniDB) Merge() error {
	// 没有数据, 忽略
	if db.dbFile.Offset == 0 {
		return nil
	}
	var (
		validEntries []*Entry
		offset int64
	)
	// 读取元数据文件中的Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的, 直接对比过滤出有效的Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		// 新建临时文件
		mergeDBFile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBFile.File.Name())
		// 重新写入有效的entry
		for _, entry := range validEntries {
			writeOff := mergeDBFile.Offset
			err := mergeDBFile.Write(entry)
			if err != nil {
				return err
			}
			// 更新内存索引
			db.indexes[string(entry.Key)] = writeOff
		}
		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)
		// 获取文件名
		mergeDBFileName := mergeDBFile.File.Name()
		mergeDBFile.File.Close()
		os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+FileName)
		db.dbFile = mergeDBFile
	}
	return nil
}