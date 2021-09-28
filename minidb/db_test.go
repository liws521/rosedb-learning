package minidb

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// 单独跑一个test, go test -run=pattern

func TestOpen(t *testing.T) {
	db, err := Open(".\\tmp\\minidb")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestMinDB_Put(t *testing.T) {
	t.Log("hello")
	db, err := Open(".\\tmp\\minidb")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"
	for i := 0; i < 10000; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		// rand.Int63(), 返回一个int64类型的非负的63位伪随机数
		// strconv.FormatInt(num, 10), 返回数字的10进制表示的string
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Put(key, val)
	}
	if err != nil {
		t.Log(err)
	}
	stat, err := db.dbFile.File.Stat()
	if err != nil {
		t.Log(err)
	}
	fmt.Println(stat.Size())
}


func TestMiniDB_Get(t *testing.T) {
	db, err := Open(".\\tmp\\minidb")
	if err != nil {
		t.Error(err)
	}
	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("read val err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}
	getVal(([]byte("test_key_0")))
	getVal(([]byte("test_key_1")))
	getVal(([]byte("test_key_2")))
	getVal(([]byte("test_key_3")))
	getVal(([]byte("test_key_4")))
}

func TestMiniDB_Del(t *testing.T) {
	db, err := Open(".\\tmp\\minidb")
	if err != nil {
		t.Error(err)
	}
	key := []byte("test_key_3")
	err = db.Del(key)
	if err != nil {
		t.Error("del err: ", err)
	}
}

func TestMiniDB_Merge(t *testing.T) {
	db, err := Open(".\\tmp\\minidb")
	if err != nil {
		t.Error(err)
	}
	err = db.Merge()
	if err != nil {
		t.Error("merge err: ", err)
	}
}