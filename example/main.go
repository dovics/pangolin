package main

import (
	"fmt"
	"log"
	"time"

	"github.com/dovics/pangolin"
	"github.com/dovics/pangolin/lsmt"
	_ "github.com/dovics/pangolin/lsmt"
	"github.com/google/uuid"
)

func main() {
	option := pangolin.DefaultOption(uuid.NewString())
	lsmt.DefaultOption.MemtableSize = 1024
	option.EngineOption = lsmt.DefaultOption
	db, err := pangolin.OpenDB(option)
	if err != nil {
		log.Fatal(err)
	}

	now := time.Now()
	go func() {
		i := 0
		for {
			if err := db.Insert(time.Now().UnixMilli(), i); err != nil {
				log.Fatal(err)
			}
			i++
			time.Sleep(time.Millisecond * 10)
		}
	}()

	time.Sleep(time.Second * 5)
	result, err := db.GetRange(now.UnixMilli(), now.Add(time.Second*5).UnixMilli(), nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result)
}
