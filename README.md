# Pangolin

Pangolin is a minio-based time series storage using lsm-tree

## Example

```go
package main

import (
	"fmt"
	"log"

	"github.com/dovics/pangolin"
	_ "github.com/dovics/pangolin/lsmt"
	"github.com/google/uuid"
)

func main() {
	db, err := pangolin.OpenDB(pangolin.DefaultOption(uuid.NewString()))
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		if err := db.Insert(int64(i), i); err != nil {
			log.Fatal(err)
		}
	}

	result, err := db.GetRange(20, 40, nil)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result)
}

```