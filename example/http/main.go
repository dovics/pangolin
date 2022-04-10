package main

import (
	"log"

	"github.com/dovics/pangolin"
	"github.com/dovics/pangolin/http"
	_ "github.com/dovics/pangolin/lsmt"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func main() {
	db, err := pangolin.OpenDB(pangolin.DefaultOption(uuid.NewString()))
	if err != nil {
		log.Fatal(err)
	}

	engine := gin.Default()
	engine.GET("range", http.NewRangeHandler(db))

	engine.Run(":8080")
}
