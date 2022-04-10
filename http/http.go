package http

import (
	"net/http"
	"strconv"

	"github.com/dovics/pangolin"
	_ "github.com/dovics/pangolin/lsmt"
	"github.com/gin-gonic/gin"
)

func NewRangeHandler(db *pangolin.DB) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		startStr, ok := ctx.GetQuery("start")
		if !ok {
			ctx.JSON(http.StatusBadRequest, gin.H{"status": http.StatusBadRequest})
			return
		}

		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"status": http.StatusBadRequest})
			return
		}

		endStr, ok := ctx.GetQuery("start")
		if !ok {
			ctx.JSON(http.StatusBadRequest, gin.H{"status": http.StatusBadRequest})
			return
		}

		end, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"status": http.StatusBadRequest})
			return
		}

		result, err := db.GetRange(start, end, nil)
		if err != nil {
			ctx.JSON(http.StatusBadGateway, gin.H{"status": http.StatusBadGateway})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{"result": result})
	}
}
