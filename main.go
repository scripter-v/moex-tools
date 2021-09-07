package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"

	"github.com/scripter-v/moex-tools/pkg/sdk"
)

func main() {
	market := flag.String("market", "", "market: currency|stock")
	ticker := flag.String("ticker", "", "ticker in format ABCD")
	interval := flag.String("interval", "1", "candle interval in minutes")
	from := flag.String("from", "", "start of requested interval")
	flag.Parse()

	if len(*ticker) == 0 || len(*from) == 0 || len(*market) == 0 {
		flag.Usage()
		return
	}

	var cursor *sdk.Cursor
	var err error

	switch *market {
	case "currency":
		cursor, err = sdk.GetCurrencyCandles(*ticker, *interval, *from, "")
	case "stock":
		cursor, err = sdk.GetStockCandles(*ticker, *interval, *from, "")
	}

	if err != nil {
		log.Println(err)
		return
	}

	csvWriter := csv.NewWriter(os.Stdout)
	if err := csvWriter.Write(cursor.GetHeaders()); err != nil {
		log.Println(err)
		return
	}

	for cursor.Next() {
		if err := csvWriter.Write(cursor.GetRow()); err != nil {
			log.Println(err)
			return
		}
	}

	if err := cursor.Err(); err != nil {
		log.Println(err)
		return
	}

	csvWriter.Flush()
}
