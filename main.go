package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const moexISSURL = "https://iss.moex.com/iss"

var mskLocation *time.Location

func init() {
	location, err := time.LoadLocation("Europe/Moscow")
	if err != nil {
		panic(err)
	}
	mskLocation = location
}

func main() {
	ticker := flag.String("ticker", "", "ticker in format ABCD")
	from := flag.String("from", "", "start of requested interval")
	flag.Parse()

	if len(*ticker) == 0 || len(*from) == 0 {
		flag.Usage()
		return
	}

	cursor, err := getCandles(*ticker, "1", *from, "")
	if err != nil {
		fmt.Println(err)
		return
	}

	csvWriter := csv.NewWriter(os.Stdout)
	if err := csvWriter.Write(cursor.GetHeaders()); err != nil {
		fmt.Println(err)
		return
	}

	for cursor.Next() {
		if err := csvWriter.Write(cursor.GetRow()); err != nil {
			fmt.Println(err)
			return
		}
	}

	if err := cursor.Err(); err != nil {
		fmt.Println(err)
		return
	}

	csvWriter.Flush()
}

type cursor struct {
	done   bool
	err    error
	offset int
	cf     *chunkFetcher
}

func (c *cursor) Next() bool {
	if c.err != nil || c.done {
		return false
	}

	c.cf.SetNextRow()

	if c.cf.Exhausted() {
		if err := c.cf.FetchNext(c.offset); err != nil {
			c.err = err
			return false
		}
		if c.cf.IsEmpty() {
			c.done = true
			return false
		}
		c.cf.SetNextRow() // set on first row (with index 0)
	}

	c.offset++

	return true
}

func (c cursor) Err() error {
	return c.err
}

func (c *cursor) GetRow() []string {
	return c.cf.GetChunkRow()
}

func (c cursor) GetHeaders() []string {
	return c.cf.GetColumnNames()
}

func PrepareCursor(cf *chunkFetcher) (*cursor, error) {
	if err := cf.FetchNext(0); err != nil {
		return nil, err
	}
	return &cursor{cf: cf}, nil
}

type chunkFetcher struct {
	columnNames    []string
	fetchNextChunk func(int) (int, error)
	getChunkRow    func(int) []string
	chunkOffset    int
	chunkSize      int
}

func (cf chunkFetcher) GetColumnNames() []string {
	return cf.columnNames
}

func (cf chunkFetcher) GetChunkRow() []string {
	if cf.chunkOffset >= 0 && cf.chunkOffset < cf.chunkSize {
		return cf.getChunkRow(cf.chunkOffset)
	}
	return nil
}

func (cf *chunkFetcher) SetNextRow() {
	cf.chunkOffset++
}

func (cf chunkFetcher) Exhausted() bool {
	return cf.chunkOffset >= cf.chunkSize
}

func (cf *chunkFetcher) FetchNext(offset int) error {
	chunkSize, err := cf.fetchNextChunk(offset)
	if err != nil {
		return err
	}

	cf.chunkSize = chunkSize
	cf.chunkOffset = -1

	return nil
}

func (cf chunkFetcher) IsEmpty() bool {
	return cf.chunkSize == 0
}

func getCandles(security, interval, from string, to string) (*cursor, error) {
	endpoint := fmt.Sprintf("/engines/stock/markets/shares/securities/%s/candles.json", security)
	reqURL, err := url.Parse(moexISSURL + endpoint)
	if err != nil {
		return nil, err
	}

	columns := []string{"begin", "open", "close", "volume"}

	setNonEmptyQueryParams(reqURL, map[string]string{
		"candles.columns": strings.Join(columns, ","),
		"from":            from,
		"interval":        interval,
		"till":            to,
	})

	var parsedResp struct {
		Candles struct {
			Metadata map[string]struct{ Type string }
			Columns  []string
			Data     [][]interface{}
		}
	}

	return PrepareCursor(&chunkFetcher{
		fetchNextChunk: func(offset int) (int, error) {
			setQueryParam(reqURL, "start", strconv.Itoa(offset))
			resp, err := http.Get(reqURL.String())
			if err != nil {
				return 0, err
			}
			defer resp.Body.Close()

			jsonDecoder := json.NewDecoder(resp.Body)
			if err := jsonDecoder.Decode(&parsedResp); err != nil {
				return 0, err
			}

			return len(parsedResp.Candles.Data), nil
		},
		getChunkRow: func(offset int) []string {
			values := make([]string, len(columns))
			values[0] = security
			for i := range columns {
				moexType := parsedResp.Candles.Metadata[columns[i]].Type
				value := parsedResp.Candles.Data[offset][i]
				parsedValue := parseMoexType(value, moexType)
				values[i] = fmt.Sprint(parsedValue)
			}
			return values
		},
		columnNames: columns,
	})
}

func parseMoexType(in interface{}, moexType string) interface{} {
	if moexType == "datetime" {
		if v, ok := in.(string); ok {
			// FIXME err
			t, _ := time.ParseInLocation("2006-01-02 15:04:05", v, mskLocation)
			in = t.Format(time.RFC3339)
		}
	}
	return in
}

func setQueryParam(u *url.URL, k string, v string) {
	q := u.Query()
	q.Set(k, fmt.Sprint(v))
	u.RawQuery = q.Encode()
}

func setNonEmptyQueryParams(u *url.URL, p map[string]string) {
	q := u.Query()
	for k, v := range p {
		if len(v) > 0 {
			q.Set(k, fmt.Sprint(v))
		}
	}
	u.RawQuery = q.Encode()
}
