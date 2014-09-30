package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"
	"sync"

	influx "github.com/influxdb/influxdb/client"
)

type Config struct {
	Address        string
	DBName         string
	User           string
	Pwd            string
	ClientCnt      int
	SeriesCnt      int
	SeriesBaseName string
	BatchSize      int
	Interval       int
}

func main() {
	var help bool
	var cfg Config
	flag.StringVar(&cfg.Address, "a", "localhost:8086", "IP:port of InfluxDB")
	flag.StringVar(&cfg.DBName, "d", "stress_test", "influxDB database name")
	flag.StringVar(&cfg.User, "u", "root", "influxDB user")
	flag.StringVar(&cfg.Pwd, "p", "root", "influxDB password")
	flag.IntVar(&cfg.ClientCnt, "c", 10, "number of influxDB clients to create")
	flag.IntVar(&cfg.SeriesCnt, "s", 10, "number of series to generate per client")
	flag.StringVar(&cfg.SeriesBaseName, "n", "stress", "series base name")
	flag.IntVar(&cfg.BatchSize, "b", 10, "number of series to send per interval")
	flag.IntVar(&cfg.Interval, "i", 1000, "milliseconds between batches")
	flag.BoolVar(&help, "h", false, "show usage")
	flag.Parse()

	if help {
		flag.Usage()
		return
	}

	if cfg.SeriesCnt < cfg.BatchSize {
		cfg.SeriesCnt = cfg.BatchSize
	}

	rand.Seed(time.Now().UnixNano())

	// Delete old stress test database, if it exists, and create new / empty db
	clientCfg := &influx.ClientConfig{Host: cfg.Address, Username: cfg.User, Password: cfg.Pwd}
	client, err := influx.NewClient(clientCfg)
	fatalIfErr(err)
	dblist, err := client.GetDatabaseList()
	fatalIfErr(err)
	if dbExists(cfg.DBName, dblist) {
		log.Printf("Deleting old database: %s\n", cfg.DBName)
		err = client.DeleteDatabase(cfg.DBName)
		fatalIfErr(err)
	}
	log.Printf("Creating database: %s\n", cfg.DBName)
	err = client.CreateDatabase(cfg.DBName)
	fatalIfErr(err)

	log.Printf("Starting %d InfluxDB clients (randomly delayed)\n", cfg.ClientCnt)
	var wg sync.WaitGroup
	for i := 0; i < cfg.ClientCnt; i++ {
		wg.Add(1)
		name := fmt.Sprintf("client%d", i)
		c := NewDBClient(name, &cfg, &wg)
		go c.Run()
	}
	wg.Wait()
}

type DBClient struct {
	Name string
	cfg *Config
	wg *sync.WaitGroup
}

func NewDBClient(name string, c *Config, wg *sync.WaitGroup) *DBClient {
	dbc := &DBClient{name, c, wg}
	return dbc
}

func (self *DBClient) Run() {
	defer self.wg.Done()

	seriesCnt := self.cfg.SeriesCnt
	batchSz := self.cfg.BatchSize
	batchInterval := time.Duration(self.cfg.Interval)

	// Wait a pseudo-random amount of time before starting so that not all
	// clients start writing to the DB at the same time.
	startDelay := time.Duration(rand.Intn(30000))
	<-time.After(startDelay * time.Millisecond)

	fmt.Printf("%s: started\n", self.Name)
	defer fmt.Printf("%s: stoped\n", self.Name)

	

	// Connect to the new db
	cfg := &influx.ClientConfig{Host: self.cfg.Address, Username: self.cfg.User, Password: self.cfg.Pwd, Database: self.cfg.DBName}
	client, err := influx.NewClient(cfg)
	fatalIfErr(err)

	// Generate array of series which we will periodically update and
	// send batches from.
	series := make([]*influx.Series, seriesCnt)
	for i := 0; i < seriesCnt; i++ {
		series[i] = &influx.Series{
			Name: fmt.Sprintf("%s.%s.series%d", self.cfg.SeriesBaseName, self.Name, i),
			Columns: []string{"value"},
			Points: [][]interface{}{},
		}
	}
	
	// Main client loop
	i := 0
	batch := make([]*influx.Series, batchSz)
	for {
		// Select the next batch from series array
		for j := 0; j < batchSz; j++ {
			s := series[i % batchSz]
			// generate new value
			genNewValue(s)
			batch[j] = s
			i++
		}

		// Write the batch to InfluxDB
		err = client.WriteSeries(batch)
		fatalIfErr(err)
		log.Printf("%s wrote a batch of %d series\n", self.Name, batchSz)

		// Wait till time to send the next batch
		<-time.After(batchInterval * time.Millisecond)
	}
}

func genNewValue(s *influx.Series) {
	newVal := []interface{}{rand.Int()}
	s.Points = [][]interface{}{newVal}
}

func dbExists(dbname string, dbList []map[string]interface{}) bool {
	for _, db := range dbList {
		if dbname == db["name"] {
			return true
		}
	}
	return false
}

func fatalIfErr(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}