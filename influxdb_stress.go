package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"sync"
	"text/template"
	"time"

	influx "github.com/influxdb/influxdb/client"
	flag "launchpad.net/gnuflag"
)

type Config struct {
	Address string
	DBName  string
	User    string
	Pwd     string

	Writers              int
	WriterSeriesCnt      int
	WriterSeriesBaseName string
	WriterBatchSize      int

	Readers int
	QueriesFile string
	Queries []string

	Interval int
	ResetDB  bool
	Help     bool
}

const (
	defaultAddress              = "localhost:8086"
	defaultDatabase             = "stress_test"
	defaultUser                 = "root"
	defaultPwd                  = "root"
	defaultWriters              = 10
	defaultWriterSeriesCnt      = 10
	defaultWriterSeriesBaseName = "stress"
	defaultWriterBatchSize      = 10
	defaultReaders              = 10
	defaultQueriesFile			= ""
	defaultInterval             = 1000
)

var cfg *Config

func main() {
	cfg = getConfig()

	if cfg.Help {
		flag.Usage()
		return
	}

	if cfg.WriterSeriesCnt < cfg.WriterBatchSize {
		cfg.WriterSeriesCnt = cfg.WriterBatchSize
	}

	if cfg.QueriesFile != "" {
		var err error
		cfg.Queries, err = loadTextFile(cfg.QueriesFile)
		fatalIfErr(err)
	} else {
		cfg.Queries = queryTemplates
	}

	rand.Seed(time.Now().UnixNano())

	if cfg.ResetDB {
		err := deleteDB(cfg)
		fatalIfErr(err)
	}

	err := createDB(cfg)
	fatalIfErr(err)

	allClientsDone := startClients(cfg)
	<-allClientsDone
}

func startClients(cfg *Config) chan struct{} {
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		log.Printf("Starting %d writers (randomly delayed)\n", cfg.Writers)
		for i := 0; i < cfg.Writers; i++ {
			wg.Add(1)
			name := fmt.Sprintf("writer%d", i)
			dbw := NewDBWriter(name, cfg, &wg)
			go dbw.Run()
		}

		log.Printf("Starting %d readers (randomly delayed)\n", cfg.Readers)
		for i := 0; i < cfg.Readers; i++ {
			wg.Add(1)
			name := fmt.Sprintf("reader%d", i)
			dbr := NewDBReader(name, cfg, &wg)
			go dbr.Run()
		}
		wg.Wait()
		close(done)
	}()
	return done
}

func createDB(cfg *Config) error {
	clientCfg := &influx.ClientConfig{Host: cfg.Address, Username: cfg.User, Password: cfg.Pwd}
	client, err := influx.NewClient(clientCfg)
	if err != nil {
		return err
	}
	dblist, err := client.GetDatabaseList()
	if err != nil {
		return err
	}
	if !dbExists(cfg.DBName, dblist) {
		log.Printf("Creating database: %s\n", cfg.DBName)
		err = client.CreateDatabase(cfg.DBName)
	} else {
		log.Printf("Database already exists: %s\n", cfg.DBName)
	}
	return err
}

func deleteDB(cfg *Config) error {
	clientCfg := &influx.ClientConfig{Host: cfg.Address, Username: cfg.User, Password: cfg.Pwd}
	client, err := influx.NewClient(clientCfg)
	if err != nil {
		return err
	}
	dblist, err := client.GetDatabaseList()
	if err != nil {
		return err
	}
	if dbExists(cfg.DBName, dblist) {
		log.Printf("Deleting old database: %s\n", cfg.DBName)
		err = client.DeleteDatabase(cfg.DBName)
		if err != nil {
			return err
		}
	}
	return nil
}

type DBWriter struct {
	Name string
	cfg  *Config
	wg   *sync.WaitGroup
}

func NewDBWriter(name string, c *Config, wg *sync.WaitGroup) *DBWriter {
	dbw := &DBWriter{name, c, wg}
	return dbw
}

func (self *DBWriter) Run() {
	defer self.wg.Done()

	WriterSeriesCnt := self.cfg.WriterSeriesCnt
	batchSz := self.cfg.WriterBatchSize
	batchInterval := time.Duration(self.cfg.Interval)

	// Wait a pseudo-random amount of time before starting so that not all
	// clients start writing to the DB at the same time.
	startDelay := time.Duration(rand.Intn(30000))
	<-time.After(startDelay * time.Millisecond)

	log.Printf("%s: started\n", self.Name)
	defer log.Printf("%s: stoped\n", self.Name)

	// Connect to the db
	cfg := &influx.ClientConfig{Host: self.cfg.Address, Username: self.cfg.User, Password: self.cfg.Pwd, Database: self.cfg.DBName}
	client, err := influx.NewClient(cfg)
	fatalIfErr(err)

	// Generate array of series which we will periodically update and
	// send batches from.
	series := make([]*influx.Series, WriterSeriesCnt)
	for i := 0; i < WriterSeriesCnt; i++ {
		series[i] = &influx.Series{
			Name:    fmt.Sprintf("%s.%s.series%d", self.cfg.WriterSeriesBaseName, self.Name, i),
			Columns: []string{"value"},
			Points:  [][]interface{}{},
		}
	}

	// Main client loop
	i := 0
	batch := make([]*influx.Series, batchSz)
	for {
		// Select the next batch from series array
		for j := 0; j < batchSz; j++ {
			s := series[i%batchSz]
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

type DBReader struct {
	Name string
	cfg  *Config
	wg   *sync.WaitGroup
}

func NewDBReader(name string, c *Config, wg *sync.WaitGroup) *DBReader {
	dbr := &DBReader{name, c, wg}
	return dbr
}

func (self *DBReader) Run() {
	defer self.wg.Done()

	// Wait a pseudo-random amount of time before starting so that not all
	// clients read the DB at the same time.
	startDelay := time.Duration(rand.Intn(30000))
	<-time.After(startDelay * time.Millisecond)

	log.Printf("%s: started\n", self.Name)
	defer log.Printf("%s: stoped\n", self.Name)

	// Connect to the db
	cfg := &influx.ClientConfig{Host: self.cfg.Address, Username: self.cfg.User, Password: self.cfg.Pwd, Database: self.cfg.DBName}
	client, err := influx.NewClient(cfg)
	fatalIfErr(err)

	queries := self.cfg.Queries

	// Main read loop
	readInterval := time.Duration(self.cfg.Interval)
	for {
		tmplTxt := queries[rand.Intn(len(queries))]
		tmpl := template.Must(template.New("query").Funcs(queryTemplateFuncs).Parse(tmplTxt))
		var query bytes.Buffer
		err = tmpl.Execute(&query, nil)
		fatalIfErr(err)
		log.Printf("query = %s\n", query.String())
		_, err := client.Query(query.String())
		logIfErr(err)
		<-time.After(readInterval * time.Millisecond)
	}
}

func randSeries() string {
	w := rand.Intn(cfg.Writers)
	s := rand.Intn(cfg.WriterSeriesCnt)
	series := fmt.Sprintf("%s.writer%d.series%d", cfg.WriterSeriesBaseName, w, s)
	return series
}

var aggregates = []string{
	"count(value)",
	"min(value)",
	"max(value)",
	"mean(value)",
	"mode(value)",
	"median(value)",
	"distinct(value)",
	"percentile(value, 10)",
	"histogram(value)",
	"histogram(value, 10)",
	"derivative(value)",
	"sum(value)",
	"stddev(value)",
	"first(value)",
	"last(value)",
	"difference(value)",
	"top(value, 10)",
	"bottom(value, 10)",
}

func randAggregate() string {
	return aggregates[rand.Intn(len(aggregates))]
}

var queryTemplateFuncs = template.FuncMap{
	"randSeries":    randSeries,
	"randAggregate": randAggregate,
}

var queryTemplates = []string{
	"list series",
	"select * from {{randSeries}}",
	"select {{randAggregate}} from {{randSeries}}",
	"select derivative(value) from {{randSeries}} group by time(20s)",
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

func logIfErr(err error) {
	if err != nil {
		log.Println(err)
	}
}

func loadTextFile(path string) ([]string, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(string(b), "\n")
	return lines, nil
}

func getConfig() *Config {
	var cfg Config
	flag.StringVar(&cfg.Address, "a", defaultAddress, "IP:port of InfluxDB")
	flag.StringVar(&cfg.Address, "address", defaultAddress, "IP:port of InfluxDB")
	flag.StringVar(&cfg.DBName, "d", defaultDatabase, "influxDB database name")
	flag.StringVar(&cfg.DBName, "database", defaultDatabase, "influxDB database name")
	flag.StringVar(&cfg.User, "u", defaultUser, "influxDB user")
	flag.StringVar(&cfg.User, "user", defaultUser, "influxDB user")
	flag.StringVar(&cfg.Pwd, "p", defaultPwd, "influxDB password")
	flag.StringVar(&cfg.Pwd, "password", defaultPwd, "influxDB password")
	flag.IntVar(&cfg.Writers, "w", defaultWriters, "number of writers to create")
	flag.IntVar(&cfg.Writers, "writers", defaultWriters, "number of writers to create")
	flag.IntVar(&cfg.WriterSeriesCnt, "s", defaultWriterSeriesCnt, "number of series to generate per writer")
	flag.IntVar(&cfg.WriterSeriesCnt, "series-cnt", defaultWriterSeriesCnt, "number of series to generate per writer")
	flag.StringVar(&cfg.WriterSeriesBaseName, "n", defaultWriterSeriesBaseName, "series base name")
	flag.StringVar(&cfg.WriterSeriesBaseName, "series-name", defaultWriterSeriesBaseName, "series base name")
	flag.IntVar(&cfg.WriterBatchSize, "b", defaultWriterBatchSize, "number of series to write per interval")
	flag.IntVar(&cfg.WriterBatchSize, "batch-size", defaultWriterBatchSize, "number of series to write per interval")
	flag.IntVar(&cfg.Readers, "r", defaultReaders, "number of readers to create")
	flag.IntVar(&cfg.Readers, "readers", defaultReaders, "number of readers to create")
	flag.StringVar(&cfg.QueriesFile, "q", defaultQueriesFile, "path to file containing query templates")
	flag.StringVar(&cfg.QueriesFile, "queries-file", defaultQueriesFile, "path to file containing query templates")
	flag.IntVar(&cfg.Interval, "i", defaultInterval, "milliseconds between writing batches")
	flag.IntVar(&cfg.Interval, "interval", defaultInterval, "milliseconds between writing batches")
	flag.BoolVar(&cfg.ResetDB, "reset-db", false, "drops & creates new database before starting")
	flag.BoolVar(&cfg.Help, "h", false, "show usage")
	flag.BoolVar(&cfg.Help, "help", false, "show usage")
	flag.Parse(false)
	return &cfg
}
