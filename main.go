package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"

	"github.com/mcgraw-bb25/csvcake/v6/iterstructscanner"
)

// go get github.com/jmoiron/sqlx
// go get github.com/lib/pq

/*
pg
CREATE DATABASE au418;
CREATE USER au418 WITH LOGIN ENCRYPTED PASSWORD 'performance-test';
GRANT ALL PRIVILEGES ON DATABASE au418 TO au418;

cr
CREATE DATABASE au418;
CREATE USER au418;
GRANT ALL ON DATABASE au418 TO au418;
*/

const battingFile string = `data/Batting.csv`
const fieldingFile string = `data/Fielding.csv`
const pitchingFile string = `data/Pitching.csv`

const initQry = `
drop table if exists Batting;
create table Batting (
    PlayerID text,
    YearID text,
    Stint text,
    TeamID text,
    LgID text,
    G text,
    AB text,
    R text,
    H text,
    H2B text,
    H3B text,
    HR text,
    RBI text,
    SB text,
    CS text,
    BB text,
    SO text,
    IBB text,
    HBP text,
    SH text,
    SF text,
    GIDP text
);

--drop index if exists idx_batting_playerid_yearid;
--create index idx_batting_playerid_yearid on Batting(PlayerID, YearID);

drop table if exists Pitching;
create table Pitching (
    PlayerID text
    , YearID text
    , Stint text
    , TeamID text
    , LGID text
    , W text
    , L text
    , G text
    , GS text
    , CG text
    , SHO text
    , SV text
    , IPouts text
    , H text
    , ER text
    , HR text
    , BB text
    , SO text
    , BAOpp text
    , ERA text
    , IBB text
    , WP text
    , HBP text
    , BK text
    , BFP text
    , GF text
    , R text
    , SH text
    , SF text
    , GIDP text
);

--drop index if exists idx_pitching_playerid_yearid;
--create index idx_pitching_playerid_yearid on Pitching(PlayerID, YearID);

drop table if exists Fielding;
create table Fielding (
    PlayerID text
    , YearID text
    , Stint text
    , TeamID text
    , LGID text
    , POS text
    , G text
    , GS text
    , InnOuts text
    , PO text
    , A text
    , E text
    , DP text
    , PB text
    , WP text
    , SB text
    , CS text
    , ZR text
);

--drop index if exists idx_fielding_playerid_yearid;
--create index idx_fielding_playerid_yearid on Fielding(PlayerID, YearID);
`

type Insertable interface {
	Insert(db *sql.DB) error
}

func insertLoop(db *sql.DB, records <-chan Insertable, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		rec, more := <-records
		if more {
			err := rec.Insert(db)
			if err != nil {
				fmt.Printf("Could not insert %+v\n", rec)
				fmt.Println(err)
			}
		} else {
			break
		}
	}
}

// BattingRecord maps to a row from Lahman's batting.csv file
type BattingRecord struct {
	PlayerID string `csvcake:"playerID"`
	YearID   string `csvcake:"yearID"`
	Stint    string `csvcake:"stint"`
	TeamID   string `csvcake:"teamID"`
	LgID     string `csvcake:"lgID"`
	G        string `csvcake:"G"`
	AB       string `csvcake:"AB"`
	R        string `csvcake:"R"`
	H        string `csvcake:"H"`
	H2B      string `csvcake:"2B"`
	H3B      string `csvcake:"3B"`
	HR       string `csvcake:"HR"`
	RBI      string `csvcake:"RBI"`
	SB       string `csvcake:"SB"`
	CS       string `csvcake:"CS"`
	BB       string `csvcake:"BB"`
	SO       string `csvcake:"SO"`
	IBB      string `csvcake:"IBB"`
	HBP      string `csvcake:"HBP"`
	SH       string `csvcake:"SH"`
	SF       string `csvcake:"SF"`
	GIDP     string `csvcake:"GIDP"`
}

func (b BattingRecord) Insert(db *sql.DB) error {
	qry := `
        insert into Batting(PlayerID, YearID, Stint, TeamID, LgID, G, AB, R, H, H2B, H3B, HR, RBI, SB, CS, BB, SO, IBB, HBP, SH, SF, GIDP)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
    `
	_, err := db.Exec(qry, b.PlayerID, b.YearID, b.Stint, b.TeamID, b.LgID, b.G, b.AB, b.R, b.H, b.H2B, b.H3B, b.HR, b.RBI, b.SB, b.CS, b.BB, b.SO, b.IBB, b.HBP, b.SH, b.SF, b.GIDP)
	if err != nil {
		return err
	}
	return nil
}

func getBattingRecords(insertQueue chan<- Insertable, wg *sync.WaitGroup) {
	defer wg.Done()

	modelBattingRecord := BattingRecord{}
	battingCSVScanner, err := iterstructscanner.NewIterStructScanner(battingFile, modelBattingRecord)
	if err != nil {
		panic(err)
	}
	defer battingCSVScanner.Close()

	for {
		row, err := battingCSVScanner.Next()
		if err != nil {
			break
		}
		battingRecord := row.(BattingRecord)
		insertQueue <- battingRecord
	}

}

// PitchingRecord maps to a row from Lahman's pitching.csv file
type PitchingRecord struct {
	PlayerID string `csvcake:"playerID"`
	YearID   string `csvcake:"yearID"`
	Stint    string `csvcake:"stint"`
	TeamID   string `csvcake:"teamID"`
	LGID     string `csvcake:"lgID"`
	W        string `csvcake:"W"`
	L        string `csvcake:"L"`
	G        string `csvcake:"G"`
	GS       string `csvcake:"GS"`
	CG       string `csvcake:"CG"`
	SHO      string `csvcake:"SHO"`
	SV       string `csvcake:"SV"`
	IPouts   string `csvcake:"IPouts"`
	H        string `csvcake:"H"`
	ER       string `csvcake:"ER"`
	HR       string `csvcake:"HR"`
	BB       string `csvcake:"BB"`
	SO       string `csvcake:"SO"`
	BAOpp    string `csvcake:"BAOpp"`
	ERA      string `csvcake:"ERA"`
	IBB      string `csvcake:"IBB"`
	WP       string `csvcake:"WP"`
	HBP      string `csvcake:"HBP"`
	BK       string `csvcake:"BK"`
	BFP      string `csvcake:"BFP"`
	GF       string `csvcake:"GF"`
	R        string `csvcake:"R"`
	SH       string `csvcake:"SH"`
	SF       string `csvcake:"SF"`
	GIDP     string `csvcake:"GIDP"`
}

func (p PitchingRecord) Insert(db *sql.DB) error {
	qry := `
        insert into Pitching(PlayerID, YearID, Stint, TeamID, LGID, W, L, G, GS, CG, SHO, SV, IPouts, H, ER, HR, BB, SO, BAOpp, ERA, IBB, WP, HBP, BK, BFP, GF, R, SH, SF, GIDP)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30)
    `
	_, err := db.Exec(qry, p.PlayerID, p.YearID, p.Stint, p.TeamID, p.LGID, p.W, p.L, p.G, p.GS, p.CG, p.SHO, p.SV, p.IPouts, p.H, p.ER, p.HR, p.BB, p.SO, p.BAOpp, p.ERA, p.IBB, p.WP, p.HBP, p.BK, p.BFP, p.GF, p.R, p.SH, p.SF, p.GIDP)
	if err != nil {
		return err
	}
	return nil
}

func getPitchingRecords(insertQueue chan<- Insertable, wg *sync.WaitGroup) {
	defer wg.Done()

	modelPitchingRecord := PitchingRecord{}
	pitchingCSVScanner, err := iterstructscanner.NewIterStructScanner(pitchingFile, modelPitchingRecord)
	if err != nil {
		panic(err)
	}
	defer pitchingCSVScanner.Close()

	for {
		row, err := pitchingCSVScanner.Next()
		if err != nil {
			break
		}
		pitchingRecord := row.(PitchingRecord)
		insertQueue <- pitchingRecord
	}

}

type FieldingRecord struct {
	PlayerID string `csvcake:"playerID"`
	YearID   string `csvcake:"yearID"`
	Stint    string `csvcake:"stint"`
	TeamID   string `csvcake:"teamID"`
	LGID     string `csvcake:"lgID"`
	POS      string `csvcake:"POS"`
	G        string `csvcake:"G"`
	GS       string `csvcake:"GS"`
	InnOuts  string `csvcake:"InnOuts"`
	PO       string `csvcake:"PO"`
	A        string `csvcake:"A"`
	E        string `csvcake:"E"`
	DP       string `csvcake:"DP"`
	PB       string `csvcake:"PB"`
	WP       string `csvcake:"WP"`
	SB       string `csvcake:"SB"`
	CS       string `csvcake:"CS"`
	ZR       string `csvcake:"ZR"`
}

func (f FieldingRecord) Insert(db *sql.DB) error {
	qry := `
        insert into Fielding(PlayerID, YearID, Stint, TeamID, LGID, POS, G, GS, InnOuts, PO, A, E, DP, PB, WP, SB, CS, ZR)
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    `
	_, err := db.Exec(qry, f.PlayerID, f.YearID, f.Stint, f.TeamID, f.LGID, f.POS, f.G, f.GS, f.InnOuts, f.PO, f.A, f.E, f.DP, f.PB, f.WP, f.SB, f.CS, f.ZR)
	if err != nil {
		return err
	}
	return nil
}

func getFieldingRecords(insertQueue chan<- Insertable, wg *sync.WaitGroup) {
	defer wg.Done()

	modelFieldingRecord := FieldingRecord{}
	fieldingCSVScanner, err := iterstructscanner.NewIterStructScanner(fieldingFile, modelFieldingRecord)
	if err != nil {
		panic(err)
	}
	defer fieldingCSVScanner.Close()

	for {
		row, err := fieldingCSVScanner.Next()
		if err != nil {
			break
		}
		fieldingRecord := row.(FieldingRecord)
		insertQueue <- fieldingRecord
	}

}

type WorkloadPerformance struct {
	Database             string
	Workers              int
	connStr              string
	IsMultiServer        bool // true database is on separate server
	IsDistributedDB      bool // true is a distributed database
	IsMultiNode          bool // true is a multi node database server
	IsMultiDC            bool // true is a multi data centre server
	MultiNodeCount       int
	TotalNetworkDistance float64
	Runtime              float64
	Records              int
	Performance          float64
}

func (w *WorkloadPerformance) runWorkload() error {

	var wg sync.WaitGroup
	allRecords := make(chan Insertable, 500000)

	fmt.Print("Gathering all records ... ")
	wg.Add(3)
	go getBattingRecords(allRecords, &wg)
	go getPitchingRecords(allRecords, &wg)
	go getFieldingRecords(allRecords, &wg)
	wg.Wait()
	close(allRecords)
	fmt.Print("Complete\n")
	fmt.Printf("%d records to insert\n", len(allRecords))
	w.Records = len(allRecords)

	db, err := sql.Open("postgres", w.connStr)
	if err != nil {
		fmt.Printf("Cannot open database connection to %s\n", w.connStr)
		return err
	}

	_, err = db.Exec(initQry)
	time.Sleep(15 * time.Second) // give time for DDL statements to sync
	if err != nil {
		fmt.Println("Cannot run database migrations")
		fmt.Println(err)
		return err
	}

	start := time.Now()

	var insertWG sync.WaitGroup
	i := 0
	for i < w.Workers {
		insertWG.Add(1)
		go insertLoop(db, allRecords, &insertWG)
		i++
	}
	insertWG.Wait()

	t := time.Now()
	elapsed := t.Sub(start)

	w.Runtime = elapsed.Seconds()

	w.Performance = float64(w.Records) / w.Runtime

	fmt.Printf("Completing => %+v\n", w)

	return nil

}

func (w *WorkloadPerformance) MarshallCSV() []string {

	x1 := fmt.Sprintf("%s", w.Database)
	x2 := fmt.Sprintf("%d", w.Workers)
	x3 := fmt.Sprintf("%t", w.IsMultiServer)
	x4 := fmt.Sprintf("%t", w.IsDistributedDB)
	x5 := fmt.Sprintf("%t", w.IsMultiNode)
	x6 := fmt.Sprintf("%t", w.IsMultiDC)
	x7 := fmt.Sprintf("%d", w.MultiNodeCount)
	x8 := fmt.Sprintf("%f", w.TotalNetworkDistance)
	x9 := fmt.Sprintf("%f", w.Runtime)
	x10 := fmt.Sprintf("%d", w.Records)
	x11 := fmt.Sprintf("%f", w.Performance)

	csvRow := []string{x1,
		x2,
		x3,
		x4,
		x5,
		x6,
		x7,
		x8,
		x9,
		x10,
		x11}
	return csvRow
}

func writeOutput(records [][]string) {
	header := []string{"Database",
		"Workers",
		"IsMultiServer",
		"IsDistributedDB",
		"IsMultiNode",
		"IsMultiDC",
		"MultiNodeCount",
		"TotalNetworkDistance",
		"Runtime",
		"Records",
		"Performance"}

	csvfile, err := os.Create("output.csv")

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	csvwriter := csv.NewWriter(csvfile)
	_ = csvwriter.Write(header)
	for _, row := range records {
		_ = csvwriter.Write(row)
	}

	csvwriter.Flush()

	csvfile.Close()

}

func main() {

	var connStr string
	flag.StringVar(&connStr, "connStr", pgConnStr, "Connection string to the database.")

	var dbVendor string
	flag.StringVar(&dbVendor, "dbVendor", "PostgreSQL", "Name of database vendor.")

	var isMultiServer bool
	flag.BoolVar(&isMultiServer, "isMultiServer", false, "Is the database on a separate server from the workload runner?")

	var isDistributedDB bool
	flag.BoolVar(&isDistributedDB, "isDistributedDB", false, "Is the database server a distributed database?")

	var isMultiNode bool
	flag.BoolVar(&isMultiNode, "isMultiNode", false, "Is the database server running with multiple nodes enabled?")

	var multiNodeCount int
	flag.IntVar(&multiNodeCount, "multiNodeCount", 1, "If multiple nodes are running, how many are running?")

	var isMultiDC bool
	flag.BoolVar(&isMultiDC, "isMultiDC", false, "Is the cluster of nodes running in multiple data centres?")

	var totalNetworkDist float64
	flag.Float64Var(&totalNetworkDist, "totalNetworkDist", 0.0, "If a multiple data centre environment, what is the linear distance between all data centres?")

	flag.Parse()

	workloads := make([]*WorkloadPerformance, 0)

	const testRunCounts int = 5
	workerCounts := [3]int{20, 40, 60}

	pgWorkload := WorkloadPerformance{
		Database:             dbVendor,
		Workers:              0,
		connStr:              connStr,
		IsMultiServer:        isMultiServer,
		IsDistributedDB:      isDistributedDB,
		IsMultiNode:          isMultiNode,
		IsMultiDC:            isMultiDC,
		TotalNetworkDistance: totalNetworkDist,
		MultiNodeCount:       multiNodeCount}

	for _, workerCount := range workerCounts {
		wrkld := pgWorkload
		wrkld.Workers = workerCount
		workloads = append(workloads, &wrkld)
	}

	output := [][]string{}

	x := 0
	for x < testRunCounts {
		for _, workload := range workloads {
			workload.runWorkload()
			record := workload.MarshallCSV()
			output = append(output, record)
		}
		x++
	}

	writeOutput(output)
}
