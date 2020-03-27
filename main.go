package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	argConcurrency = flag.Int("j", 5, "level of concurrency (simultaneous tasks)")
	argDb          = flag.String("db", "./import.db", "path to the database to import")
	argToken       = flag.String("token", "", "Gaia API token")
)

type Entry struct {
	UID        string
	Payload    string
	ImportedAt *string
}

func makeEntry(rows *sql.Rows) (entry Entry, err error) {
	err = rows.Scan(&entry.UID, &entry.Payload, &entry.ImportedAt)
	if err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (e *Entry) doImport() error {
	req, err := http.NewRequest("POST", "https://api.critizr.com/v2/responses", strings.NewReader(e.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", *argToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("unexpected http status %d: %s", resp.StatusCode, body)
	}

	return nil
}

func (e *Entry) markImported(db *sql.DB) error {
	statement, err := db.Prepare("UPDATE imports SET imported_at = ? WHERE uid = ?")
	if err != nil {
		return err
	}
	defer statement.Close()
	now := time.Now().UTC()
	_, err = statement.Exec(now.Format(time.RFC3339), e.UID)
	return err
}

func fetchEntries(db *sql.DB) ([]Entry, error) {
	var entries []Entry
	rows, err := db.Query("SELECT * FROM imports WHERE imported_at IS NULL")
	if err != nil {
		return entries, err
	}
	for rows.Next() {
		entry, err := makeEntry(rows)
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func main() {
	flag.Parse()
	if *argToken == "" {
		log.Fatal("an API token is needed")
	}

	db, err := sql.Open("sqlite3", *argDb)
	if err != nil {
		log.Fatalf("failed to open database: %s", err)
	}
	defer db.Close()

	entries, err := fetchEntries(db)
	if err != nil {
		log.Fatalf("failed to fetch data: %s", err)
	}

	var wg sync.WaitGroup
	sem := make(chan bool, *argConcurrency)
	for i := 0; i < *argConcurrency; i++ {
		sem <- true
	}
	defer close(sem)

	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("%d entries to process", len(entries))
loop:
	for _, entry := range entries {
		select {
		case <-stop:
			log.Print("stop signal received, preparing termination...")
			break loop
		case <-sem:
		}
		wg.Add(1)
		go func(entry Entry) {
			log.Printf("processing entry %s", entry.UID)
			if err := entry.doImport(); err != nil {
				log.Printf("failed to import entry %s: %s", entry.UID, err)
				goto Done
			}
			if err := entry.markImported(db); err != nil {
				log.Printf("failed to mark import for entry %s: %s", entry.UID, err)
				goto Done
			}
		Done:
			sem <- true
			wg.Done()
		}(entry)
	}

	wg.Wait()
}
