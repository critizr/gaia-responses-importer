package main

import (
	"database/sql"
	"encoding/json"
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
	argURL         = flag.String("url", "https://api.critizr.com/v2", "Gaia base URL")
)

type APIError struct {
	Status  int
	Payload string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error: HTTP %d > %s", e.Status, e.Payload)
}

type ResponsePayload struct {
	ID string
}

type Entry struct {
	UID        string
	Payload    string
	ResponseId *string
	ImportedAt *string
	Err        error
	ImportTime int64
}

func makeEntry(rows *sql.Rows) (entry Entry, err error) {
	err = rows.Scan(&entry.UID, &entry.Payload, &entry.ImportedAt)
	if err != nil {
		return Entry{}, err
	}
	return entry, nil
}

func (e *Entry) doImport() error {
	req, err := http.NewRequest("POST", *argURL+"/responses", strings.NewReader(e.Payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", *argToken)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	e.ImportTime = time.Since(start).Milliseconds()
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 201 {
		e.Err = &APIError{resp.StatusCode, string(body)}
		return fmt.Errorf("unexpected status: %v", e.Err)
	}

	var response ResponsePayload
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to parse payload: %s", body)
	}
	e.ResponseId = &response.ID

	return nil
}

func (e *Entry) markImported(db *sql.DB) error {
	statement, err := db.Prepare("UPDATE imports SET response_id = ?, imported_at = ?, import_time_ms = ? WHERE uid = ?")
	if err != nil {
		return err
	}
	defer statement.Close()
	now := time.Now().UTC()
	_, err = statement.Exec(e.ResponseId, now.Format(time.RFC3339), e.ImportTime, e.UID)
	return err
}

func (e *Entry) markErrored(db *sql.DB) error {
	statement, err := db.Prepare("UPDATE imports SET error = ? WHERE uid = ?")
	if err != nil {
		return err
	}
	defer statement.Close()
	_, err = statement.Exec(e.Err.Error(), e.UID)
	return err
}

func fetchEntries(db *sql.DB) ([]Entry, error) {
	var entries []Entry
	rows, err := db.Query("SELECT uid, payload, imported_at FROM imports WHERE imported_at IS NULL")
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
				if entry.Err == nil {
					entry.Err = err
				}
				if err := entry.markErrored(db); err != nil {
					log.Printf("failed to mark error for entry %s: %s", entry.UID, err)
				}
			} else {
				if err := entry.markImported(db); err != nil {
					log.Printf("failed to mark import for entry %s: %s", entry.UID, err)
				}
			}
			sem <- true
			wg.Done()
		}(entry)
	}

	wg.Wait()
}
