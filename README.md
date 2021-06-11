# Gaia responses importer

## Usage (-h)

```
Usage of gaia-responses-importer:
  -db string
        path to the database to import (default "./import.db")
  -j int
        level of concurrency (simultaneous tasks) (default 5)
  -token string
        Gaia API token
  -url string
        Gaia base URL (default "https://api.critizr.com/v2")
```

## Schema

```sql
CREATE TABLE IF NOT EXISTS imports (
    uid TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    response_id TEXT,
    imported_at TEXT,
    error TEXT,
    import_time_ms INTEGER
);
```

## Linux cross-compilation

```sh
$ brew install FiloSottile/musl-cross/musl-cross # macOS only
$ ./build_linux
```

## Export to CSV

```
sqlite3 -header -csv ./import.db "SELECT * FROM import ORDER BY imported_at DSC;" > import.csv
```