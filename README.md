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
```

## Schema

```sql
CREATE TABLE IF NOT EXISTS imports (
    uid TEXT NOT NULL UNIQUE,
    payload TEXT NOT NULL,
    imported_at TEXT
);
```

## Linux cross-compilation

```sh
$ brew install FiloSottile/musl-cross/musl-cross # macOS only
$ ./build_linux
```