# GoDB

High-performance embedded database engine in Go featuring B-tree indexing and LRU caching.

## Core Features

- In-memory storage with persistence
- B-tree indexes (O(log n) operations)
- LRU query caching
- Thread-safe operations
- ACID transactions
- CLI interface

## Technical Specifications

### Data Types
- STRING
- INTEGER
- FLOAT
- BOOLEAN
- BLOB

### Constraints
- PRIMARY KEY
- NOT NULL
- UNIQUE

### Performance
- B-tree degree: 4
- Cache size: 1000 entries
- Concurrent read/write

## Installation

```bash
git clone https://github.com/Itzhep/godb
cd godb
go build
```
## Start GoDB
./godb

# Database Operations
create database testdb
use testdb

# Table Operations
create table users id:INTEGER,PK name:STRING,NN email:STRING,UNIQUE
insert users id=1 name=John email=john@example.com
select users name=John