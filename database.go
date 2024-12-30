package main

import (
	"encoding/gob"
	"fmt"
	"os"
)

type Database struct {
	Name   string          `json:"name"`
	Tables map[string]Table `json:"tables"`
}

// NewDatabase creates a new database.
func NewDatabase(name string) *Database {
	return &Database{
		Name:   name,
		Tables: make(map[string]Table),
	}
}

// SaveToFile saves the database to a binary file using gob encoding
func (db *Database) SaveToFile() error {
	file, err := os.Create(db.Name + ".gob")
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	return encoder.Encode(db)
}

// LoadDatabase loads a database from a binary file using gob encoding
func LoadDatabase(filename string) (*Database, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var db Database
	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&db)
	return &db, err
}

// CreateTable creates a new table in the database.
func (db *Database) CreateTable(name string, columns []Column) error {
    if _, exists := db.Tables[name]; exists {
        return fmt.Errorf("table %s already exists", name)
    }
    
    db.Tables[name] = Table{
        Name:    name,
        Columns: columns,
        Rows:    make([]map[string]interface{}, 0),
        Indexes: make(map[string]map[interface{}][]int),
    }
    return nil
}

// GetTable retrieves a table by name.
func (db *Database) GetTable(name string) (*Table, error) {
	table, exists := db.Tables[name]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	return &table, nil
}
