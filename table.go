package main

import (
	"fmt"
	"strconv"

	"github.com/Itzhep/godb/btree"
	"github.com/Itzhep/godb/cache"
)

type ColumnType string

const (
    TypeString  ColumnType = "STRING"
    TypeInteger ColumnType = "INTEGER"
    TypeFloat   ColumnType = "FLOAT"
    TypeBoolean ColumnType = "BOOLEAN"
    TypeBlob    ColumnType = "BLOB"  // Add BLOB type
)

type Column struct {
    Name        string
    Type        ColumnType
    Indexed     bool
    PrimaryKey  bool
    NotNull     bool
    Unique      bool
    DefaultVal  interface{}
}

type Table struct {
    Name        string
    Columns     []Column
    Rows        []map[string]interface{}
    Indexes     map[string]map[interface{}][]int
    PrimaryKey  string
    UniqueKeys  map[string]bool
    BTreeIndexes map[string]*btree.BTree
    QueryCache   *cache.Cache
}

func (t *Table) createIndex(columnName string) {
    if t.Indexes == nil {
        t.Indexes = make(map[string]map[interface{}][]int)
    }
    index := make(map[interface{}][]int)
    for i, row := range t.Rows {
        value := row[columnName]
        index[value] = append(index[value], i)
    }
    t.Indexes[columnName] = index
}

func (t *Table) createBTreeIndex(columnName string) {
    if t.BTreeIndexes == nil {
        t.BTreeIndexes = make(map[string]*btree.BTree)
    }
    
    tree := btree.NewBTree()
    for i, row := range t.Rows {
        if value, exists := row[columnName]; exists {
            tree.Insert(value, i)
        }
    }
    t.BTreeIndexes[columnName] = tree
}

// Insert adds a new row to the table.
func (t *Table) Insert(values map[string]interface{}) error {
    // Apply default values
    for _, col := range t.Columns {
        if _, exists := values[col.Name]; !exists && col.DefaultVal != nil {
            values[col.Name] = col.DefaultVal
        }
    }

    // Validate constraints
    for _, col := range t.Columns {
        val, exists := values[col.Name]
        
        // Check NOT NULL
        if col.NotNull && (!exists || val == nil) {
            return fmt.Errorf("column %s cannot be null", col.Name)
        }
        
        // Check UNIQUE
        if col.Unique {
            if t.isDuplicate(col.Name, val) {
                return fmt.Errorf("duplicate value in unique column %s", col.Name)
            }
        }
        
        // Validate PRIMARY KEY
        if col.PrimaryKey {
            if val == nil {
                return fmt.Errorf("primary key %s cannot be null", col.Name)
            }
            if t.isDuplicate(col.Name, val) {
                return fmt.Errorf("duplicate primary key value in %s", col.Name)
            }
        }
    }

    // Validate types and update indexes
    for _, col := range t.Columns {
        val, exists := values[col.Name]
        if (!exists) {
            return fmt.Errorf("missing value for column: %s", col.Name)
        }
        if !isValidType(val, col.Type) {
            return fmt.Errorf("invalid type for column %s: expected %s", col.Name, col.Type)
        }
    }
    
    rowIndex := len(t.Rows)
    t.Rows = append(t.Rows, values)
    
    // Update indexes
    for colName, index := range t.Indexes {
        value := values[colName]
        index[value] = append(index[value], rowIndex)
    }
    
    return nil
}

// Select retrieves rows that match the given conditions.
func (t *Table) Select(conditions map[string]interface{}) ([]map[string]interface{}, error) {
    // Generate cache key
    cacheKey := t.generateCacheKey(conditions)
    
    // Check cache
    if results, found := t.QueryCache.Get(cacheKey); found {
        return results.([]map[string]interface{}), nil
    }
    
    // Use BTree index if available for first condition
    for colName, value := range conditions {
        if tree, exists := t.BTreeIndexes[colName]; exists {
            rowIndicesInterface := tree.Search(value)
            rowIndices, ok := rowIndicesInterface.([]int)
            if !ok {
                return nil, fmt.Errorf("unexpected type for row indices")
            }
            results := make([]map[string]interface{}, 0)
            for _, idx := range rowIndices {
                row := t.Rows[idx]
                if t.matchesAllConditions(row, conditions) {
                    results = append(results, row)
                }
            }
            // Cache results
            t.QueryCache.Set(cacheKey, results)
            return results, nil
        }
    }
    
    // Fallback to full table scan
    var results []map[string]interface{}
    for _, row := range t.Rows {
        match := true
        for key, value := range conditions {
            if row[key] != value {
                match = false
                break
            }
        }
        if match {
            results = append(results, row)
        }
    }
    return results, nil
}

func (t *Table) matchesAllConditions(row map[string]interface{}, conditions map[string]interface{}) bool {
    for key, value := range conditions {
        if row[key] != value {
            return false
        }
    }
    return true
}

func (t *Table) generateCacheKey(conditions map[string]interface{}) string {
    // Create deterministic cache key from conditions
    var key string
    for k, v := range conditions {
        key += fmt.Sprintf("%s=%v;", k, v)
    }
    return key
}

func isValidColumnType(ct ColumnType) bool {
    switch ct {
    case TypeString, TypeInteger, TypeFloat, TypeBoolean, TypeBlob:
        return true
    default:
        return false
    }
}

func isValidType(value interface{}, expectedType ColumnType) bool {
    switch expectedType {
    case TypeString:
        _, ok := value.(string)
        return ok
    case TypeInteger:
        _, ok := value.(int)
        return ok
    case TypeFloat:
        _, ok := value.(float64)
        return ok
    case TypeBoolean:
        _, ok := value.(bool)
        return ok
    case TypeBlob:
        _, ok := value.([]byte)
        return ok
    default:
        return false
    }
}

func convertStringToType(value string, colType ColumnType) (interface{}, error) {
    switch colType {
    case TypeString:
        return value, nil
    case TypeInteger:
        return strconv.Atoi(value)
    case TypeFloat:
        return strconv.ParseFloat(value, 64)
    case TypeBoolean:
        return strconv.ParseBool(value)
    case TypeBlob:
        return []byte(value), nil // Convert string to byte slice for BLOB
    default:
        return nil, fmt.Errorf("unsupported type: %s", colType)
    }
}

func (t *Table) isDuplicate(colName string, value interface{}) bool {
    for _, row := range t.Rows {
        if row[colName] == value {
            return true
        }
    }
    return false
}
