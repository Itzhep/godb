package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/Itzhep/godb/btree"
	"github.com/Itzhep/godb/cache"
)

type CLI struct {
    currentDB *Database
    scanner   *bufio.Scanner
}

func NewCLI() *CLI {
    return &CLI{
        scanner: bufio.NewScanner(os.Stdin),
    }
}

func (cli *CLI) Run() {
    fmt.Println("Welcome to GoDB. Type 'help' for commands.")

    for {
        if cli.currentDB != nil {
            fmt.Printf("godb(%s)> ", cli.currentDB.Name)
        } else {
            fmt.Print("godb> ")
        }

        if !cli.scanner.Scan() {
            break
        }

        input := cli.scanner.Text()
        if input == "exit" || input == "quit" {
            break
        }

        cli.executeCommand(input)
    }
}

func (cli *CLI) executeCommand(input string) {
    tokens := strings.Fields(input)
    if len(tokens) == 0 {
        return
    }

    command := tokens[0]
    args := tokens[1:]

    switch command {
    case "help":
        cli.showHelp()
    case "show":
        if len(args) == 1 {
            cli.showTable(args[0])
        }
    case "create":
        if len(args) >= 2 && args[0] == "database" {
            cli.createDatabase(args[1])
        } else if len(args) >= 2 && args[0] == "table" {
            cli.createTable(args[1], args[2:])
        }
    case "use":
        if len(args) == 1 {
            cli.useDatabase(args[0])
        }
    case "insert":
        if len(args) >= 2 {
            cli.insertData(args[0], args[1:])
        }
    case "select":
        if len(args) >= 1 {
            cli.selectData(args[0], args[1:])
        }
    case "tables":
        cli.listTables()
    default:
        fmt.Println("Unknown command. Type 'help' for available commands.")
    }
}

func (cli *CLI) showHelp() {
    fmt.Println("Available commands:")
    fmt.Println("  create database <name>")
    fmt.Println("  use <database>")
    fmt.Println("  create table <name> <column1> <column2> ...")
    fmt.Println("  insert <table> <col1>=<val1> <col2>=<val2> ...")
    fmt.Println("  select <table> [<col>=<val> ...]")
    fmt.Println("  tables - list all tables")
    fmt.Println("  show <table> - show table details")
    fmt.Println("  help - show this help")
    fmt.Println("  exit/quit - exit the program")
}

func (cli *CLI) createDatabase(name string) {
    // Check if database file already exists
    if _, err := os.Stat(name + ".gob"); err == nil {
        fmt.Printf("Error: Database '%s' already exists\n", name)
        return
    }

    // Create new database
    cli.currentDB = NewDatabase(name)
    
    // Save the database to file
    err := cli.currentDB.SaveToFile()
    if err != nil {
        fmt.Printf("Error creating database: %v\n", err)
        cli.currentDB = nil
        return
    }
    
    fmt.Printf("Created database: %s\n", name)
}

func (cli *CLI) useDatabase(name string) {
    db, err := LoadDatabase(name + ".gob")
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    cli.currentDB = db
    fmt.Printf("Using database: %s\n", name)
}

func (cli *CLI) createTable(name string, args []string) {
    columns := make([]Column, 0)
    for _, arg := range args {
        parts := strings.Split(arg, ":")
        if len(parts) < 2 {
            fmt.Printf("Error: Invalid column format: %s\n", arg)
            return
        }

        colName := parts[0]
        specs := strings.Split(parts[1], ",")
        
        col := Column{
            Name:    colName,
            Type:    ColumnType(strings.ToUpper(specs[0])),
            Indexed: false,
        }

        // Parse constraints
        for _, spec := range specs[1:] {
            switch strings.ToUpper(spec) {
            case "PK":
                col.PrimaryKey = true
                col.NotNull = true
            case "NN":
                col.NotNull = true
            case "UNIQUE":
                col.Unique = true
            }
        }

        columns = append(columns, col)
    }

    // Create table with new structure
    table := &Table{
        Name:    name,
        Columns: columns,
        Rows:    make([]map[string]interface{}, 0),
        Indexes: make(map[string]map[interface{}][]int),
        BTreeIndexes: make(map[string]*btree.BTree),
        QueryCache:   cache.NewCache(1000),
    }
    
    cli.currentDB.Tables[name] = *table
    fmt.Printf("Created table: %s\n", name)
}

func (cli *CLI) insertData(tableName string, args []string) {
    if cli.currentDB == nil {
        fmt.Println("Error: No database selected")
        return
    }

    table, err := cli.currentDB.GetTable(tableName)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    data := make(map[string]interface{})
    for _, arg := range args {
        parts := strings.Split(arg, "=")
        if len(parts) != 2 {
            fmt.Printf("Error: Invalid format for %s\n", arg)
            return
        }

        // Find column type
        var col Column
        for _, c := range table.Columns {
            if c.Name == parts[0] {
                col = c
                break
            }
        }

        // Convert string value to proper type
        value, err := convertStringToType(parts[1], col.Type)
        if err != nil {
            fmt.Printf("Error converting value for column %s: %v\n", parts[0], err)
            return
        }
        data[parts[0]] = value
    }

    err = table.Insert(data)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    fmt.Println("Data inserted successfully")
}

func (cli *CLI) selectData(tableName string, conditions []string) {
    if cli.currentDB == nil {
        fmt.Println("Error: No database selected")
        return
    }

    table, err := cli.currentDB.GetTable(tableName)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    condMap := make(map[string]interface{})
    for _, condition := range conditions {
        parts := strings.Split(condition, "=")
        if len(parts) != 2 {
            fmt.Printf("Error: Invalid condition format: %s\n", condition)
            return
        }
        condMap[parts[0]] = parts[1]
    }

    results, err := table.Select(condMap)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    for _, row := range results {
        fmt.Println(row)
    }
}

func (cli *CLI) listTables() {
    if cli.currentDB == nil {
        fmt.Println("Error: No database selected")
        return
    }

    fmt.Println("Tables:")
    for name := range cli.currentDB.Tables {
        fmt.Printf("- %s\n", name)
    }
}

func (cli *CLI) showTable(tableName string) {
    if cli.currentDB == nil {
        fmt.Println("Error: No database selected")
        return
    }

    table, err := cli.currentDB.GetTable(tableName)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Printf("\nTable: %s\n", table.Name)
    fmt.Println("Columns:")
    for _, col := range table.Columns {
        fmt.Printf("  - %s\n", col)
    }
    fmt.Printf("Row count: %d\n", len(table.Rows))
    
    // Show sample data if available
    if len(table.Rows) > 0 {
        fmt.Println("\nSample row:")
        fmt.Printf("%v\n", table.Rows[0])
    }
}

var queryCache *cache.Cache

func init() {
    // Initialize query cache with 1000 entries capacity
    queryCache = cache.NewCache(1000)
}