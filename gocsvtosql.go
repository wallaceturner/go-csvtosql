package main

import "fmt"
import "log"
import "io"
import "io/ioutil"
import "os"
import "database/sql"
import "encoding/csv"
import "encoding/json"

import _ "github.com/denisenkom/go-mssqldb"

func main() {
	file, err := os.Open("sample.csv")
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	defer file.Close()
	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Println(len(record))
	}

	var b []byte
	b, err = ioutil.ReadFile("mapping.json")
	if err != nil {
		panic(err)
	}

	var m Mapping

	err = json.Unmarshal(b, &m)
	if err != nil {
		fmt.Println(err)
	} else {
		//fmt.Println(m)
		for _, element := range m.Mappings {
			fmt.Printf("Csv %v   Sql %v\n", element.Csv, element.Sql)
		}
	}

	db, errdb := sql.Open("mssql", m.DbConnectionString)
	if errdb != nil {
		panic(errdb)
	}

	ep := db.Ping()
	if ep != nil {
		panic(ep)
	}

	_, err = db.Exec(
		"INSERT INTO table1 (column1, column2) VALUES (?, ?)",
		"gopher2",
		"foobar2",
	)
	if err != nil {
		log.Fatal(err)
	}

	rows, e1 := db.Query("select column1 from table1")
	if err != nil {
		panic(e1)
	}

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", name)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

}

type Mapping struct {
	SqlTableName       string
	DbConnectionString string
	Mappings           []ColumnMapping
}
type ColumnMapping struct {
	Csv string
	Sql string
}
