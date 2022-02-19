package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"syscall"
	"time"

	"github.com/goccy/go-graphviz"
	"github.com/goccy/go-graphviz/cgraph"
	"github.com/jackc/pgx/v4"
	"golang.org/x/term"
)

func main() {
	start := time.Now()
	user := flag.String("user", "stoper_test", "User name")
	pass := flag.String("pass", "", "Password. When empty, can be provided from STDIN")
	host := flag.String("host", "127.0.0.1", "Host name")
	port := flag.Uint("port", 5432, "Port")
	db := flag.String("db", "stoper_test1", "Database name")
	schema := flag.String("schema", "public", "Schema name")
	flag.Parse()

	if len(*pass) == 0 {
		fmt.Print("Password: ")
		bytepw, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			os.Exit(1)
		}
		newpass := string(bytepw)
		pass = &newpass
		fmt.Println()
	}

	if true {
		schemaToGraph(fetchSchema(*user, *pass, *host, uint16(*port), *db, *schema), "postgresql.svg")
		stop := time.Now()
		fmt.Print("Time taken: ", stop.Sub(start), "\n")
	} else {
		// small unrelated DB benchmark...
		const times int = 1000
		fReuse := func() {
			conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://%s:%s@%s:%d/%s", *user, *pass, *host, uint16(*port), *db))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
				os.Exit(1)
			}
			defer conn.Close(context.Background())
			for i := 0; i < times; i++ {
				fetchSchemaConnected(conn, *schema)
			}
		}
		fScratch := func() {
			for i := 0; i < times; i++ {
				fetchSchema(*user, *pass, *host, uint16(*port), *db, *schema)
			}
		}
		measureTime(fmt.Sprintf("Reuse connection %dx: ", times), fReuse)
		measureTime(fmt.Sprintf("Fresh connection %dx: ", times), fScratch)
	}
}

func measureTime(name string, f func()) {
	start := time.Now()
	f()
	stop := time.Now()
	fmt.Print(name, " time taken: ", stop.Sub(start), "\n")
}

type columnData struct {
	name string
	desc string
}

type table struct {
	name    string
	columns []columnData
}

type relation struct {
	name       string
	fromTable  int
	fromColumn int
	toTable    int
	toColumn   int
}

type schema struct {
	tables    []table
	relations []relation
}

// gets schema from postgresql DB
func fetchSchema(user, password string, host string, port uint16, dbName string, schemaName string) schema {
	// todo: validate input
	conn, err := pgx.Connect(context.Background(), fmt.Sprintf("postgres://%s:%s@%s:%d/%s", user, password, host, port, dbName))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	return fetchSchemaConnected(conn, schemaName)
}

var schemaValidRegex, _ = regexp.Compile(`^[\w\d_]+$`) // this is unlikely correct

// gets schema from postgresql DB with existing connection
func fetchSchemaConnected(conn *pgx.Conn, schemaName string) schema {

	if !schemaValidRegex.MatchString(schemaName) {
		panic(fmt.Sprintf("'%s' is not a valid schema name", schemaName))
	}

	mtables := make(map[string]table)
	{
		rows, err := conn.Query(context.Background(), fmt.Sprintf("SELECT table_name,column_name,udt_name FROM information_schema.columns WHERE table_schema = '%s' ORDER BY ordinal_position", schemaName))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}
		for rows.Next() {
			var table string
			var column string
			var udtName string
			err := rows.Scan(&table, &column, &udtName)
			if err != nil {
				panic("failed scanning rows")
			}
			entry := mtables[table]
			entry.name = table
			entry.columns = append(entry.columns, columnData{column, udtName})
			mtables[table] = entry
		}
	}

	var schema schema
	type table_col_index struct {
		table   int
		columns map[string]int
	}
	index_map := make(map[string]table_col_index)
	{
		table_index := 0
		for _, table := range mtables {
			schema.tables = append(schema.tables, table)
			columns := make(map[string]int)
			for i, col := range table.columns {
				columns[col.name] = i
			}
			index_map[table.name] = table_col_index{
				table_index,
				columns,
			}
			table_index++
		}
	}

	{
		// https://stackoverflow.com/a/42248468/950131
		query := `SELECT
    tc.constraint_name, tc.table_name, kcu.column_name, 
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
WHERE constraint_type = 'FOREIGN KEY'`

		rows, err := conn.Query(context.Background(), fmt.Sprintf(query+" AND tc.table_schema = '%s'", schemaName))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Query failed: %v\n", err)
			os.Exit(1)
		}
		for rows.Next() {
			var constraintName string
			var fromTable string
			var fromColumn string
			var toTable string
			var toColumn string
			err := rows.Scan(&constraintName, &fromTable, &fromColumn, &toTable, &toColumn)
			if err != nil {
				panic("failed scanning rows")
			}

			var relation relation
			relation.name = constraintName
			{
				tableIdx, ok := index_map[fromTable]
				if !ok {
					panic("can't find table name: " + fromTable)
				}
				relation.fromTable = tableIdx.table
				{
					columnIdx, ok := tableIdx.columns[fromColumn]
					if !ok {
						panic("can't find table name: " + fromTable)
					}
					relation.fromColumn = columnIdx
				}
			}
			{
				tableIdx, ok := index_map[toTable]
				if !ok {
					panic("can't find table name: " + toTable)
				}
				relation.toTable = tableIdx.table
				{
					columnIdx, ok := tableIdx.columns[toColumn]
					if !ok {
						panic("can't find table name: " + toTable)
					}
					relation.toColumn = columnIdx
				}
			}

			schema.relations = append(schema.relations, relation)
		}
	}

	return schema
}

// convert intermediate data of DB schema to PNG
func schemaToGraph(schema schema, pathSVG string) {
	g := graphviz.New()

	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()

	var nodes []*cgraph.Node
	for _, table := range schema.tables {
		n, err := graph.CreateNode(table.name)
		if err != nil {
			log.Fatal(err)
		}
		n.SetShape(cgraph.NoneShape)
		n.SetMargin(0)
		html := fmt.Sprintf(`<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0"><TR><TD BGCOLOR="gray">%s</TD></TR>`, table.name)
		for i, column := range table.columns {
			html = html + fmt.Sprintf("<TR><TD PORT=\"p%d\">%s <i>%s</i></TD></TR>", i, column.name, column.desc)
		}
		html = html + "</TABLE>"
		n.Set("label", graph.StrdupHTML(html))
		nodes = append(nodes, n)
	}

	for _, rel := range schema.relations {

		e, err := graph.CreateEdge("e", nodes[rel.fromTable], nodes[rel.toTable])
		if err != nil {
			log.Fatal(err)
		}
		e.SetLabel(rel.name)
		e.SetTailPort(fmt.Sprintf("p%d", rel.fromColumn))
		e.SetHeadPort(fmt.Sprintf("p%d", rel.toColumn))
	}

	if err := g.RenderFilename(graph, graphviz.SVG, pathSVG); err != nil {
		log.Fatal(err)
	}
}
