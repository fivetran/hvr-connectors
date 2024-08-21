//===========================================================================
//
// hvrspanneragent.go
//
// This program can be configured as an HVR AgentPlugin to send table rows
// into Google Spanner tables.
//
// OPTIONS
//     -s      Implement Softkey logic for deletes
//     -n      Do not truncate Spanner tables on refresh
//
// ENVIRONMENT VARIABLES
//     HVR_SPANNER_PROJECTID       (required)
//        Google cloud project id
//
//     HVR_SPANNER_INSTANCEID      (required)
//        Google cloud instance id
//
//     HVR_SPANNER_DATABASE        (required)
//        Google Spanner database name
//
//     HVR_SPANNER_REFRESH_THREADS (optional, default 10)
//	  Number of threads to be used for processing files during refresh
//
//     HVR_SPANNER_TRACE           (optional)
//        Enables tracing of the hvrspanneragent.
//           0 - No tracing
//           1 - Minimal logging
//           2 - File-level logging
//           3 - Row-level logging
//
// CHANGE_LOG
//     	2022-01-13 RLR: Is basically working.  Needs improvement.
//     	2024-02-01 CA:  Tied logging of Integrate map to trace_level, added
//						handling of null columns.
//     	2024-02-22 CA:  Added checking of datatypes from Spanner table
//                      to allow insert of float types
//		2024-03-07 CA:	Added multi-threading goroutine for refresh only.
//						Uses HVR_SPANNER_REFRESH_THREADS with default of 10 threads.
//		2024-07-15 CA: 	Added check for string column to prevent conversion of numeric strings
//						to integers
//		2024-08-13 CA:  Added -n flag to indicate Spanner tables will not be truncated on refresh
//		2024-08-20 CA:  Made non-truncating refresh use InsertOrUpdateMap just like integrate
//		2024-08-21 CA:  Refresh and Integrate both use threading
//
//===========================================================================

package main

import (
	"bufio"
	"context"
	csv "encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	spanner "cloud.google.com/go/spanner"
)

type Options struct {
	trace_level    int
	max_threads    int
	mode           string
	file_loc       string
	files          []string
	tables         []string
	target_tables  []string
	keys           []string
	columns        []string
	target_columns []string
	project        string
	instance       string
	database       string
	full_db_id     string
	soft_delete    bool
	no_truncate    bool
}

var options Options

func fail_out(msg string) {
	log.Println("F_JX0D03: " + msg)
	os.Exit(1)
}

func log_message(level int, msg string) {
	if level <= options.trace_level {
		log.Println(msg)
	}
}

func log_environment() {
	if options.trace_level < 3 {
		return
	}
	log.Println("Runtime arguments", os.Args)
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "HVR") || strings.HasPrefix(e, "GOOGLE") {
			log.Println(e)
		}
	}
}

func get_required_ev(evname string) string {
	ev := os.Getenv(evname)
	if ev == "" {
		fail_out("Required Environment Variable " + evname + " is missing; cannot continue")
	}
	return ev
}

func get_integer_ev(evname string, default_value int) int {
	ev := os.Getenv(evname)
	if ev != "" {
		i, err := strconv.ParseInt(ev, 10, 32)
		if err != nil {
			fail_out("Value of " + evname + " is " + ev + "; must be an integer")
		}
		return int(i)
	}
	return default_value
}

func get_list_ev(evname string, delimiter string) []string {
	ev := os.Getenv(evname)
	if ev == "" {
		return []string{}
	}
	return strings.Split(ev, delimiter)
}

func parse_args() {
	if len(os.Args) < 4 {
		fail_out(os.Args[0] + " called with less than 4 arguments; cannot continue")
	}
	options.mode = os.Args[1]
	options.soft_delete = false
	options.no_truncate = false
	if len(os.Args) > 4 {
		user_args := strings.Split(os.Args[4], " ")
		for _, arg := range user_args {
			log_message(1, "your args "+arg)
			if arg == "-s" {
				log_message(1, "you got SD")
				options.soft_delete = true
			}
			if arg == "-n" {
				log_message(1, "you got no trunc")
				options.no_truncate = true
			}
		}
	}
}

func env_load() {
	log_message(1, "Loading Environment Variables...")
	options.trace_level = get_integer_ev("HVR_SPANNER_TRACE", 5)
	options.max_threads = get_integer_ev("HVR_SPANNER_THREADS", 10)
	options.file_loc = os.Getenv("HVR_FILE_LOC")
	options.files = get_list_ev("HVR_FILE_NAMES", ":")
	options.tables = get_list_ev("HVR_TBL_NAMES", ":")
	options.target_tables = get_list_ev("HVR_BASE_NAMES", ":")
	options.keys = get_list_ev("HVR_TBL_KEYS", ":")
	options.columns = get_list_ev("HVR_COL_NAMES", ":")
	options.target_columns = get_list_ev("HVR_COL_NAMES_BASE", ":")
	options.project = get_required_ev("HVR_SPANNER_PROJECTID")
	options.instance = get_required_ev("HVR_SPANNER_INSTANCEID")
	options.database = get_required_ev("HVR_SPANNER_DATABASE")
}

func valid_tablename(parsed string) bool {
	for _, tab := range options.tables {
		if tab == parsed {
			return true
		}
	}
	return false
}

func build_table_map() map[string][]string {
	table_map := make(map[string][]string)

	file_count := len(options.files)
	if file_count > 300 {
		log_message(1, "Found "+fmt.Sprint(file_count)+" files. ")
	}

	for _, file := range options.files {
		if !strings.HasSuffix(file, ".csv") {
			fail_out("Expecting CSV files; got " + file + "; cannot continue")
		}
		dash := strings.LastIndex(file, "-")
		if dash < 0 {
			fail_out("Expecting default filename format {integ_tstamp}-{tbl_name}.csv; got " + file)
		}
		table_name := file[dash+1 : len(file)-4]
		if !valid_tablename(table_name) {
			log.Println("Table name " + table_name + " parsed from " + file + " is not valid")
			log.Println("Expecting default filename format {integ_tstamp}-{tbl_name}.csv")
			fail_out("Invalid table name or invalid filename format; cannot continue")
		}
		table_map[table_name] = append(table_map[table_name], file)
	}
	return table_map
}

func readHeader(file_name string) (line string, lastLine int, err error) {

	file, _ := os.Open(file_name)
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		lastLine++
		if lastLine == 1 {
			return sc.Text(), lastLine, sc.Err()
		}
	}

	return line, lastLine, io.EOF
}

func loadCsv(file_full_name string, number_of_columns int) ([][]string, int) {

	var lineCount int

	file, _ := os.Open("" + file_full_name + "")
	fileScanner := bufio.NewScanner(file)
	lineCount = 0
	for fileScanner.Scan() {
		lineCount++
	}

	f, _ := os.Open(file_full_name)

	row := []string{}

	// Create a new reader.
	counter := 0
	r := csv.NewReader(f)
	for {
		record, err := r.Read()
		// Stop at EOF.
		if err == io.EOF {
			break
		}
		if err != nil {
			fail_out(fmt.Sprintf("Error reading %s: %v", file_full_name, err))
		}

		if counter >= 1 {
			for _, v := range record {
				row = append(row, v)
			}
		}
		counter++
	}

	//all values from the CSV are loaded into one slice, row.
	rows_length := len(row) / number_of_columns

	rows := make([][]string, rows_length)

	k := 0
	for i := 0; i < rows_length; i++ {

		rows[i] = make([]string, number_of_columns)

		for j := 0; j < number_of_columns; j++ {
			rows[i][j] = row[k]
			k++
		}
	}

	return rows, lineCount
}

func truncateTarget(tbl_name string) {

	ctx := context.Background()

	client, err := spanner.NewClient(ctx, options.full_db_id)
	if err != nil {
		fail_out(fmt.Sprintf("Error creating Spanner client: %v", err))
	}
	log_message(2, "Spanner client created successfully")

	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {

		m2 := spanner.Delete(""+tbl_name+"", spanner.AllKeys())

		return txn.BufferWrite([]*spanner.Mutation{m2})
	})
	client.Close()
	if err != nil {
		fail_out(fmt.Sprintf("Error truncating %s: %v", tbl_name, err))
	}

	log_message(1, "All rows deleted from target table "+tbl_name+".")
}

func integRows(lineCount int, tbl_name string, col_names_slice []string, rows [][]string, file_full_name string, dtypes map[string]string, threadnum int) (rowCount int) {

	var colMap = make(map[string]string)

	ctx := context.Background()

	client, err := spanner.NewClient(ctx, options.full_db_id)
	if err != nil {
		log.Fatal("Thread "+fmt.Sprint(threadnum)+": An error occured while creating Spanner client", "Error:", err)
	}
	log_message(2, "Thread "+fmt.Sprint(threadnum)+": Spanner client created successfully.")

	if options.mode == "integ_end" {
		if options.soft_delete {
			log_message(3, "Using Softdelete replication.")
		} else {
			log_message(3, "Using Timekey replication.")
		}
	}

	for i := range col_names_slice {
		colMap[col_names_slice[i]] = col_names_slice[i]
	}
	rowCount = 0

	log_message(2, fmt.Sprint("Thread "+fmt.Sprint(threadnum)+": Integrate ", lineCount, " rows from ", file_full_name, "  starting"))
	for i := 0; i < len(rows); i++ {
		var insertMap = make(map[string]string)
		recordCount := 0
		rowCount++
		for c := 0; c < len(col_names_slice); c++ {
			if len(rows[i][c]) > 0 {
				insertMap[col_names_slice[c]] = rows[i][c]
				if rows[i][c] != "" {
					recordCount++
				}
			}
		}
		log_message(3, fmt.Sprint("Thread "+fmt.Sprint(threadnum)+":  Integrate InsertMap:", insertMap))
		log_message(3, fmt.Sprint("Thread "+fmt.Sprint(threadnum)+": dtypes: ", dtypes))
		insertItf := make(map[string]interface{}, len(insertMap))
		for k, v := range insertMap {
			if dtypes[strings.ToLower(k)] == "FLOAT64" {
				log_message(3, fmt.Sprint(k, ": value ", v, ": is a float"))
				f, _ := strconv.ParseFloat(v, 64)
				insertItf[k] = f
			} else if strings.HasPrefix(dtypes[strings.ToLower(k)], "STRING") {
				log_message(3, fmt.Sprint(k, ": value ", v, ": is a string"))
				insertItf[k] = v
			} else {
				i, int_err := strconv.Atoi(v)
				if int_err != nil {
					log_message(3, fmt.Sprint(k, ": value ", v, ": is a string"))
					insertItf[k] = v
				} else {
					log_message(3, fmt.Sprint(k, ": value ", i, " is an int"))
					insertItf[k] = i
				}
			}
		}

		if (options.mode == "integ_end" && options.soft_delete) || (options.mode == "refr_write_end" && options.no_truncate) {
			log_message(3, fmt.Sprintf("Thread "+fmt.Sprint(threadnum)+": Insert row interface %v", insertItf))
			_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				m2 := spanner.InsertOrUpdateMap(""+tbl_name+"", insertItf)

				return txn.BufferWrite([]*spanner.Mutation{m2})
			})
			if err != nil {
				client.Close()
				fail_out(fmt.Sprintf("Thread "+fmt.Sprint(threadnum)+": InsertOrUpdate failed: %v", err))
			}
		} else {
			_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				m2 := spanner.InsertMap(""+tbl_name+"", insertItf)

				return txn.BufferWrite([]*spanner.Mutation{m2})
			})
			if err != nil {
				client.Close()
				fail_out(fmt.Sprintf("Thread "+fmt.Sprint(threadnum)+": Insert failed: %v", err))
			}
		}
	}

	client.Close()
	file_loc_cleanup(file_full_name)
	log_message(2, "Thread "+fmt.Sprint(threadnum)+": Spanner client closed for "+file_full_name)
	log_message(2, fmt.Sprint("Thread "+fmt.Sprint(threadnum)+": Integrate ", lineCount, " rows from ", file_full_name, "  finished"))

	return rowCount
}

func file_loc_cleanup(file_full_name string) {
	err := os.Remove("" + file_full_name + "")
	if err != nil {
		fmt.Printf("Error removing staging file %s: %v", file_full_name, err)
	}
}

func process_table(table string, tabindex int, table_map map[string][]string) {

	var integ_rows_total int
	var lineCount_total int

	table_file_list := table_map[table]
	file_count := len(table_file_list)
	target_table := options.target_tables[tabindex]
	max_threads := options.max_threads
	actual_threads := min(max_threads, file_count)

	log_message(1, fmt.Sprint("**** Processing table ", table, " ****"))
	log_message(2, fmt.Sprint("   target table name ", target_table))
	log_message(2, fmt.Sprint("   columns ", options.columns[tabindex]))
	log_message(2, fmt.Sprint("   target_columns ", options.target_columns[tabindex]))
	log_message(2, fmt.Sprint("   keys ", options.keys[tabindex]))
	log_message(1, fmt.Sprint("Found "+fmt.Sprint(file_count)+" files for table "+target_table+"."))

	options.full_db_id = "projects/" + options.project + "/instances/" + options.instance + "/databases/" + options.database + ""
	log_message(1, fmt.Sprint("Writing to: ", options.full_db_id, "/", target_table, "."))
	datatypes_map := get_table_datatypes(target_table, options.full_db_id)

	integ_rows_total = 0

	// Determine whether truncation is needed and execute
	if options.mode == "refr_write_end" {
		if options.no_truncate {
			log_message(1, "Flag was set for no truncation, "+target_table+" not being truncated")
		} else {
			log_message(1, "Truncating "+target_table)
			truncateTarget(target_table)
		}
	}

	log_message(1, "Making "+fmt.Sprint(actual_threads)+" threads for table "+target_table)
	var ch = make(chan int, file_count)
	var wg sync.WaitGroup
	wg.Add(actual_threads)

	for i := 0; i < actual_threads; i++ {
		i := i
		go func() {
			for {
				a, ok := <-ch
				if !ok { // if there is nothing to do and the channel has been closed then end the goroutine
					wg.Done()
					return
				}
				var integ_rows int
				filename := table_file_list[a]
				integ_rows, lineCount := process_file(filename, target_table, datatypes_map, i) // process the file
				// log the processing with human-friendly thread and file numbers
				log_message(1, "Thread "+fmt.Sprint(i)+": processed file "+fmt.Sprint(a+1)+", "+filename+" for "+target_table+" table")

				integ_rows_total += integ_rows
				lineCount_total += lineCount
			}

		}()
	}

	// make a queue for the files
	for i := 0; i < file_count; i++ {
		ch <- i // add i to the queue
	}

	close(ch) // This tells the goroutines there's nothing else to do
	wg.Wait()

	log_message(1, "Finished "+fmt.Sprint(actual_threads)+" threads, "+fmt.Sprint(file_count)+" files for table "+target_table)
	if options.mode == "refr_write_end" {
		log.Println("Refreshed " + strconv.Itoa(integ_rows_total) + " rows to target table " + target_table + ".")
	} else {
		log.Println("Integrated " + strconv.Itoa(integ_rows_total) + " rows to target table " + target_table + ".")
	}
}

func process_file(fname string, tbl string, dtypes map[string]string, threadnum int) (int, int) {
	var int_rows int
	var lineCount int

	file_full_name := options.file_loc + "/" + fname
	log_message(2, "Thread "+fmt.Sprint(threadnum)+": Staging file full name "+file_full_name)
	if _, err := os.Stat(file_full_name); err == nil {
		headerLine, _, err := readHeader(file_full_name)
		log_message(2, "Thread "+fmt.Sprint(threadnum)+": Staging file header "+headerLine)
		if err != nil {
			fail_out(fmt.Sprintf("Thread "+fmt.Sprint(threadnum)+": Error reading header: %v", err))
		}
		col_names_slice := strings.Split(headerLine, ",")
		log_message(2, fmt.Sprintf("Thread "+fmt.Sprint(threadnum)+": Table columns based on header %s", col_names_slice))
		rows, lineCount := loadCsv(file_full_name, len(col_names_slice))
		log_message(2, "Thread "+fmt.Sprint(threadnum)+": Staging file loaded successfully.")
		int_rows = integRows(lineCount, tbl, col_names_slice, rows, file_full_name, dtypes, threadnum)

	}

	return int_rows, lineCount
}

func get_table_datatypes(table_name string, db_id string) map[string]string {

	datatypes_map := make(map[string]string)
	ctx := context.Background()
	col_count := 0
	log_message(1, fmt.Sprint("Getting column defintions from Spanner for ", table_name, "."))

	client, err := spanner.NewClient(ctx, db_id)
	if err != nil {
		fail_out(fmt.Sprintf("Error creating Spanner client: %v", err))
	}

	stmt := spanner.NewStatement("SELECT column_name, spanner_type FROM INFORMATION_SCHEMA.COLUMNS where lower(table_name) = lower(@tbl)")
	stmt.Params["tbl"] = table_name
	iter := client.Single().Query(ctx, stmt)

	err = iter.Do(func(r *spanner.Row) error {
		var column_name string
		var spanner_type string
		if err := r.Columns(&column_name, &spanner_type); err != nil {
			return err
		}
		datatypes_map[strings.ToLower(column_name)] = spanner_type
		log_message(3, fmt.Sprint(column_name, " ", spanner_type))
		col_count += 1
		return nil
	})
	if err != nil {
		log.Panic()
	}

	if col_count == 0 {
		log_message(1, fmt.Sprint(col_count, " column defintions found in Spanner for ", table_name, ", check case."))
	}

	//log_message(1, fmt.Sprint(datatypes_map, "\n"))
	return datatypes_map
}

func process() {

	log.Println("Building TableMap (map of tables to files).")
	table_map := build_table_map()
	log.Println("TableMap: ", table_map)
	for tabindex, tablename := range options.tables {
		process_table(tablename, tabindex, table_map)
	}
}

func main() {
	log.SetFlags(0)

	parse_args()
	env_load()
	log_environment()

	if options.mode == "refr_write_end" ||
		(options.mode == "integ_end" && len(options.files) > 0) {
		process()
	}
}
