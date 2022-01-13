//===========================================================================
//
// hvrspanneragent.go
//
// This program can be configured as an HVR AgentPlugin to send table rows
// into Google Spanner tables.
//
// OPTIONS
//     -s      Implement Softkey logic for deletes
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
//     HVR_SPANNER_TRACE           (optional)
//        Enables tracing of the hvrspanneragent.
//           0 - No tracing
//           1 - logs each step being performed
//
// CHANGE_LOG
//     01/13/2022 RLR:  Is basically working.  Needs improvement.
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

    spanner "cloud.google.com/go/spanner"
)

type Options struct {
    trace_level     int
    mode            string
    file_loc        string
    files           []string
    tables          []string
    target_tables   []string
    keys            []string
    columns         []string
    target_columns  []string
    project         string
    instance        string
    database        string
    full_db_id      string
    soft_delete     bool
}

var options Options

func fail_out(msg string) {
    log.Println("F_JX0D03: "+msg)
    os.Exit(1)
}

func log_message(level int, msg string) {
    if level <= options.trace_level {
        log.Println(msg)
    }
}

func log_environment(){
    if options.trace_level < 3 {
        return
    }
    log.Println("Runtime arguments", os.Args)
    for _,e := range os.Environ() {
        if strings.HasPrefix(e, "HVR") || strings.HasPrefix(e, "GOOGLE") {
            log.Println(e)
        }
    }
}

func get_required_ev(evname string) (string){
    ev := os.Getenv(evname)
    if (ev == "") {
        fail_out("Required Environment action "+evname+" is missing; cannot continue")
    }
    return ev
}

func get_integer_ev(evname string, default_value int) (int){
    ev := os.Getenv(evname)
    if ev != ""{
        i, err := strconv.ParseInt(ev, 10, 32)
        if err != nil {
            fail_out("Value of "+evname+" is "+ev+"; must be an integer")
        }
        return int(i)
    }
    return default_value 
}

func get_list_ev(evname string, delimiter string) ([]string){
    ev := os.Getenv(evname)
    if (ev == "") {
        return []string{}
    }
    return strings.Split(ev, delimiter)
}

func parse_args() {
    if len(os.Args) < 4 {
        fail_out(os.Args[0]+" called with less than 4 arguments; cannot continue")
    }
    options.mode = os.Args[1]
    options.soft_delete = false
    if len(os.Args) > 4 {
        user_args := strings.Split(os.Args[4], " ")
        for _, arg := range user_args{
            if arg == "-s" {
                options.soft_delete = true
            }
        }
    }
}

func env_load() {
    options.trace_level = get_integer_ev("HVR_SPANNER_TRACE", 0)
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

func valid_tablename(parsed string) bool{
    for _,tab := range options.tables {
        if tab == parsed {
            return true
        }
    }
    return false
}

func build_table_map() (map[string][]string){
    table_map := make(map[string][]string)

    for _, file := range options.files{
        if !strings.HasSuffix(file, ".csv") {
            fail_out("Expecting CSV files; got "+file+"; cannot continue")
        }
        dash := strings.LastIndex(file, "-")
        if dash < 0 {
            fail_out("Expecting default filename format {integ_tstamp}-{tbl_name}.csv; got "+file)
        }
        table_name := file[dash+1:len(file)-4]
        if !valid_tablename(table_name) {
            log.Println("Table name "+table_name+" parsed from "+file+" is not valid")
            log.Println("Expecting default filename format {integ_tstamp}-{tbl_name}.csv")
            fail_out("Invalid table name or invalid filename format; cannot continue")
        }
        table_map[table_name] = append(table_map[table_name], file)
    }
    return table_map
}

func readHeader(file_name string) (line string, lastLine int, err error){

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

func loadCsv(file_full_name string, number_of_columns int) ([][]string, int){

    var lineCount int

    file, _ := os.Open(""+file_full_name+"")
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
        
        if counter >= 1{
            for _, v := range record{
                row = append(row, v)                
            }
        }
        counter++
    }

    //all values from the CSV are loaded into one slice, row.
    rows_length := len(row)/number_of_columns

    rows := make([][]string, rows_length)

    k := 0
    for i := 0; i < rows_length; i++ {

        rows[i] = make([]string, number_of_columns)
        
        for j := 0; j < number_of_columns; j++{
            rows[i][j] = row[k]
            k++
        }
    }
    
    return rows, lineCount
}

func truncateTarget(tbl_name string){

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

func integRows(lineCount int, tbl_name string, col_names_slice []string, rows [][]string, file_full_name string) (rowCount int){
    var colMap = make(map[string]string)

    ctx := context.Background()

    client, err := spanner.NewClient(ctx, options.full_db_id)
    if err != nil {
        log.Fatal("An error occured while creating Spanner client", "Error:", err)
    }
    log_message(2,"Spanner client created successfully.")

    if options.mode == "integ_end" {
        if options.soft_delete == true {
            log_message(1, "Softdelete")
        } else {
            log_message(1, "Timekey")
        }
    }

    for i := range col_names_slice {
        colMap[col_names_slice[i]] = col_names_slice[i]
    }
    rowCount = 0

    log.Println("*** Integrate ", lineCount, " rows")
    for i := 0; i < len(rows); i++ {
        var insertMap = make(map[string]string)
        recordCount := 0
        rowCount++
        for c := 0; c < len(col_names_slice); c++ {
            insertMap[col_names_slice[c]] = rows[i][c]
            if (rows[i][c] != "") {
                recordCount++
            }
        }
        log.Println("*** Integrate ", insertMap)
        insertItf := make(map[string]interface{}, len(insertMap))
        for k, v := range insertMap {
            insertItf[k] = v
        }

        if options.mode == "integ_end" && options.soft_delete == true {
            log_message(1, fmt.Sprintf("Insert row interface %v", insertItf))
            _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
                m2 := spanner.InsertOrUpdateMap(""+tbl_name+"", insertItf)

                return txn.BufferWrite([]*spanner.Mutation{m2})
            })
            if err != nil {
                client.Close()
                fail_out(fmt.Sprintf("InsertOrUpdate failed: %v", err))
            }
        } else {
            _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
                m2 := spanner.InsertMap(""+tbl_name+"", insertItf)

                return txn.BufferWrite([]*spanner.Mutation{m2})
            })
            if err != nil {
                client.Close()
                fail_out(fmt.Sprintf("Insert failed: %v", err))
            }
        }
    }

    client.Close()
    file_loc_cleanup(file_full_name)
    log_message(2, "Spanner client closed.")
    
    return rowCount
}

func file_loc_cleanup(file_full_name string){
    err := os.Remove(""+file_full_name+"")
    if err != nil {
        log.Println(fmt.Sprintf("Error removing staging file %s: %v", file_full_name, err))
    }
}

func process_table(table string, tabindex int, table_map map[string][]string) { 
    var integ_rows int
    var integ_rows_total int
    var lineCount_total int

    log.Println("Process ", table)
    log.Println("   target table name", options.target_tables[tabindex])
    log.Println("   columns", options.columns[tabindex])
    log.Println("   target_columns", options.target_columns[tabindex])
    log.Println("   keys", options.keys[tabindex])
    log.Println("   files", table_map[table])

    options.full_db_id = "projects/"+options.project+"/instances/"+options.instance+"/databases/"+options.database+""
    target_table := options.target_tables[tabindex]

    if options.mode == "refr_write_end" {
        truncateTarget(target_table)
    }
    integ_rows_total = 0
    for _, filename := range table_map[table] {
        file_full_name := options.file_loc+"/"+filename
        log_message(2, "Staging file full name"+file_full_name)
        if _, err := os.Stat(file_full_name); err == nil {
            headerLine, _, err := readHeader(file_full_name)
            log_message(1, "Staging file header "+headerLine)
            if err != nil{
                fail_out(fmt.Sprintf("Error reading header: %v", err))
            }
            col_names_slice := strings.Split(headerLine, ",")
            log_message(2, fmt.Sprintf("Table columns based on header %s", col_names_slice))
            rows, lineCount := loadCsv(file_full_name, len(col_names_slice))
            log_message(2, "Staging file loaded successfully.")
            integ_rows = integRows(lineCount, target_table, col_names_slice, rows, file_full_name)
            lineCount_total += lineCount
            integ_rows_total += integ_rows
        }
    }
    if options.mode == "refr_write_end" {
        log.Println("Refreshed "+strconv.Itoa(integ_rows_total)+" rows to target table "+target_table+".")
    } else {
        log.Println("Integrated "+strconv.Itoa(integ_rows_total)+" rows to target table "+target_table+".")
    }
}

func process() {
    table_map := build_table_map()
    log.Println("TableMap: ", table_map)
    for tabindex, tablename := range options.tables {
        process_table(tablename, tabindex, table_map)
    }
}

func main(){
    log.SetFlags(0)

    parse_args()
    env_load()
    log_environment()

    if options.mode == "refr_write_end" || 
        (options.mode == "integ_end" && len(options.files) > 0) {

        process()
    }
}
