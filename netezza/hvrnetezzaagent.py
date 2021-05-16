#!python
# Copyright (c) 2000-2021 HVR Software bv

################################################################################
# 
# NAME
#     hvrdefaultagent.py
#
# SYNOPSIS
#     as agent
#     python hvrdefaultagent.py mode loc chn
#   
# DESCRIPTION
#     < description >
#
# OPTIONS
#
# AGENT OPERATION
#
#     MODES
#
#     refr_write_begin
#        Is ignored (agent currently cannot create the target tables)
#
#     refr_write_end
#        Convert each row in data files into SQL insert statements 
#
#     integ_end
#        Apply the CSV data files as SQL insert/update/delete statements as 
#        if it were a burst table.
#
# OPTIONS
#     -d <name> - name of the SoftDelete column, default is 'is_deleted'
#     -D <name> - name of the SoftDelete column, default is 'is_deleted'
#                 if specified with "-D", the target table has this column
#     -o <name> - name of the hvr_op column, default is 'op_type'
#     -O <name> - name of the hvr_op column, default is 'op_type'
#                 if specified with "-O", the target table has this column
#     -p - preserve target data during timekey refresh
#     -t - target is timekey
#     -v - check column data before upload
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_FILE_LOC           (required)
#        This varibale sets a location of files to be sent
#  
#     HVR_TBL_NAMES          (required)       
#        This  variable contains list of tables
#  
#     HVR_BASE_NAMES         (required)       
#        This  variable contains list of tables
#  
#     HVR_TBL_KEYS           (required)       
#        This  variable contains list of key columns
#  
#     HVR_COL_NAMES          (required)       
#        This  variable contains list of columns
#  
#     HVR_FILE_NAMES         (required)
#        This  variable contains list of files transfered into HDFS.
#        If empty - intergarion process is  omitted
#
#     HVR_NZ_DSN             (required)
#        The DSN to use when connecting to the Netezza database
#
#     HVR_NZ_TRACE           (advanced,optional, default:0)
#
# CHANGE_LOG
#     02/09/2021 RLR: Initial release
#     02/10/2021 RLR: Fixed ODBC error message reporting
#     02/17/2021 RLR: Load works if target columns are in a different order than the source columns
#     03/02/2021 RLR: Added column checks as follows:
#                        Fail if 'op_type' is in column list & refresh
#                        Fail if number of source and target columns does not match
#                        Fail if the source and target column names does not match
#     03/03/2021 RLR: Reimplemented support for different order columns broken by last change
#     03/12/2021 RLR: Added support for a non-comma separated CSV file
#                     Require /HeaderLine in FileFormat /Csv and use it to get correct column list
#     03/18/2021 RLR: Fixed the configuration for replicate copies - the two required actions are:
#                         ColumnProperties /Name=op_type /Extra /IntegrateExpression={hvr_op} /Datatype=integer
#                         ColumnProperties /Name=is_deleted /Extra /SoftDelete /Datatype=integer
#                     Support for Replicate copy, TimeKey, SoftDelete
#     04/12/2021 RLR: Added option to check the column data against the target columns
#     05/14/2021 RLR: Fixed the trace line in the column data check
#     05/16/2021 RLR: Skip the header line in the column data check
#
################################################################################
import sys
import traceback
import getopt
import os
import re
import time
import pyodbc
from timeit import default_timer as timer

class Options:
    mode = ''
    channel = ''
    location = ''
    agent_env = {}
    trace = 0
    odbc = None
    cursor = None
    dsn = None
    connect_string = None
    delimiter = ','
    optype = 'op_type'
    optype_burst_only = True
    isdeleted = 'is_deleted'
    isdeleted_burst_only = True
    target_is_timekey = False
    check_column_data = False
    truncate_target_on_refresh = True

file_counter = 0

options = Options()

##### Support function #########################################################

def trace(level, msg):
    if options.trace >= level:
        print(msg)
        sys.stdout.flush() 

def version_normalizer(version):
    version_array = version.split('.')
    version_num = 0
    least_significant = len(version_array) - 1
    if least_significant > 2:
        least_significant = 2
    shift = 1
    for i in range(least_significant, -1, -1):
        version_num += int(version_array[i]) * shift
        shift *= 1000
    return version_num
    
def version_check(progname):
    global python3

    python3 = sys.version_info[0] == 3
    if not python3:
        raise Exception("Python version is {0}.{1}.{2}; {3} requires Python 3".format(sys.version_info[0], sys.version_info[1], sys.version_info[2], progname))
    
def load_agent_env():
    agent_env= {}

    if 'HVR_LONG_ENVIRONMENT' in os.environ:
        hvr_long_environment= os.environ['HVR_LONG_ENVIRONMENT']
        try:
            with open(hvr_long_environment, "r") as f:
                long_env= json.loads(f.read())

            for k,v in long_env.items():
                agent_env[str(k)]= str(v)

        except Exception as e:
            sys.stderr.write( ("W_JX0E00: Warning An error occured while "
                               "processing $HVR_LONG_ENVIRONMENT file "
                               "'{}'. Will continue without processing this "
                               "file. Error: {} {}").format(
                                   hvr_long_environment,
                                   str(e),
                                   traceback.format_exc()) )

    for k,v in os.environ.items():
        k= str(k)
        if k not in agent_env:
            agent_env[k]= str(v)

    return agent_env

def env_load():
    options.trace = int(os.getenv('HVR_NZ_TRACE', options.trace))
    options.delimiter = os.getenv('HVR_NZ_DELIMITER', ',')
    if len(options.delimiter) != 1:
        raise Exception("Invalid value {0} for {1}; must be one character".format(options.delimiter, 'HVR_NZ_DELIMITER'))
    options.dsn = os.getenv('HVR_NZ_DSN', '')
    options.agent_env = load_agent_env()
   
def trace_input():
    """
    """
    trace(3, "============================================")
    trace(3, "Optype column is {}; column exists on target = {}".format(options.optype, (not options.optype_burst_only)))
    trace(3, "Isdeleted column is {}; column exists on target = {}".format(options.isdeleted, (not options.isdeleted_burst_only)))
    trace(3, "TimeKey is {0}; preserve data during refresh is {1}".format(options.target_is_timekey, not options.truncate_target_on_refresh))
    trace(3, "Check column data against target types {}".format(options.check_column_data))
    trace(3, "============================================")
    env = os.environ
    if python3:
        for key, value  in env.items():
            if key.find('HVR') != -1:
                trace(3, key + " = " + value)
    else:
        for key, value  in env.iteritems():
            if key.find('HVR') != -1:
                trace(3, key + " = " + value)
    trace(3, "============================================")

def process_args(argv):
    options.mode= argv[1]
    options.channel= argv[2]
    options.location= argv[3]

    cmdargs = argv[4]
    if len(cmdargs):
        try:
            list_args = cmdargs.split(" ");
            opts, args = getopt.getopt(list_args,"d:D:o:O:ptv")
        except getopt.GetoptError:
            raise Exception("Error parsing command line arguments '" + cmdargs + "' due to invalid argument or invalid syntax")

        for opt, arg in opts:
            if opt == '-d':
                options.isdeleted = arg
            elif opt == '-D':
                options.isdeleted = arg
                options.isdeleted_burst_only = False
            elif opt == '-o':
                options.optype = arg
            elif opt == '-O':
                options.optype = arg
                options.optype_burst_only = False
            elif opt == '-p':
                options.truncate_target_on_refresh = False
            elif opt == '-t':
                options.target_is_timekey = True
            elif opt == '-v':
                options.check_column_data = True
  
    if int(os.getenv('HVR_NZ_TRACE', options.trace)) > 2:
        print("agent plugin called with {0} {1} {2} {3}".format(options.mode, options.channel, options.location, cmdargs))

def upshift_list(str_list):
    ret_list = []
    for val in str_list:
        ret_list.append(val.upper())
    return ret_list

##### Main function ############################################################

def table_file_name_map():
    # build search map

    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_base_names = options.agent_env['HVR_BASE_NAMES'].split(":")
    hvr_col_names = options.agent_env['HVR_COL_NAMES_BASE'].split(":")
    hvr_tbl_keys = options.agent_env['HVR_TBL_KEYS'].split(":")
    files = options.agent_env.get('HVR_FILE_NAMES', None)
    rows = options.agent_env.get('HVR_FILE_NROWS', None)

    if files:
        files = files.split(":")
    if rows:
        rows = rows.split(":")

    tbl_map = {}
    num_rows = {}
    for item in zip(hvr_base_names, hvr_tbl_names, hvr_col_names, hvr_tbl_keys):
        tbl_map[item] = []
        num_rows[item] = 0
        if files :
            pop_list = []
            for idx, f in enumerate(files):
                name = f[f.find("-")+1:]
                if name[-4:] == ".csv":
                    name = name[:-4]
                if name == item[1]:
                    tbl_map[item].append(f)
                    pop_list.append(idx)
                    num_rows[item] += int(rows[idx])
            # Pop files from list from high index to low to maintain index sanity
            for idx in reversed(pop_list):
                files.pop(idx)
                rows.pop(idx)

    if files :  
        raise Exception ("$HVR_FILE_NAMES contains unexpected list of files {0}".format(files))

    return tbl_map, num_rows

def files_found_in_filestore(target_table, file_list):
    files_there = 0
    file_loc = options.agent_env['HVR_FILE_LOC']
    for fname in file_list:
        full_name = os.path.join(file_loc, fname)
        if os.path.exists(full_name):
            files_there += 1
    if files_there == 0:
        trace(1, "Skipping table {0}; no files in {1}".format(target_table, file_loc))
        return False
    if files_there < len(file_list):
        raise Exception("Not all files in HVR_FILE_NAMES found in {0} for {1}".format(file_loc, target_table))
    return True

def delete_files_from_filestore(file_list):
    file_loc = options.agent_env['HVR_FILE_LOC']
    for fname in file_list:
        full_name = os.path.join(file_loc, fname)
        if os.path.exists(full_name):
            os.remove(full_name)

def get_col_list_from_header(file_list):
    file_loc = options.agent_env['HVR_FILE_LOC']
    for fname in file_list:
        full_name = os.path.join(file_loc, fname)
        if os.path.exists(full_name):
            trace(2, "Read header line of {} to get column list".format(fname))
            with open(full_name, "r") as f:
                clist = f.readline()
                return clist[:-1].split(options.delimiter)
    return []

#
# ODBC functions ineracting with Databricks
#
def get_connect():
    connect_string = "DSN={}".format(options.dsn)
    try:
        options.odbc = pyodbc.connect(connect_string, autocommit=True)
        options.odbc.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        options.odbc.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
        options.odbc.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-16')
        options.odbc.setencoding(encoding='utf-8')
        options.cursor = options.odbc.cursor()
    except pyodbc.Error as ex:
        print("Failed to connect using '{}'".format(connect_string))
        raise ex

def close_connect():
    options.cursor.close()
    options.odbc.close()

def execute_sql(sql_stmt, sql_name):
    trace(2, "Execute: {0}".format(sql_stmt))
    try:
        options.cursor.execute(sql_stmt)
        options.cursor.commit()
    except pyodbc.Error as pe:
        print("{0} SQL failed: {1}".format(sql_name, sql_stmt))
        raise pe
    except Exception as ex:
        print("Executing {0} SQL raised: {1}".format(sql_name, type(ex)))
        raise ex
    except:
        print("Executing {0} SQL generated unexpected error {1}".format(sql_name, format(sys.exc_info()[0])))
        raise

def truncate_table(table_name):
    trunc_sql = "TRUNCATE TABLE {0}".format(table_name)
    trace(1, "Truncating table " + table_name)
    execute_sql(trunc_sql, 'Truncate')

def drop_table(table_name):
    drop_sql = "DROP TABLE {0} IF EXISTS".format(table_name)
    trace(1, "Dropping table " + table_name)
    execute_sql(drop_sql, 'Drop')

def get_table_columns(base_name):
    sql_stmt = "SELECT COLUMN_NAME, TYPE_NAME, COLUMN_SIZE, DECIMAL_DIGITS FROM _V_ODBC_COLUMNS3 WHERE TABLE_NAME='{0}'".format(base_name.upper())
    trace(2, "Execute: {0}".format(sql_stmt))
    tab_cols = []
    try:
        options.cursor.execute(sql_stmt)
        while True:
            col = options.cursor.fetchone()
            if col is None:
                break
            if not col:
                break
            tab_cols.append(col)
    except pyodbc.Error as ex:
        print("Select SQL failed: {1}".format(sql_stmt))
        raise ex
    trace(2, "Result set for '{0}': {1}".format(base_name, tab_cols))
    return tab_cols

#
# Process the data
#
def merge_into_target_from_burst(burst_table, target_table, columns, keylist):
    if options.optype_burst_only:
        if options.optype in columns:
            columns.remove(options.optype)
    if options.isdeleted_burst_only:
        if options.isdeleted in columns:
            columns.remove(options.isdeleted)
    keys = keylist.split(',')
    if options.optype in keys:
        keys.remove(options.optype)
    if options.isdeleted in keys:
        keys.remove(options.isdeleted)

    merge_sql = "MERGE INTO {0} a USING {1} b".format(target_table, burst_table)
    merge_sql += " ON"
    for key in keys:
        merge_sql += " a.{0} = b.{0} AND".format(key)
    merge_sql = merge_sql[:-4]
    if options.isdeleted_burst_only:
        merge_sql += " WHEN MATCHED AND b.{} = 0 THEN DELETE".format(options.optype)
        merge_sql += " WHEN MATCHED AND b.{} != 0 THEN UPDATE".format(options.optype)
    else:
        merge_sql += " WHEN MATCHED THEN UPDATE".format(options.optype)
    merge_sql += "  SET"
    for col in columns:
        merge_sql += " a.{0} = b.{0},".format(col)
    merge_sql = merge_sql[:-1]
    merge_sql += " WHEN NOT MATCHED THEN INSERT"
    merge_sql += "  ("
    for col in columns:
        merge_sql += "{0},".format(col)
    merge_sql = merge_sql[:-1]
    merge_sql += ") VALUES ("
    for col in columns:
        merge_sql += "b.{0},".format(col)
    merge_sql = merge_sql[:-1]
    merge_sql += ")"

    trace(1, "Merging changes from {0} into {1}".format(burst_table, target_table))
    execute_sql(merge_sql, 'Merge')

def create_external_burst_table(sql, burst_table, file_path):
    create_sql = sql.format(file_path)
    trace(1, "Create external table into {0} from {1}".format(burst_table, file_path))
    execute_sql(create_sql, 'Create')

def external_burst_table_sql(load_table, target_table, columns, col_types):
    cols = '('
    for c in columns:
        if len(cols) > 1:
            cols += ', '
        cols += c
        if c == options.optype:
            cols += ' INTEGER'
        elif c == options.isdeleted:
            cols += ' INTEGER'
        elif c in col_types:
            cols += ' ' + col_types[c]
        else:
            print("Table {0}, HVR columns {1}".format(target_table, columns))
            print("Table {0}, columns & types {1}".format(target_table, col_types))
            raise Exception("Mismatch between columns in HVR definition and columns in target table")
    cols += ')'
    extern_sql = "CREATE EXTERNAL TABLE {0} {1} USING (dataobject('{{0}}') DELIMITER '{2}' REMOTESOURCE 'ODBC' SKIPROWS 1)".format(load_table, cols, options.delimiter)
    return extern_sql

def insert_into_target_from_file(target_table, full_name, columns):
    source_columns = ''
    target_columns = ''
    for c in columns:
        if len(source_columns) > 1:
            source_columns += ', '
        source_columns += c
        if c == options.optype and options.optype_burst_only:
            continue
        if c == options.isdeleted and options.isdeleted_burst_only:
            continue
        if len(target_columns) > 1:
            target_columns += ', '
        target_columns += c
    insert_sql = "INSERT INTO {0}({1}) SELECT {2} FROM EXTERNAL '{3}' ({4}) USING (DELIMITER '{5}' REMOTESOURCE 'ODBC' SKIPROWS 1)".format(target_table, target_columns, target_columns, full_name, source_columns, options.delimiter)
    trace(1, "Insert into {0} from {1}".format(target_table, full_name))
    execute_sql(insert_sql, 'Insert')

def merge_table_data_to_target(burst_table, target_table, columns, col_types, key_list, file_list):
    extern_fmt = external_burst_table_sql(burst_table, target_table, columns, col_types)
    file_loc = options.agent_env['HVR_FILE_LOC']
    for fname in file_list:
         full_name = os.path.join(file_loc, fname)
         create_external_burst_table(extern_fmt, burst_table, full_name)
         merge_into_target_from_burst(burst_table, target_table, columns, key_list)

def push_table_data_to_target(target_table, columns, file_list):
    file_loc = options.agent_env['HVR_FILE_LOC']
    for fname in file_list:
         full_name = os.path.join(file_loc, fname)
         insert_into_target_from_file(target_table, full_name, columns)

def col_in_table_list(col, table_list):
    for tab_col in table_list:
        if col == tab_col[0]:
            return True
    return False

def get_col_type(colname, table_columns_and_types):
    for tab_col in table_columns_and_types:
        if colname.lower() == tab_col[0].lower():
            return tab_col
    return []
        
def check_data_in_file(columns, table_columns_and_types, file_list):
    import csv

    for fname in file_list:
        with open(fname) as csvfile:
            trace(2,"Check data in {}".format(fname))
            rows = csv.reader(csvfile, delimiter=options.delimiter)
            next(rows)   # skip the header
            showdate = 0
            for row in rows:
                for colnm, colval in zip(columns, row):
                    col_type = get_col_type(colnm, table_columns_and_types)
                    if col_type:
                        if 'CHAR' in col_type[1] and len(colval) > col_type[2]:
                            print("Column {}, type {} has length {} (> {}), value {}, in row {}".format(colnm, col_type[1], len(colval), col_type[2], colval, row))
                        if not showdate and col_type[1] == 'DATE' and len(colval) > 10:
                            showdate += 1
                            print("Date column {} has value {} in row {}".format(colnm, colval, row))

def process_table_columns(base_name, columns):
    table_columns_and_types = get_table_columns(base_name)

    # make sure the number of columns is equal
    num_hvr_cols = len(columns)
    if options.mode == "integ_end":
        if options.optype in columns and options.optype_burst_only:
            num_hvr_cols -= 1
        if options.isdeleted in columns and options.isdeleted_burst_only:
            num_hvr_cols -= 1
    if num_hvr_cols != len(table_columns_and_types):
        raise Exception("Source/target column mismatch; number of source columns = {0}; number of target columns = {1}".format(num_hvr_cols, len(table_columns_and_types)))

    # return a list of table columns and types
    col_types = {}
    for tab_col in table_columns_and_types:
        if tab_col[0].lower() in columns:
            typespec = tab_col[1]
            if 'CHAR' in typespec:
                typespec += '(' + str(tab_col[2]) + ')'
            if typespec == 'NUMERIC':
                typespec += '(' + str(tab_col[2]) + ',' + str(tab_col[3]) + ')'
            col_types[tab_col[0].lower()] = typespec
    trace(2, "Column types: {}".format(col_types))
    return col_types, table_columns_and_types

def process_table(tab_entry, file_list, numrows):
    global file_counter
    
    target_table = tab_entry[0]
    columns = tab_entry[2].split(",")
    load_table = target_table + '__bur'

    # if refreshing an empty table, or table already processed, then skip this table
    if len(file_list) == 0 or not files_found_in_filestore(target_table, file_list):
        return

    file_columns = get_col_list_from_header(file_list)
    trace(2, "Column list from csv file: {}".format(file_columns))

    t = [0,0,0,0,0]
    t[0] = timer()
    use_burst_logic = options.mode == "integ_end" and not options.target_is_timekey
    if not use_burst_logic:
        if options.optype_burst_only:
            if options.optype in columns:
                columns.remove(options.optype)
        if options.isdeleted_burst_only:
            if options.isdeleted in columns:
                columns.remove(options.isdeleted)

    if use_burst_logic:
        drop_table(load_table)
    else:
        if options.mode == "refr_write_end" and options.truncate_target_on_refresh:
            truncate_table(target_table)
    t[1] = timer()
    col_types, raw_columns = process_table_columns(target_table, columns)
    t[2] = timer()

    if use_burst_logic:
        merge_table_data_to_target(load_table, target_table, file_columns, col_types, tab_entry[3], file_list)
    else:
        if options.check_column_data:
            check_data_in_file(columns, raw_columns, file_list)
        push_table_data_to_target(target_table, file_columns, file_list)

    t[3] = timer()
    if use_burst_logic:
        drop_table(load_table)

    file_counter += len(file_list)
    delete_files_from_filestore(file_list)
    t[4] = timer()
    if use_burst_logic:
        trace(0, "Merged {0} changes into '{1}' in {2:.2f} seconds:"
                 " process columns: {3:.2f}s,"
                 " merge into target: {4:.2f}s"
                 " ".format(numrows, target_table, t[4]-t[0], t[2]-t[1], t[3]-t[2]))
    else:
        if options.mode == "refr_write_end":
            trace(0, "Refresh of '{0}', {1} rows, took {2:.2f} seconds:"
                     " copy into target: {3:.2f}s,".format(target_table, numrows, t[3]-t[0], t[2]-t[1],))
        else:
            trace(0, "Copy of {0} rows into {1} took {2:.2f} seconds".format(numrows, target_table, t[4]-t[0]))

def process_tables():
    get_connect()
    tbl_map, num_rows = table_file_name_map()
    try:
        for t in tbl_map:
            process_table(t, tbl_map[t], num_rows[t])
    finally:        
        close_connect()
        pass

def process(argv):
    version_check(argv[0])
    process_args(argv)
    env_load()
    trace_input()

    if ((options.mode == "refr_write_end" or options.mode == "integ_end") and
         os.getenv('HVR_FILE_NAMES', '') != ''):

        process_tables()

        if (file_counter > 0) :
            print("Successfully processed {0:d} file(s)".format(file_counter))

if __name__ == "__main__":
    try:
        process(sys.argv)
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D03: {0}\n".format(err))
        sys.stderr.flush()
        sys.exit(1);

