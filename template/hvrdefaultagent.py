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
#        Apply the data files as SQL insert/update/delete statements as 
#        if it were a burst table.
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
#     HVR_??????_TRACE   (advanced,optional, default:0)
#
# CHANGE_LOG
#     mm/dd/yyyy III: Initial release
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
    cursor = None
    dsn = None
    connect_string = None

file_counter = 0

options = Options()

##### Support function #########################################################

def trace(level, msg):
    if options.trace >= level:
        print(msg)
        sys.stdout.flush() 

def version_check():
    global python3

    python3 = sys.version_info[0] == 3
    
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
#    options.trace = int(os.getenv('HVR_????_TRACE', options.trace))
#    options.dsn = os.getenv('HVR_????_DSN', '')
    options.agent_env = load_agent_env()
   
def env_var_print():
    """
    """
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
  
    # log the calling options
    if int(os.getenv('HVR_????_TRACE', options.trace)) > 2:
        print("agent plugin called with {0} {1} {2} {3}".format(options.mode, options.channel, options.location, cmdargs))

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
    trace(1, "*** check files are there ***")
    pass

def delete_files_from_filestore(file_list):
    trace(1, "*** delete files after processing ***")

#
# ODBC functions ineracting with Databricks
#
def get_connect():
    connect_string = "DSN={}".format(options.dsn)
    try:
        Connections.odbc = pyodbc.connect(connect_string, autocommit=True)
        Connections.cursor = Connections.odbc.cursor()
    except pyodbc.Error as ex:
        print("Failed to connect using '{}'".format(connect_string))
        raise ex

def close_connect():
    Connections.cursor.close()
    Connections.odbc.close()

def execute_sql(sql_stmt, sql_name):
    trace(2, "Execute: {0}".format(sql_stmt))
    try:
        Connections.cursor.execute(sql_stmt)
        Connections.cursor.commit()
    except pyodbc.Error as ex:
        print("{0} SQL failed: {1}".format(sql_name, sql_stmt))
        raise ex
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
    drop_sql = "DROP TABLE IF EXISTS {0}".format(table_name)
    trace(1, "Dropping table " + table_name)
    execute_sql(drop_sql, 'Drop')

def create_burst_table(burst_table_name, base_name):
    create_sql = "CREATE TABLE {0} USING DELTA AS SELECT * FROM {1} WHERE 1=0".format(burst_table_name, base_name)
    trace(1, "Creating table " + burst_table_name)
    execute_sql(create_sql, 'Create')
    alter_sql = 'ALTER TABLE {0} ADD COLUMN (op_type integer)'.format(burst_table_name)
    trace(1, "Altering table " + burst_table_name)
    execute_sql(alter_sql, 'Alter')

def get_col_types(base_name, columns):
    sql_stmt = "DESCRIBE TABLE {0}".format(base_name)
    trace(1, "Describe table " + base_name)
    trace(2, "Execute: {0}".format(sql_stmt))
    col_types = {}
    try:
        Connections.cursor.execute(sql_stmt)
        while True:
            col = Connections.cursor.fetchone()
            if not col:
                break
            trace(3, "  {0}".format(col))
            if len(col) > 1 and col[0] in columns:
                col_types[col[0]] = col[1]
    except pyodbc.Error as ex:
        print("{0} SQL failed: {1}".format(sql_name, sql_stmt))
        raise ex
    trace(2, "Column types: {}".format(col_types))
    return col_types

#
# Process the data
#
def merge_into_target_from_burst(burst_table, target_table, columns, keylist):
    if 'op_type' in columns:
        columns.remove('op_type')
    keys = keylist.split(',')
    if "op_type" in keys:
        keys.remove('op_type')

    merge_sql = "MERGE INTO {0} a USING {1} b".format(target_table, burst_table)
    merge_sql += " ON"
    for key in keys:
        merge_sql += " a.{0} = b.{0} AND".format(key)
    merge_sql = merge_sql[:-4]
    merge_sql += " WHEN MATCHED AND b.op_type = 0 THEN DELETE"
    merge_sql += " WHEN MATCHED AND b.op_type != 0 THEN UPDATE"
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

def move_changes_to_burst(load_table, target_table, columns, col_types, file_list):
    trace(1, "*** move changes to burst ***")

def process_table(tab_entry, file_list, numrows):
    global file_counter
    
    target_table = tab_entry[0]
    columns = tab_entry[2].split(",")
    load_table = target_table

    # if refreshing an empty table, or table already processed, then skip this table
    if len(file_list) == 0 or not files_found_in_filestore(target_table, file_list):
        return

    t = [0,0,0,0,0,0]
    t[0] = timer()
    if options.mode == "integ_end":
        load_table += '__bur'
    else:
        if 'op_type' in columns:
            columns.remove('op_type')

    if options.mode == "integ_end":
        drop_table(load_table)
        create_burst_table(load_table, target_table)
    else:
        truncate_table(load_table)
    t[1] = timer()

    col_types = get_col_types(target_table, columns)
    copy_into_delta_table(load_table, target_table, columns, col_types, file_list)
    t[2] = timer()

    if options.mode == "integ_end":
        merge_into_target_from_burst(load_table, target_table, columns, tab_entry[3])
        t[3] = timer()
        drop_table(load_table)
        t[4] = timer()

    file_counter += len(file_list)
    delete_files_from_filestore(file_list)
    t[5] = timer()
    if options.mode == "integ_end":
        trace(0, "Merged {0} changes into '{1}' in {2:.2f} seconds:"
                 " create burst: {3:.2f}s,"
                 " copy into burst: {4:.2f}s,"
                 " merge into target: {5:.2f}s,"
                 " drop burst: {6:.2f}s".format(numrows, target_table, t[5]-t[0], t[1]-t[0], t[2]-t[1], t[3]-t[2], t[4]-t[3]))
    else:
        trace(0, "Refresh of '{0}', {1} rows, took {2:.2f} seconds:"
                 " truncate target: {3:.2f}s,"
                 " copy into target: {4:.2f}s,".format(target_table, numrows, t[5]-t[0], t[1]-t[0], t[2]-t[1],))

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
    version_check()
    process_args(argv)
    env_load()
    env_var_print()

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

