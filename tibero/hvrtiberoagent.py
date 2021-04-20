#!python
# $Id: hvrtiberoagent.py 15576 2017-06-20 15:06:03Z aleksey $
# Copyright (c) 2000-2017 HVR Software bv

################################################################################
#
#     &{return $DEF{HVR_VER_LONG}}&
#     &{return $DEF{HVR_COPYRIGHT}}&
#
################################################################################
# 
# NAME
#     hvrtiberoagent.py - HVR Tibero DB integrate agent
#
# SYNOPSIS
#     as agent
#     python hvrtiberoagent.py mode loc chn agrs
#   
# DESCRIPTION
#
#     This script can be used to send CDC data to Tibero DB.
#
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
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#          
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_TIBERO_DSN         (required)
#        This variable set the ODBC DSN used to access the target database.
#
#     HVR_TIBERO_DB_USER     (required)
#        This variable set the database user name used to access the target database.
#
#     HVR_TIBERO_DB_PASSWD   (required)
#        This variable set the database passowrd used to access the target database.
#
#     HVR_TIBERO_DB_SID      (required)
#        This variable set the database name used to access the target database.
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
#     HVR_SCHEMA             (optional)       
#        This  variable sets schema name
#    
#     HVR_FILE_NAMES         (required)
#        This  variable contains list of files transfered into HDFS.
#        If empty - intergarion process is  omitted
#
#     HVR_CASSANDRA_TRACE   (advanced,optional, default:0)
#         See the DIAGNOSTICS section.
#
#
################################################################################


import sys
import getopt
import os
import traceback
import pyodbc
import subprocess
from timeit import default_timer as timer
from enum import Enum

class Options:
    trace = 0
    dsn = ''
    db_user = ''
    db_passwd = ''
    db_sid = ''

file_counter = 0
options = Options()

##### Support function #########################################################

def trace(level, msg):
    if options.trace >= level:
        print(msg)

def env_load():

    options.trace = int(os.getenv('HVR_TIBERO_TRACE', options.trace))
    options.dsn = os.getenv('HVR_TIBERO_DSN', options.dsn)
    options.db_sid = os.getenv('HVR_TIBERO_DB_SID', options.db_sid)
    options.db_user = os.getenv('HVR_TIBERO_DB_USER', options.db_user)
    options.db_passwd = os.getenv('HVR_TIBERO_DB_PASSWD', options.db_passwd)

    if (options.dsn == '') :
        raise Exception("HVR_TIBERO_DSN enviroment variable must be defined")

    return options

def env_var_print():
    """
    """
    trace(3, "============================================")
    env = os.environ
    for key, value  in env.iteritems():
        if key.find('HVR') != -1:
            trace(3, key + " = " + value)
    trace(3, "============================================")

##### Main function ############################################################

def table_name_normalize(name):
    index = name.find(".")
    schema = ''
    if index != -1 :
       schema = name [:index]

    return schema, name[index + 1:]

def table_file_name_map():
    # build search map
    
    hvr_tbl_names = os.getenv('HVR_TBL_NAMES').split(":")
    hvr_base_names = os.getenv('HVR_BASE_NAMES').split(":")
    hvr_col_names = os.getenv('HVR_COL_NAMES').split(":")
    hvr_tbl_keys = os.getenv('HVR_TBL_KEYS').split(":")
    files = os.getenv('HVR_FILE_NAMES')
    if files:
        files = files.split(":")
    
    tbl_map = {}
    for item in zip(hvr_base_names, hvr_tbl_names, hvr_col_names, hvr_tbl_keys):
        tbl_map[item] = []
        if files :
            pop_list = []
            for idx, f in enumerate(files):
                name = f[f.find("-")+1:]
                if name[-4:] == ".csv":
                    name = name[:-4]
                if name == item[1]:
                    tbl_map[item].append(f)
                    pop_list.append(idx)
            # Pop files from list from high index to low to maintain index sanity
            for idx in reversed(pop_list):
                files.pop(idx)

    if files :
        raise Exception ("$HVR_FILE_NAMES contains unexpected list of files {0}".format(files))

    return tbl_map


def drop_table(session, entry):

        drop_table_query = "DROP TABLE IF EXISTS {0}"
        schema_name, base_name = table_name_normalize(entry[0])
        
        trace(1, "Dropping table... " + base_name)

        session.execute(drop_table_query.format(base_name))
        

def create_burst_table(cursor, burst_table_name, base_name, cols, keys):

    create_table_query = """
            CREATE TABLE {0}
         (
    """
                
    for col in cols:
        create_table_query += "{0},\n".format(col)

    thekey = ','.join([str(x) for x in keys]) 

    create_table_query += "PRIMARY KEY ({0})) ".format(thekey)
    create_table_query += "AS SELECT "
    for col in cols:
        if col != 'op_type':
            create_table_query += "{0},\n".format(col)
    create_table_query += "CAST(1 AS NUMBER) \n"
    create_table_query += "FROM {0} WHERE 1=0".format(base_name)

    trace(1, "Creating table... " + burst_table_name)
    
    try:
        stmt = create_table_query.format(burst_table_name)
        trace(2, stmt)
        cursor.execute(stmt)
        cursor.commit()
    except Exception as e:
        if e.args[0] == '72000':
            cursor.execute("""truncate table {0}""".format(burst_table_name))
            cursor.commit()
        else:
            raise e

def process_burst_table(cursor, mode, burst_table, target_table, cols, keys):

#remove the op_type from keys
    keys.remove('op_type')
    if mode == "integ_end":
        # First do the deletes
        sql_stmt = "DELETE FROM {0} WHERE EXISTS (SELECT 1 FROM {1} bur_ WHERE ".format(target_table, burst_table)
        for key in keys:
            sql_stmt += "{0}.{1} = bur_.{1} AND ".format(target_table, key)
        sql_stmt += "bur_.op_type = 0)"

        trace(2, sql_stmt)
        cursor.execute(sql_stmt)
        cursor.commit()

        # Second, do the updates
        # Build the list of cols we want to set (non-key cols and removing the op_type col)
        upd_cols = []
        for col in cols:
            if (not (col in keys)) and (col != 'op_type'):
                upd_cols.append(col)

        sql_stmt = "MERGE INTO {0} bas_ USING {1} bur_ ON (".format(target_table, burst_table)
        first_col = True
        for key in keys:
            if not first_col:
                sql_stmt += "AND "
            sql_stmt += "bas_.{0}=bur_.{0} ".format(key)
            first_col = False
        sql_stmt += "AND bur_.op_type=2) WHEN MATCHED THEN UPDATE SET "

        first_col = True
        for col in upd_cols:
            if not first_col:
                sql_stmt += ", "
            sql_stmt += "bas_.{0}=bur_.{0} ".format(col)
            first_col = False
        
        trace(2, sql_stmt)
        cursor.execute(sql_stmt)
        cursor.commit()

# Finally, do the inserts, this applies to the refr_end mode as well
# In refr_end mode we want to truncate the target table first
    if mode == "refr_write_end":
        sql_stmt = "TRUNCATE TABLE {0} ".format(target_table)
        trace(2, sql_stmt)
        cursor.execute(sql_stmt)
        cursor.commit()

    cols.remove('op_type')

    sql_stmt = "INSERT INTO {0} (".format(target_table)
    first_col = True
    for col in cols:
        if not first_col:
            sql_stmt += ", "
        sql_stmt += col
        first_col = False
    sql_stmt += ") SELECT "
    # TODO Optimize building SELECT into the loop above.
    first_col = True
    for col in cols:
        if not first_col:
            sql_stmt += ", "
        sql_stmt += "bur_.{0}".format(col)
        first_col = False
    sql_stmt += " FROM {0} bur_ WHERE ".format(burst_table)
    sql_stmt += "bur_.op_type=1"

    trace(2, sql_stmt)
    cursor.execute(sql_stmt)
    cursor.commit()

#def get_table_col_defs(cursor, base_name):
#    for row in cursor.columns(base_name):
#        trace(2,row)
    
def process_burst_file(cursor, mode, tbl_entry, full_name):

    global file_counter
    first_col = True
    
    trace(1, "Processing file '" + full_name+ "' ... ")

    cols = tbl_entry[2].split(",")
    keys = tbl_entry[3].split(",")
    fieldnames = tbl_entry[2].split(",")
    
    col_names = ','.join([str(x) for x in cols])
    
    cols_placeholders = ','.join(['%s' for x in cols]) 
    keys_placeholders = ' AND '.join([x +'=%s' for x in keys])
    
    burst_table_name = tbl_entry[0]+"__b"
    base_name = tbl_entry[1]

 #  get_table_col_defs(cursor, base_name)

    create_burst_table(cursor, burst_table_name, base_name, cols, keys)
    
    loader_str = "LOAD DATA\n"
    loader_str += "INFILE '{0}'\n".format(full_name)
    loader_str += "APPEND\n"
    loader_str += "INTO TABLE {0}\n".format(burst_table_name)
    loader_str += "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n"
    loader_str += "(\n"
    for col in cols:
        if first_col :
            loader_str += "{0}\n".format(col)
            first_col = False
        else:
            loader_str += ",{0}\n".format(col)
    loader_str += ")"
    trace(2, loader_str)
    
    try:
        ctl_file = open(full_name+".ctl", "w")
        ctl_file.write(loader_str)
        ctl_file.close()
    except IOError as err:
        raise Exception ("Couldn't write file " + full_name)

    #TODO Improve error handling and report failed loader run
    #
    stdout_file = open(full_name+".out", "w")
    rc = subprocess.call(["/home/tibero/tibero6/client/bin/tbloader","control={0}".format(full_name+".ctl"),"userid={0}/{1}@{2}".format(options.db_user,options.db_passwd,options.db_sid),"skip=1","direct=Y"],stdout=stdout_file)
    stdout_file.close()
    files_to_process = True
    try:
        os.remove(full_name)
        os.remove(full_name+".ctl")
        os.remove(full_name+".out")
        os.remove(full_name+".log")
    except Exception as e:
        files_to_process = False

    if files_to_process:
        process_burst_table (cursor, mode, burst_table_name, base_name, cols, keys)

def file_loc_process(mode, file_loc):
    conn = pyodbc.connect("DSN={0}".format(options.dsn))
    cursor = conn.cursor()
    tbl_map = table_file_name_map()
       
    try:
        for t in tbl_map:
            for name in tbl_map[t]:
                full_name = os.path.join(file_loc, name)
                process_burst_file(cursor, mode, t, full_name)
    finally:        
        cursor.close()
        conn.close()


def file_loc_cleanup():
    file_loc = os.getenv('HVR_FILE_LOC')
    files = os.listdir(file_loc)
    for name in files:
        if (name == "." or name == ".." or os.path.isdir(name) == True) :
            continue
        
        full_name = os.path.join(file_loc, name)
        os.remove(full_name)   
     

def userargs_parse(cmdargs):
    try:
        list_args = cmdargs[0].split(" ");
        opts, args = getopt.getopt(list_args,"apst")
    except getopt.GetoptError:
        raise Exception("Couldn't parse command line arguments")


def process(argv, userarg):

    env_load()
    trace(3,argv)
    userargs_parse(userarg)
    env_var_print()
    print("mode={0}".format(argv))
    if ((argv == "refr_write_end" or argv == "integ_end") and
         os.getenv('HVR_FILE_NAMES') != ''):

        file_loc_process(argv, os.getenv('HVR_FILE_LOC'))

        if (file_counter > 0) :
            print ("Successfully transmitted {0:d} file(s)".format(file_counter))

    if (argv == "refr_write_begin"):
        tbl_map = table_file_name_map()
        file_loc_cleanup()

if __name__ == "__main__":
    try:
        process(sys.argv[1], sys.argv[4:])
        trace(3,"T6")
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D01: {0}\n".format(err))
        trace(1,"{0}\n".format(tb))
        sys.stderr.flush()
        sys.exit(1)

