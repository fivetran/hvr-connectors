#!python
# Copyright (c) 2000-2017 HVR Software bv

################################################################################
#
#     &{return $DEF{HVR_VER_LONG}}&
#     &{return $DEF{HVR_COPYRIGHT}}&
#
################################################################################
# 
# NAME
#     hvractianagent.py - HVR Actian Avalanche DB integrate agent
#
# SYNOPSIS
#     as agent
#     python hvractianagent.py mode loc chn
#   
# DESCRIPTION
#
#     This script can be used to send CDC data to Actian Avalanche.
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
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_ACTIAN_DSN         (required)
#        This variable set the ODBC DSN used to access the target database.
#
#     HVR_ACTIAN_DB_USER     (optional)
#        This variable set the database user name used to access the target database.
#        Default is 'dbuser'
#
#     HVR_ACTIAN_DB_PASSWD   (required)
#        This variable set the database password used to access the target database.
#
#     HVR_ACTIAN_DB_HOST     (required)
#        This variable set the host id for the target database.
#
#     HVR_ACTIAN_DB_PORT     (required)
#        This variable set the port for connecting to the for the target database.
#        Default is 27832
#
#     HVR_ACTIAN_FILESTORE_ID  (required)
#        This variable sets the access ID/client ID for the cloud storage where the
#        files were written by integrate
#
#     HVR_ACTIAN_FILESTORE_KEY (required)
#        This variable sets the secret key for the cloud storage where the files
#        were written by integrate
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
#     HVR_ACTIAN_TRACE   (advanced,optional, default:0)
#
# CHANGE_LOG
#     07/30/2020 RLR: Implementation done for S3 as the integrate target
#     08/15/2020 RLR: Start of changes for Azure BLOB
#                     Fixed before refresh action
#
################################################################################


import sys
import getopt
import os
import traceback
import pyodbc
import subprocess
import time
import boto3
from timeit import default_timer as timer
from enum import Enum

RETRY_INSERT = 10
RETRY_DELAY_FACTOR = 0.2

class Options:
    s3_path = ''
    s3_bucket = ''
    trace = 0
    dsn = ''
    db_user = 'dbuser'
    db_passwd = ''
    db_host = ''
    db_port = '27832'
    access_id = ''
    secret_key = ''
    copy_file = ''
    load_opts = ''

file_counter = 0
options = Options()

##### Support function #########################################################

def trace(level, msg):
    if options.trace >= level:
        print(msg)

def env_load():

    options.s3_path = os.getenv('HVR_FILE_LOC', options.s3_path)
    options.trace = int(os.getenv('HVR_ACTIAN_TRACE', options.trace))
    options.dsn = os.getenv('HVR_ACTIAN_DSN', options.dsn)
    options.db_user = os.getenv('HVR_ACTIAN_DB_USER', options.db_user)
    options.db_passwd = os.getenv('HVR_ACTIAN_DB_PASSWD', options.db_passwd)
    options.db_host = os.getenv('HVR_ACTIAN_DB_HOST', options.db_host)
    options.db_port = os.getenv('HVR_ACTIAN_DB_PORT', options.db_port)
    options.access_id = os.getenv('HVR_ACTIAN_FILESTORE_ID', options.access_id)
    options.secret_key = os.getenv('HVR_ACTIAN_FILESTORE_KEY', options.secret_key)
    options.load_opts = os.getenv('HVR_ACTIAN_LOAD_OPTS', options.load_opts)

    if options.s3_path:
        ind = options.s3_path.find("@")
        if ind > 0:
            options.s3_bucket = options.s3_path[ind+1:]
            if options.s3_bucket[-1:] == "/":
                options.s3_bucket = options.s3_bucket[:-1]
        
    if (options.dsn == '') :
        raise Exception("HVR_ACTIAN_DSN enviroment variable must be defined")
    if (options.db_passwd == '') :
        raise Exception("HVR_ACTIAN_DB_PASSWD enviroment variable must be defined")
    if (options.db_host == '') :
        raise Exception("HVR_ACTIAN_DB_HOST enviroment variable must be defined")
    if (options.access_id == '') :
        raise Exception("HVR_ACTIAN_DB_AWS_ID enviroment variable must be defined")
    if (options.secret_key == '') :
        raise Exception("HVR_ACTIAN_DB_AWS_KEY enviroment variable must be defined")

    os.putenv("II_ODBC_WCHAR_SIZE", "2")

    return options

def env_var_print():
    """
    """
    trace(3, "============================================")
    env = os.environ
    for key, value  in env.items():
        if key.find('HVR') != -1:
            trace(3, key + " = " + value)
    for key, value  in env.items():
        if key.find('II') != -1:
            trace(3, key + " = " + value)
        if key == 'PATH':
            trace(3, key + " = " + value)
    trace(3, "============================================")

##### Main function ############################################################

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

def delete_files_from_s3(name, file_list):
    trace(3, "Delete files from {0} for {1}".format(options.s3_bucket, name))
    try:
        s3 = boto3.resource('s3', aws_access_key_id=options.access_id, aws_secret_access_key=options.secret_key)
    except Exception as ex:
        print("Failed creating resource service client for s3")
        raise ex
    for name in file_list:
        try:
            obj = s3.Object(options.s3_bucket, name)
            obj.delete()
        except Exception as ex:
            print("Failed deleting {0} from s3:{1}", name, options.s3_bucket)
            raise ex

def drop_table(cursor, table_name):

    drop_table_query = "DROP TABLE IF EXISTS {0}"
    trace(2, "Dropping table... " + table_name)
    try:
        stmt = drop_table_query.format(table_name)
        trace(2, stmt)
        cursor.execute(stmt)
        cursor.commit()
    except pyodbc.Error as ex:
        print("Drop SQL failed: " + stmt)
        raise ex
        
def create_burst_table(cursor, burst_table_name, base_name, cols):

    create_table_query = """
            CREATE TABLE {0}
         (
    """
                
    for col in cols:
        if col != 'op_type':
            create_table_query += "{0},\n".format(col)

    create_table_query = create_table_query[:-2]
    create_table_query += ")\n AS SELECT "
    for col in cols:
        if col != 'op_type':
            create_table_query += "{0},\n".format(col)
    create_table_query = create_table_query[:-2]
    create_table_query += "\nFROM {0} WHERE 1=0 WITH NOPARTITION".format(base_name)

    trace(2, "Creating table... " + burst_table_name)
    
    only_truncate= False;
    try:
        stmt = create_table_query.format(burst_table_name)
        trace(2, stmt)
        cursor.execute(stmt)
        cursor.commit()
    except pyodbc.Error as ex:
        if ex.args[0] == '42502':
            trace(1, "Burst table {0} already exists".format(burst_table_name))
            only_truncate= True
            pass
        else:
            print("Create SQL failed: " + stmt)
            raise ex

    if only_truncate:
        truncate_table_query = """
              TRUNCATE TABLE {0}
        """
        trace(2, "Truncating table... " + burst_table_name)
    
        try:
            stmt = truncate_table_query.format(burst_table_name)
            trace(2, stmt)
            cursor.execute(stmt)
            cursor.commit()
        except pyodbc.Error as ex:
            print("Truncate SQL failed: " + stmt)
            raise ex
        return

    alter_table_query = """
           ALTER TABLE {0} ADD COLUMN op_type int
    """
    trace(2, "Altering table... " + burst_table_name)
    
    try:
        stmt = alter_table_query.format(burst_table_name)
        trace(2, stmt)
        cursor.execute(stmt)
        cursor.commit()
    except pyodbc.Error as ex:
        print("Alter SQL failed: " + stmt)
        raise ex
    
def execute_bulk_insert(cursor, sql_stmt, try_count):
    try:
        cursor.execute(sql_stmt)
        cursor.commit()
    except pyodbc.Error as ex:
        if ex.args[0] != '40001':
            raise ex
        delay = RETRY_DELAY_FACTOR * try_count
        trace(1, "Bulk update not complete; rollback insert and try again after {0:0.1f} ms".format(delay))
        cursor.rollback()
        time.sleep(delay)
        return -1
    return cursor.rowcount

def process_burst_table(cursor, mode, burst_table, target_table, cols, keys):

    if "op_type" in keys:
        keys.remove('op_type')
    counts = [0,0,0]
    if mode == "integ_end":
        # force batch mode
#        sql_stmt = "SET INSERTMODE ROW"
#        trace(2, sql_stmt)
#        cursor.execute(sql_stmt)

        # First do the deletes
        sql_stmt = "DELETE FROM {0} WHERE EXISTS (SELECT 1 FROM {1} bur_ WHERE ".format(target_table, burst_table)
        for key in keys:
            sql_stmt += "{0}.{1} = bur_.{1} AND ".format(target_table, key)
        sql_stmt += "bur_.op_type = 0)"

        trace(2, sql_stmt)
        cursor.execute(sql_stmt)
        counts[0] = cursor.rowcount
        cursor.commit()

        # Second, do the updates
        # Build the list of cols we want to set (non-key cols and removing the op_type col)
        upd_cols = []
        for col in cols:
            if (not (col in keys)) and (col != 'op_type'):
                upd_cols.append(col)

        if upd_cols:
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
            counts[1] = cursor.rowcount
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
    first_col = True
    for col in cols:
        if not first_col:
            sql_stmt += ", "
        sql_stmt += "bur_.{0}".format(col)
        first_col = False
    sql_stmt += " FROM {0} bur_ WHERE ".format(burst_table)
    sql_stmt += "bur_.op_type=1"

    trace(2, sql_stmt)
    for i in range(1,RETRY_INSERT+1):
        counts[2] = execute_bulk_insert(cursor, sql_stmt, i)
        if counts[2] > -1:
            break
        if i >= RETRY_INSERT:
            print("Bulk insert failed after {0} retries".format(RETRY_INSERT))
            print("Bulk update of {0} rows still running".format(counts[1]))
            raise Exception("[40001] Error committing transaction: Concurrent appends on table are not allowed")

    print("{0} rows deleted, {1} rows updated, and {2} rows inserted to {3}".format(counts[0],counts[1],counts[2],target_table))

def load_burst_from_files(cursor, mode, tbl_entry, file_list):
    global file_counter
    
    cols = tbl_entry[2].split(",")
    keys = tbl_entry[3].split(",")
    if len(file_list) == 0:
        return

    burst_table_name = tbl_entry[0]+"__b"
    create_burst_table(cursor, burst_table_name, tbl_entry[0], cols)
   
    copy_sql = "COPY {0}() VWLOAD FROM \n"

    for name in file_list:
        filespec = "'s3a://" + options.s3_bucket + "/" + name + "'"
        if len(file_list) > 1 and name != file_list[-1]:
            filespec += ","
        copy_sql += filespec + "\n"

    copy_sql += "with AWS_ACCESS_KEY='{}'".format(options.access_id)
    copy_sql += ",AWS_SECRET_KEY='{}'".format(options.secret_key)
    if options.load_opts:
        copy_sql += "," + options.load_opts
    copy_sql += "\g"
    stmt = copy_sql.format(burst_table_name)

    trace(2, stmt)
    trace(2, options.copy_file)
    with open(options.copy_file, 'w') as fh:
        fh.write(stmt)

    connect_string= "{0},tcp_ip,{1}[{2},{3}]::db".format(options.db_host, options.db_port,options.db_user, options.db_passwd)
    trace(2, connect_string)
    try:
        rval = subprocess.check_output("sql @" + connect_string + " <" + options.copy_file, shell=True)
    except subprocess.CalledProcessError as cpe:
        raise Exception(cpe.output)

    retval = rval.decode("utf-8")
    trace(3, retval)
    if options.trace:
        ind = retval.find("Executing")
        if ind > 0:
            ind = retval.find("(", ind)
            ind2 = retval.find(")", ind)
            if ind > 0 and ind2 > 0:
                trace(1, "{0} uploaded to {1}".format(retval[ind+1:ind2], burst_table_name))

    process_burst_table (cursor, mode, burst_table_name, tbl_entry[0], cols, keys)
    drop_table(cursor, burst_table_name)
    delete_files_from_s3(tbl_entry[0], file_list)
    file_counter += len(file_list)

def process_files(mode):
    try:
        conn = pyodbc.connect("DSN={0};UID={1};PWD={2}".format(options.dsn, options.db_user, options.db_passwd))
        cursor = conn.cursor()
    except pyodbc.Error as ex:
        print("Failed to connect using 'DSN={0};UID={1};PWD=xxxxx'".format(options.dsn, options.db_user))
        raise ex

    tbl_map = table_file_name_map()
       
    try:
        for t in tbl_map:
            load_burst_from_files(cursor, mode, t, tbl_map[t])
    finally:        
        cursor.close()
        conn.close()

def cleanup_s3_bucket(mode):
    file_list = []
    try:
        s3 = boto3.resource('s3', aws_access_key_id=options.access_id, aws_secret_access_key=options.secret_key)
    except Exception as ex:
        print("Failed creating resource service client for s3")
        raise ex
    try:
        bucket_ptr = s3.Bucket(options.s3_bucket)
        for file in bucket_ptr.objects.all():
             file_list.append(file.key)
    except Exception as ex:
        print("Failed creating resource service client for s3")
        raise ex
    if file_list:
        delete_files_from_s3(mode, file_list)

def args_parse(argv):
    if len(argv) < 4:
        raise Exception("Invalid command line: {}".format(argv))

    hvr_tmp= os.getenv('HVR_TMP', '')
    if (hvr_tmp == '') :
        raise Exception("HVR_TMP enviroment variable must be defined")
    options.copy_file = os.path.join(hvr_tmp, "pcopy_" + argv[2] + "_" + argv[3])
    return argv[1]

def process(argv):

    env_load()
    mode = args_parse(argv)
    trace(3,mode)
    env_var_print()
    if ((mode == "refr_write_end" or mode == "integ_end") and
         os.getenv('HVR_FILE_NAMES') != ''):

        process_files(mode)

        if (file_counter > 0) :
            print ("Successfully transmitted {0:d} file(s)".format(file_counter))

    if (mode == "refr_write_begin"):
        cleanup_s3_bucket(mode)

if __name__ == "__main__":
    try:
        process(sys.argv)
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D01: {0}\n".format(err))
        trace(1,"{0}\n".format(tb))
        sys.stderr.flush()
        sys.exit(1)

