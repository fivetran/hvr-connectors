#!python

# HVR 5.7.0
# Copyright (c) 2017-2020 HVR Software bv

################################################################################
#
#     HVR 5.7.0
#     Copyright (c) 2000-2020 HVR Software bv
#
################################################################################
# 
# NAME
#     hvrbigqueryagent.py - HVR Google BigQuery integrate agent
#
# SYNOPSIS
#     as agent
#     python hvr.py mode loc chn agrs
#   
# DESCRIPTION
#
#     This script can be used to send table rows into Google BigQuery
#
#
# OPTIONS
#     -l - set avro_logical_types
#     -m <hvr_op column name> -  handle multi-row deletes
#     -r - recreate table during refresh
#     -s <softcol> - Softdelete mode
#
# AGENT OPERATION
#
#     MODES
#
#     refr_write_begin
#
#     refr_write_end
#
#     integ_end
#
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#          
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_FILE_LOC          (required)
#        This varibale sets a location of files to be sent
#  
#     HVR_TBL_NAMES         (required)       
#        This  variable contains list of tables
#  
#     HVR_BASE_NAMES         (required)       
#        This  variable contains list of tables
#  
#     HVR_TBL_KEYS         (required)       
#        This  variable contains list of key columns
#  
#     HVR_COL_NAMES_BASE    (required)       
#        This  variable contains list of columns
#  
#     HVR_SCHEMA            (optional)       
#        This  variable sets schema name
#    
#     HVR_FILE_NAMES        (required)
#        This  variable contains list of files transfered into HDFS.
#        If empty - intergarion process is  omitted
#
#     HVR_GBQ_CREDFILE  (required)
#        This variable contans credential file path
#         If empty it is assumed that plugin has been stared inside  Google Cloud Platform
#
#     HVR_GBQ_PROJECTID (required)
#        This variable contains Google Cloud project id
#
#     HVR_GBQ_DATASETID  (required)
#        Thsi variable contains Google Cloud Cloud dataset Id, belongs to project Id
#
#     HVR_GBQ_TRACE   (advanced,optional, default:0)
#         See the DIAGNOSTICS section.
#
#     HVR_GBQ_MULTIDELETE  (advanced,optional, default:'')
#         The agent supports multi-delete operations that are outputted by the
#         SAPXForm module.  A multi-delete operation has an incomplete key.  For
#         the agent to implement the multi-delete using SQL it needs to know 
#         which operations are multi-delete and which columns are missing.
#         If the "-m" option is specified then by default the hvrbigqueryagent
#         will process all deletes for tables BSEG, BSET & BSEM as soft deletes,
#         defining the where clause with all the key columns except PAGENO.
#         The HVR_GBQ_MULTIDELETE env var allows the user to configure this
#         behavior for additional tables if necessary.  
#            <tablename>:<column list>;<tablename>:<column list>; . . 
#         For example:
#            TAB1:COL77;TAB2:COL33
#
# CHANGE LOG
#     12/03/2020 RLR  - Fixed the recovery logic.  Now the files are not deleted 
#                       until after all the changes have been applied.
#                     - Removed the obselete '-o' option (since recovery is fixed)
#                       The script always checks to see if the file is there and
#                       skips it if it is not
#                     - Added new option "-l" that tells the script to set 
#                       use_avro_logical_types = True on the load into BQ from avro
#                     - Fixed the issue where the target table is not recreated on
#                       refresh when "-r" is set if the target table's base name
#                       is different than the table name
#                     - Fixed the script to use the base column names
#
################################################################################
import sys
import traceback
import getopt
import os
import time
from enum import Enum
from google.cloud import bigquery
from timeit import default_timer as timer

class BqMode(Enum):
    MODE_NOT_DEFINED = 0
    MODE_TIMEKEY =  2
    MODE_SOFTDELETE =  3

class TablePolicy(Enum):
    TABLE_PRESERVE = 0
    TABLE_RECREATE = 1

class Options:
    table_policy = TablePolicy.TABLE_PRESERVE
    mode = BqMode.MODE_TIMEKEY
    credential_file = None
    project_id = None
    dataset_id = None
    softcol = ''
    trace = 0
    tbl_suffix = ''
    agent_env = None
    hvr_op_name = ''
    softdelete_map = {}
    logical_types = False

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
    
def version_check():
    global python3
    global bigqry_version
    global bq_new_interface

    python3 = sys.version_info[0] == 3
    bigqry_version = version_normalizer(bigquery.__version__)
    bq_new_interface = bigqry_version >= version_normalizer('1.0.0')
    
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
    options.trace = int(os.getenv('HVR_GBQ_TRACE', options.trace))
    options.credential_file = os.getenv('HVR_GBQ_CREDFILE', None)
    options.project_id = os.getenv('HVR_GBQ_PROJECTID', None)
    options.dataset_id = os.getenv('HVR_GBQ_DATASETID', None)

    if options.credential_file is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = options.credential_file

    options.agent_env = load_agent_env()
    get_softdelete_map()

def print_env_var(evname, value):
    if evname.find('HVR') != -1:
        if len(value) <= 100:
            trace(3, evname + " = " + value)
        else:
            trace(3, evname + " = " + value[:100] + " . . .")

def env_var_print():
    """
    """
    trace(3, "============================================")
    env = os.environ
    if python3:
        for key, value  in env.items():
            print_env_var(key, value)
    else:
        for key, value  in env.iteritems():
            print_env_var(key, value)
    trace(3, "============================================")

def userargs_parse(cmdargs):
    if len(cmdargs):
        try:
            list_args = cmdargs[0].split(" ");
            opts, args = getopt.getopt(list_args,"lm:s:r")
        except getopt.GetoptError:
            raise Exception("Error parsing command line arguments '" + cmdargs[0] + "' due to invalid argument or invalid syntax")
    
        for opt, arg in opts:
            if opt == '-l':
                options.logical_types = True
            elif opt == '-m':
                options.hvr_op_name = arg
            elif opt == '-s':
                options.mode = BqMode.MODE_SOFTDELETE
                options.softcol = arg
                options.tbl_suffix = '__b'
            elif opt == '-r':
                options.table_policy = TablePolicy.TABLE_RECREATE
  
##### Main function ############################################################

def table_name_normalize(name):
    index = name.find(".")
    schema = ''
    if index != -1 :
       schema = name [:index]

    return schema, name[index + 1:]

def table_file_name_map():
    # build search map

    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_base_names = options.agent_env['HVR_BASE_NAMES'].split(":")
    hvr_col_names = options.agent_env['HVR_COL_NAMES_BASE'].split(":")
    hvr_tbl_keys = options.agent_env['HVR_TBL_KEYS'].split(":")
    files = options.agent_env.get('HVR_FILE_NAMES', None)

    if files:
        files = files.split(":")

    tbl_map = {}
    for item in zip(hvr_base_names, hvr_tbl_names, hvr_col_names, hvr_tbl_keys):
        tbl_map[item] = []
        if files :
            pop_list = []
            for idx, f in enumerate(files):
                name = f[f.find("-")+1:]
                if name[-5:] == ".avro":
                    name = name[:-5]
                if name == item[1]:
                    tbl_map[item].append(f)
                    pop_list.append(idx)
            # Pop files from list from high index to low to maintain index sanity
            for idx in reversed(pop_list):
                files.pop(idx)

    if files :  
        raise Exception ("$HVR_FILE_NAMES contains unexpected list of files.")

    return tbl_map

def get_softdelete_map():
    envvar = os.getenv('HVR_GBQ_MULTIDELETE', '')
    if not envvar:
        return
    keys_per_table = envvar.split(";")
    softdelete_map = {}
    for val in keys_per_table:
        tabend = val.find(':')
        if tabend < 0 or tabend+1>=len(val):
            raise Exception("Value " + envvar + " for HVR_GBQ_MULTIDELETE is invalid. Format is table:keylist;table:keylist")
        elif tabend + 1 < len(val):
            softdelete_map[val[:tabend]] = val[tabend+1:]

    if len(softdelete_map):
        options.softdelete_map = softdelete_map
        trace(2, "Key columns to ignore in multi-delete")
        for key in softdelete_map.keys():
            trace(2, "   " + key + ": " + softdelete_map[key])

def softdelete_table(tablename):
    if tablename in ['bseg','bset','bsem']:
        return True
    if tablename in options.softdelete_map:
        return True
    return False

def skip_keys(tablename):
    if tablename in options.softdelete_map:
        return options.softdelete_map[tablename]
    return 'pageno'

def dataset_create(client, dataset_id):
    is_present = False

    for dataset in client.list_datasets():
        if dataset_id == dataset.dataset_id:
            is_present = True
            break;

    if not is_present:
        dataset_ref = client.dataset(dataset_id)
        dataset = client.create_dataset(bigquery.Dataset(dataset_ref))
        trace(1, "Dataset {0} created".format(dataset_id))


def file_transmit(client, tbl_entry, full_name):
    global file_counter
    
    trace(1, "Uploading file '" + full_name+ "' ... ")

    base_name = tbl_entry[0]

    # create dataset if not present
    dataset_create(client, options.dataset_id)

    dataset_ref = client.dataset(options.dataset_id)
    table_ref = dataset_ref.table(base_name + options.tbl_suffix)

    try:
        with open(full_name, 'rb') as source_file:
            job_config = bigquery.LoadJobConfig()
            job_config.source_format = 'AVRO'
            if options.logical_types:
                job_config.use_avro_logical_types = True
            job = client.load_table_from_file(
                source_file, table_ref, job_config=job_config)

            job.result()  # Waits for job to complete
    except Exception as e:
        details = 'Error occurred uploading ' + full_name + ' to ' + base_name + options.tbl_suffix
        raise Exception(str(e), details)

    file_counter = file_counter + 1

def delete_table(client, table_id):
    dataset_ref = client.dataset(options.dataset_id)

    trace(3, "Delete {0}".format(table_id))
    if bq_new_interface:
        for table in client.list_tables(dataset_ref):
            if table_id == table.table_id:
                table_ref = dataset_ref.table(table_id)
                client.delete_table(table_ref)
                trace(1, "Table {0} deleted".format(table_id))
                break
    else:
        for table in client.list_dataset_tables(dataset_ref):
            if table_id == table.table_id:
                table_ref = dataset_ref.table(table_id)
                client.delete_table(table_ref)
                trace(1, "Table {0} deleted".format(table_id))
                break
    
def process_soft_delete(client, entry):
    base_name = entry[0]
    tbl_name = entry[1]
    cols = entry[2].split(",")
    keys = entry[3].split(",")
    append_mdelete_ignore = ''
    append_mdelete_select = ''

    if options.hvr_op_name:
        if options.hvr_op_name in cols:
            cols.remove(options.hvr_op_name)
        if softdelete_table(tbl_name):
            append_mdelete_ignore = ' AND s.' + options.hvr_op_name + '>0'
            append_mdelete_select = ' AND s.' + options.hvr_op_name + '=0 AND s.' + options.softcol + '=1'

    base_name__b = base_name + options.tbl_suffix

    keys_placeholders = ' AND '.join(["d." + x + '=' + 's.' + x for x in keys])
    cols_placeholders = ','.join([ "d." + x + '=' + 's.' + x for x in cols])

    table_exists = False
    dataset_ref = client.dataset(options.dataset_id)

    if bq_new_interface:
        for table in client.list_tables(dataset_ref):
            if base_name == table.table_id:
                table_exists = True 
    else:
        for table in client.list_dataset_tables(dataset_ref):
            if base_name == table.table_id:
                table_exists = True 

    # update existing rows from burst table
    if table_exists :
        cmd = 'UPDATE {0} d SET {1} FROM {2} s WHERE {3}{4}'.format(
                options.dataset_id + "."+ base_name,
                cols_placeholders,
                options.dataset_id + "."+ base_name__b,
                keys_placeholders,
                append_mdelete_ignore
                )
    
        query_job = client.query(cmd)
        trace(1, "Update rows in base table {0}...".format(base_name))
        trace(2, cmd)
        try:
            query_job.result()
        except Exception as e:
            details = 'Error occurred updating ' + options.dataset_id + "."+ base_name
            raise Exception(str(e), details)

    # process multi-deletes
    if table_exists and append_mdelete_select:
        del_keys = keys
        for skip in skip_keys(tbl_name).split(","):
            if skip in del_keys:
                del_keys.remove(skip)
        del_key_list = ' AND '.join(["d." + x + '=' + 's.' + x for x in del_keys])
        cmd = 'UPDATE {0} d SET d.{1}=s.{1} FROM {2} s WHERE {3}{4}'.format(
                options.dataset_id + "."+ base_name,
                options.softcol,
                options.dataset_id + "."+ base_name__b,
                del_key_list,
                append_mdelete_select
                )
    
        query_job = client.query(cmd)
        trace(1, "Update deleted rows in base table {0} from multi-delete".format(base_name))
        trace(2, cmd)
        try:
            query_job.result()
        except Exception as e:
            details = 'Error occurred updating ' + options.dataset_id + "."+ base_name
            raise Exception(str(e), details)

    # insert new rows
    table_ref = dataset_ref.table(base_name)
    
    job_config = bigquery.QueryJobConfig()
    job_config.destination = table_ref
    job_config.flatten_results = True
    job_config.allow_large_results = True
    job_config.write_disposition = 'WRITE_APPEND'

    trace(1, "Insert new rows into base table {0}...".format(base_name))

    cmd = None
    if table_exists :
        cmd = 'SELECT {0} FROM {1} s WHERE NOT EXISTS (SELECT 1 FROM {2} d WHERE {3}){4}'.format(
                ','.join(x for x in cols),
                options.dataset_id + "."+ base_name__b, 
                options.dataset_id + "."+ base_name, 
                keys_placeholders,
                append_mdelete_ignore)
    else :
        cmd = 'SELECT * FROM {0}'.format(options.dataset_id + "."+ base_name__b)

    trace(2, cmd)
    query_job = client.query(cmd, job_config=job_config)
    query_job.result()

    delete_table(client, base_name__b)
    
def remove_integ_files(file_loc, file_list):
    for name in file_list:
        full_name = os.path.join(file_loc, name)
        if os.path.exists(full_name):
            os.remove(full_name)


def file_loc_process(file_loc, is_refresh):
    tbl_map = table_file_name_map()

    client = bigquery.Client(project=options.project_id)    
    
    for t in tbl_map:
        if options.table_policy == TablePolicy.TABLE_RECREATE and is_refresh:
            # drop base table if not required to preserve it during refresh
            delete_table(client, t[0])

        if options.mode == BqMode.MODE_SOFTDELETE and not is_refresh:
            # drop burst table if it exists
            delete_table(client, t[0] + options.tbl_suffix)

        start = timer() 
        count_before= file_counter

        # upload all files per table 
        trace(3,"Upload {0} {1}".format(len(tbl_map[t]), tbl_map[t]))
        for name in tbl_map[t]:
            full_name = os.path.join(file_loc, name)
            if not os.path.exists(full_name):
                trace(1, "File '" + full_name+ "' doesn't exist, skipping ... ")
                continue
            file_transmit(client, t, full_name)

        if options.mode == BqMode.MODE_SOFTDELETE and not is_refresh:
            if file_counter > count_before:
                process_soft_delete(client, t)

        remove_integ_files(file_loc, tbl_map[t])

        finish_time = timer()
        elapsed_time = finish_time - start
        trace(0, "Spent time {0:.2f} secs to upload {1} file(s) and process changes to table {2}".format(elapsed_time, len(tbl_map[t]), t[1]))

def file_loc_cleanup():
    SECONDS_IN_DAY= 60 * 60 * 24
    file_loc = options.agent_env['HVR_FILE_LOC']
    files = os.listdir(file_loc)
    for name in files:
        full_name = os.path.join(file_loc, name)
        if (name == "." or name == ".." or os.path.isdir(full_name) == True) :
            continue

        if time.time() - os.path.getmtime(full_name) > SECONDS_IN_DAY * 7:
            os.remove(full_name)
     
def process(argv, userarg):

    version_check()
    env_load()
    userargs_parse(userarg)
    env_var_print()

    if ((argv == "refr_write_end" or argv == "integ_end") and
         options.agent_env.get('HVR_FILE_NAMES', '') != ''):
        is_refresh = (argv == "refr_write_end")
        if is_refresh:
            options.tbl_suffix = ''

        file_loc_process(options.agent_env['HVR_FILE_LOC'], is_refresh)

        if (file_counter > 0) :
            print("Successfully transmitted {0:d} file(s)".format(file_counter))

    if (argv == "refr_write_begin"):
        file_loc_cleanup()

if __name__ == "__main__":
    try:
        process(sys.argv[1], sys.argv[4:])
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D03: {0}\n".format(err))
        sys.stderr.flush()
        sys.exit(1);

