#!python


# HVR 5.7.5/6 (linux_glibc2.12-x64-64bit) hvrcassagent.py@22733 2018-07-06
# Copyright (c) 2000-2016 HVR Software bv

################################################################################
#
#     HVR 5.7.5/6 (linux_glibc2.12-x64-64bit)
#     Copyright (c) 2000-2021 HVR Software bv
#
################################################################################
# 
# NAME
#     hvrcassagent.py - HVR Apache/Cassandra integrate agent
#
# SYNOPSIS
#     as agent
#     python hvrcassagent.py mode loc chn agrs
#   
# DESCRIPTION
#
#     This script can be used to send table rows via Apache Kafka message broker
#
#
# OPTIONS
#
# AGENT OPERATION
#
#     MODES
#
#     refr_write_begin
#        Creates all missed tables in Apache Cassandra if option -p not set
#
#     refr_write_end
#        Convert each row in data files into Apache Cassandra SQL statements 
#
#     integ_end
#        Convert each row in data files into Apache Cassandra SQL statements
#
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#          
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_CASSANDRA_KEYSPACE   (required)
#        This variable sets a Apache Cassandra keyspace
#      
#     HVR_CASSANDRA_HOST    (required)
#        This variable sets a Apache Cassandra host name
#
#     HVR_CASSANDRA_PORT     (optional, default: 9042)
#        This variable sets a Apache Kafka port
#
#     HVR_CASSANDRA_USER    (optional)
#        This variable sets a Apache Cassandra user name
#
#     HVR_CASSANDRA_PWD     (optional)
#        This variable sets a Apache Cassandar user password
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
#     HVR_COL_NAMES         (required)       
#        This  variable contains list of columns
#  
#     HVR_SCHEMA            (optional)       
#        This  variable sets schema name
#    
#     HVR_FILE_NAMES        (required)
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
import csv
from timeit import default_timer as timer
from enum import Enum

import cassandra
from cassandra.cluster import Cluster
from cassandra.io.asyncorereactor import AsyncoreConnection
from cassandra.auth import PlainTextAuthProvider

from six.moves import queue
from greplin import scales

class CassMode(Enum):
    MODE_NOT_DEFINED = 0
    MODE_PLAIN  = 1
    MODE_TIMEKEY =  2
    MODE_SOFTDELETE =  3

class TablePolicy(Enum):
    TABLE_PRESERVE = 0
    TABLE_RECREATE = 1

class Options:
    port = 9042
    hosts = ''
    trace = 0
    keyspace = ''
    table_policy = TablePolicy.TABLE_RECREATE
    user_name = None
    user_pwd = None
    mode = CassMode.MODE_PLAIN
    auto_create = False
    agent_env = None

QUEUE_SIZE = 1001
file_counter = 0
options = Options()

##### Support function #########################################################

def trace(level, msg):
    if options.trace >= level:
        print(msg)

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

    options.trace = int(os.getenv('HVR_CASSANDRA_TRACE', options.trace))
    options.port = os.getenv('HVR_CASSANDRA_PORT', options.port)
    options.hosts = os.getenv('HVR_CASSANDRA_HOST', options.hosts)
    options.keyspace = os.getenv('HVR_CASSANDRA_KEYSPACE', options.keyspace)
    options.user_name = os.getenv('HVR_CASSANDRA_USER', options.user_name)
    options.user_pwd = os.getenv('HVR_CASSANDRA_PWD', options.user_pwd)

    if (options.hosts == '' or options.keyspace == '') :
        raise Exception("At least HVR_CASSANDRA_KEYSPACE and HVR_CASSANDRA_HOST enviroment variables must be defined")

    options.hosts = options.hosts.split(',')
    options.agent_env = load_agent_env()

    return options

def get_auth_provider(options):

    auth_provider = None
    if options.user_name and options.user_pwd:
        auth_provider = PlainTextAuthProvider(username=options.user_name, 
        password=options.user_pwd)

    return auth_provider  
 
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
    
    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_base_names = options.agent_env['HVR_BASE_NAMES'].split(":")
    hvr_col_names = options.agent_env['HVR_COL_NAMES'].split(":")
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
                if name[-4:] == ".csv":
                    name = name[:-4]
                if name == item[1]:
                    tbl_map[item].append(f)
                    pop_list.append(idx)
            # Pop files from list from high index to low to maintain index sanity
            for idx in reversed(pop_list):
                files.pop(idx)

    if files :
        raise Exception ("$HVR_FILE_NAMES contains unexpected list of files.")

    return tbl_map


def setup(options, tables_info):

    cluster = Cluster(options.hosts, schema_metadata_enabled=False, 
                       token_metadata_enabled=False,
                       auth_provider=get_auth_provider(options))
    try:
        session = cluster.connect()

        if options.auto_create == True:
            trace(1, "Creating keyspace...")
            try:
                session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS %s
                    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
                    """ % options.keyspace)
    
            except cassandra.AlreadyExists:
                trace(1, "Keyspace already exists")

        trace(1,"Setting keyspace...")
        session.set_keyspace(options.keyspace)

        for t in tables_info:
            if options.table_policy == TablePolicy.TABLE_RECREATE:
                drop_table(session, t)
                
            create_table(session, t)

    finally:
        cluster.shutdown()


def drop_table(session, entry):

        drop_table_query = "DROP TABLE IF EXISTS {0}"
        schema_name, base_name = table_name_normalize(entry[0])
        
        trace(1, "Dropping table... " + base_name)

        session.execute(drop_table_query.format(base_name))
        

def create_table(session, entry):

        create_table_query = """
            CREATE TABLE IF NOT EXISTS {0} (
        """
        
        base_name = entry[0]
        tbl_name = entry[1]
        cols = entry[2].split(",")
        keys = entry[3].split(",")
        
        if options.mode == CassMode.MODE_PLAIN:
            cols.remove('hvr_is_deleted')
        
        for col in cols:
            create_table_query += "{0} {1},\n".format(col, 'text')

        thekey = ','.join([str(x) for x in keys]) 
   
        create_table_query += "PRIMARY KEY ({0}))".format(thekey)

        trace(1, "Creating table... " + base_name)
        
        try:
            session.execute(create_table_query.format(base_name))
        except cassandra.AlreadyExists:
            trace(1, "Table already exists.")

def file_transmit(session, futures, tbl_entry, full_name):

    global file_counter
    
    trace(1, "Uploading file '" + full_name+ "' ... ")

    cols = tbl_entry[2].split(",")
    keys = tbl_entry[3].split(",")
    fieldnames = tbl_entry[2].split(",")
    
    # Remove extra service column
    if options.mode == CassMode.MODE_PLAIN:
        cols.remove('hvr_is_deleted')
    
    col_names = ','.join([str(x) for x in cols])
    
    cols_placeholders = ','.join(['%s' for x in cols]) 
    keys_placeholders = ' AND '.join([x +'=%s' for x in keys])
    
    base_name = tbl_entry[0]
    
    # statistics information
    start = timer() 
    row_count = 0
    prepare_query_time = 0.0
    read_file = 0.0
    row_size = 0
    
    with open(full_name) as csvfile:

        reader = csv.DictReader(csvfile, fieldnames=fieldnames)
        start_read = timer()
        for row in reader:
            #Sending a single row
            file_time_s = timer()
            read_file += (file_time_s - start_read)     
            trace(3, "Sending row #{0}".format(row_count + 1))
            
            query = None
            values = []
            
            if options.mode == CassMode.MODE_PLAIN:
                if int(row['hvr_is_deleted']) == 1:
                    query = "DELETE FROM {0} WHERE {1}".format(base_name, keys_placeholders)
                    [values.append(str(row[k])) for k in keys]
                    for k in keys:
                        row_size += len(row[k])
                else:
                    query = "INSERT INTO {0} ({1}) VALUES ({2})".format(base_name, 
                                                   col_names, cols_placeholders)
                    [values.append(str(row[c])) for c in cols]
            else:
                query = "INSERT INTO {0} ({1}) VALUES ({2})".format(base_name, 
                                               col_names, cols_placeholders)
                [values.append(str(row[c])) for c in cols]
            
                for c in cols:
                    row_size += len(row[c])
                
            prepare_query_time += (timer() - file_time_s) 
            
            if row_count > 0 and row_count % (QUEUE_SIZE - 1) == 0:
                # clear the existing queue
                while True:
                    cass_e = timer()
                    try:
                        futures.get_nowait().result()
                    except queue.Empty:
                        break
            
            future = session.execute_async(query, values);
            futures.put_nowait(future)
            
            row_count = row_count + 1
            start_read = timer()
        
    # wait to  finalize of messages transmitting process
    while True:
        try:
            futures.get_nowait().result()
        except queue.Empty:
            break

    finish_time = timer()
    elapsed_time = finish_time - start
    cass_time = elapsed_time - prepare_query_time - read_file

    print("Uploading '{0}' is finished".format(full_name))
    print("Transmitted {0} rows".format(row_count))
    print("Spent time {0:.2f} secs".format(elapsed_time))
    
    trace(1,"Read file {0:.2f} secs".format(read_file))  
    trace(1,"Prepare queries {0:.2f} secs".format(prepare_query_time))
    trace(1,"Put data into Cassandra {0:.2f} secs".format(cass_time))
    
    trace(1, "Speed {0:.2f} rows/secs".format((row_count/elapsed_time)))
    trace(1, "Cass speed {0:.2f} rows/secs".format((row_count/cass_time)))
    trace(1, "Avg row size {0:.0f} bytes".format((row_size/row_count)))
    
    if options.trace >= 2 :
        stats = scales.getStats()['cassandra']
        print("Connection errors: %d" % stats['connection_errors'])
        print("Write timeouts: %d" % stats['write_timeouts'])
        print("Read timeouts: %d" % stats['read_timeouts'])
        print("Unavailables: %d" % stats['unavailables'])
        print("Other errors: %d" % stats['other_errors'])
        print("Retries: %d" % stats['retries'])
    
        request_timer = stats['request_timer']
        print("Request latencies:")
        print("  count: %d" % request_timer['count'])
        print("  min: %0.4fs" % request_timer['min'])
        print("  max: %0.4fs" % request_timer['max'])
        print("  mean: %0.4fs" % request_timer['mean'])
        print("  stddev: %0.4fs" % request_timer['stddev'])
        print("  median: %0.4fs" % request_timer['median'])
        print("  75th: %0.4fs" %  request_timer['75percentile'])
        print("  95th: %0.4fs" % request_timer['95percentile'])
        print("  98th: %0.4fs" % request_timer['98percentile'])
        print("  99th: %0.4fs" % request_timer['99percentile'])
        print("  99.9th: %0.4fs" % request_timer['999percentile'])
    
    # remove successfully transmitted file
    os.remove(full_name)
    file_counter = file_counter + 1

def file_loc_process(file_loc):
    
    tbl_map = table_file_name_map()
    
    is_metrics_enabled = False
    
    if options.trace >=2 :
        is_metrics_enabled = True
        
    kwargs = {'metrics_enabled': is_metrics_enabled,
              'connection_class': AsyncoreConnection }

    #if options.protocol_version:
    #    kwargs['protocol_version'] = options.protocol_version
    
    cluster = Cluster(options.hosts, auth_provider=get_auth_provider(options),
                        **kwargs)
    session = cluster.connect(options.keyspace)
    futures = queue.Queue(maxsize=QUEUE_SIZE)

    try:
        for t in tbl_map:
            for name in tbl_map[t]:
                full_name = os.path.join(file_loc, name)
                file_transmit(session, futures, t, full_name)
    finally:        
        cluster.shutdown()


def file_loc_cleanup():
    file_loc = options.agent_env['HVR_FILE_LOC']
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

    for opt, arg in opts:
        if opt == '-p':
            options.table_policy = TablePolicy.TABLE_PRESERVE
        elif opt == '-s':
            options.mode = CassMode.MODE_SOFTDELETE
        elif opt == '-t':
            options.mode = CassMode.MODE_TIMEKEY
        elif opt == '-a':
            options.auto_create = True


def process(argv, userarg):

    env_load()
    userargs_parse(userarg)
    env_var_print()

    if ((argv == "refr_write_end" or argv == "integ_end") and
        options.agent_env.get('HVR_FILE_NAMES','') != ''):
        
        file_loc_process(options.agent_env['HVR_FILE_LOC'])

        if (file_counter > 0) :
            print("Successfully transmitted {0:d} file(s)".format(file_counter))

    if (argv == "refr_write_begin"):
          
        tbl_map = table_file_name_map()
        setup(options, tbl_map)
    
        file_loc_cleanup()

if __name__ == "__main__":
    try:
        process(sys.argv[1], sys.argv[4:])
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D03: {0}\n".format(err))
        trace(3,"{0}\n".format(tb))
        sys.stdout.flush() 
        sys.stderr.flush()
        sys.exit(1);

