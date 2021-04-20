#!python

# $Id: hvrhbaseagent.py 37994 2020-01-28 15:30:53Z christoph $
# Copyright (c) 2000-2020 HVR Software bv

################################################################################
#
#     &{return $DEF{HVR_VER_LONG}}&
#     &{return $DEF{HVR_COPYRIGHT}}&
#
################################################################################
#
# NAME
#     hvrhivegent.py - HVR Hive/HBase integrate agent
#
# SYNOPSIS
#     as agent
#     python hvrhiveagent.py mode loc chn -h -k hvr_keys -t hvr_op_val [-p]
#     python hvrhiveagent.py mode loc chn -h -k hvr_keys -s hvr_is_deleted [-p]
#
# DESCRIPTION
#
#     This script can be used to integrate table rows into HBase tables
#
#     This script operates in 2 modes:
#          - HBase/CSV tables with /TimeKey
#          - HBase/CSV tables with /SoftDelete
#
# OPTIONS
#     -p pre-serve existed HBase tables or CSV tables
#     -h [re]create HBase tables and move data rfrom CSV tables into HBAse ones
#     -k field name for composite key
#     -t /TimeKey mode, hvr_op_val - not used
#     -s /SoftDelete mode, hvr_is_deleted - not used
#
# AGENT OPERATION
#
#     MODES
#     refr_write_begin
#        [Re]create  CSV/HBase tables
#     refr_write_end
#        Convert data from Hive/CSV into Hive/HBase
#        Drop staging tables
#
#     integ_end
#        Convert data from Hive/CSV into Hive/HBase
#        Drop staging tables
#
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#
#     TABLES
#         Integration of HVR tables to Hive/HBase requires
#         a channel with next definitions:
#
#         HBase/CSV timekey mode:
#
#            AgentPlugin /Command="hvrhiveagent.py"
#                        /UserArgument="-h -k hvr_keys -t hvr_op_val"
#            ColumnProperties /Name=hvr_keys
#                             /Extra /IntegrateExpression="{{hvr_key_names :}}"
#                             /Datatype=varchar /Length=100
#            ColumnProperties /Name=hvr_op_val
#                             /Extra /IntegrateExpression={hvr_op}
#                             /Key /TimeKey /Datatype=int
#            ColumnProperties /Name=hvr_integ_tstamp
#                             /Extra /IntegrateExpression={hvr_integ_tstamp}
#                            /Datatype=datetime
#            FileFormat /Csv /QuoteCharacter=""""
#            Integrate /Burst /RenameExpression="{hvr_tbl_name}__b/{hvr_tbl_name}.csv"
#                      /ErrorOnOverwrite
#
#
#         HBase/CSV softdelete mode:
#
#            AgentPlugin /Command="hvrhiveagent.py"
#                        /UserArgument="-h -k hvr_keys -s hvr_is_deleted"
#            ColumnProperties /Name=hvr_keys
#                             /Extra /IntegrateExpression="{{hvr_key_names :}}"
#                             /Datatype=varchar /Length=100
#            ColumnProperties /Name=hvr_integ_tstamp
#                             /Extra /IntegrateExpression={hvr_integ_tstamp}
#                             /Datatype=datetime
#            ColumnProperties /Name=hvr_is_deleted /Extra /SoftDelete
#            FileFormat /Csv /QuoteCharacter=""""
#            Integrate /Burst /RenameExpression="{hvr_tbl_name}__b/{hvr_tbl_name}.csv"
#                      /ErrorOnOverwrite
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_HIVE_HOST         (default: equal to host name from HVR_FILE_LOC)
#        This variable sets a Hive service host
#     HVR_HIVE_PORT         (default: 10000)
#        This variable sets a port number of Hive service
#
#     HVR_HIVE_DATABASE     (default: "default")
#        This variable sets a Hive database name where tables are stored
#
#     HVR_HIVE_USER         (default: taken from HVR_FILE_LOC)
#        This variable sets a Hive user name for Hive connection
#
#     HVR_TBL_NAMES         (required if -h option is present)
#        This  variable contains list of tables stored in Hive/HBase
#
#     HVR_BASE_NAMES        (required)
#        This  variable contains list of tables stored in Hive/CSV
#
#     HVR_COL_NAMES         (required if -h option is present)
#        This  variable contains list of table columns from $HVR_TBL_NAMES
#
#     HVR_COL_NAMES_BASE    (required)
#        This  variable contains list of table columns from $HVR_BASE_NAMES
#
#     HVR_FILE_NAMES        (required)
#        This  variable contains list of files transfered into HDFS.
#        If empty - intergarion process is  omitted
#
# DIAGNOSTICS
################################################################################
import sys
import getopt
import os
import json
import traceback
import impala
import impala.dbapi
from timeit import default_timer as timer
from enum import Enum

class HiveMode(Enum):
    MODE_NOT_DEFINED  = 0
    MODE_HBASE_TIMEKEY =  2
    MODE_HBASE_SOFTDELETE =  3

class TablePolicy(Enum):
    TABLE_PRESERVE = 0
    TABLE_RECREATE = 1

# define global variables;

class Options:
    table_suffix = "__b"

    setup_mode = None
    table_setup = TablePolicy.TABLE_RECREATE
    timekey_mode = None
    hvr_keys = None 
    hive_engine = None
    hvr_hive_trace = 0
    hdfs_path = ''

    # env variables
    hive_port = 10000
    hive_database = "default"
    hive_user = ""
    hive_host = ""

    # Kerberos credentials
    hvr_hive_krb_principal = ""

    # Json file with all env varibales 
    agent_env = None


dbh = None
options = Options()

def trace(msg):
    if options.hvr_hive_trace >= 1:
        print(msg)
        sys.stdout.flush()

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
    options.hive_host = os.getenv('HVR_HIVE_HOST', options.hive_host)
    options.hive_port = int(os.getenv('HVR_HIVE_PORT', options.hive_port))
    options.hive_database = os.getenv('HVR_HIVE_DATABASE', options.hive_database)
    options.hive_user = os.getenv('HVR_HIVE_USER', options.hive_user)
    options.hvr_hive_krb_principal = os.getenv('HVR_HIVE_KRB_PRINCIPAL', options.hvr_hive_krb_principal)
    options.hvr_hive_trace = int(os.getenv('HVR_HIVE_TRACE', options.hvr_hive_trace))
    options.agent_env = load_agent_env()

def env_var_print():
    """
    """
    env = os.environ
    for key, value  in env.iteritems():
        if key.find('HVR') != -1:
            trace(key + " = " + value)

def  userargs_parse(cmdargs):
    try:
        list_args = cmdargs[0].split(" ")
        opts, args = getopt.getopt(list_args,"hk:t:s:px:")
    except getopt.GetoptError:
        raise Exception("Couldn't parse command line arguments")

    for opt, arg in opts:
        if opt == '-k':
            options.hvr_keys = arg
        elif opt == '-t':
            options.setup_mode = HiveMode.MODE_HBASE_TIMEKEY
            options.timekey_mode = arg
        elif opt == '-s':
            options.setup_mode = HiveMode.MODE_HBASE_SOFTDELETE
        elif opt == '-p':
            options.table_setup = TablePolicy.TABLE_PRESERVE
        elif opt == '-x':
            options.hive_engine = arg

def url_host_parse(address):
    semi_index = address.find(':')
    port = None
    host = address
    if semi_index != -1:
        port = address[semi_index + 1:]
        host = address[0 : semi_index]

    return host, port

"""
parsed = urlsplit("hdfs://user:{!ewewewewew!}@hist:80/path/subpath")
print parsed
print 'scheme  :', parsed.scheme
print 'netloc  :', parsed.netloc
print 'path    :', parsed.path
print 'query   :', parsed.query
print 'fragment:', parsed.fragment
print 'username:', parsed.username
print 'password:', parsed.password
print 'hostname:', parsed.hostname, '(netloc in lower case)'
print 'port    :', parsed.port
"""
def url_parse(raw_url):
    """
    $HVR_FILE_LOC support such formats:
    hdfs://host[:port]/path
    hdfs://user@host[:port]/path
    hdfs://user:!{encrypted_ticket_cache}!@host[:port]/path
    """
    url = raw_url
    user = None
    host = None
    port = None
    path = None
    encrypted_ticket_cache = None

    start = url.find("://")
    if start == -1:
        raise Exception("Parse URL Error: expected //")

    start = start + 3

    user_index = url.find( "@", start);

    if user_index == -1:
        path_index = url.find("/", start);

        if path_index == -1 :
            raise Exception("Parse URL Error: '/' is expected")

        path = url[path_index:]

        host = url[start: path_index ]
        host, port = url_host_parse(host)
    else:
        path_index = url.find("/", user_index + 1);

        if path_index == -1 :
            raise Exception("Parse URL Error: '/' is expected")

        path = url[path_index:]

        host = url[user_index + 1 : path_index]
        host, port = url_host_parse(host)

        user_url = url[start: user_index]

        semi_index = user_url.find(':');
        if semi_index == -1:
            user = user_url
        else:
            left = user_url.find( '!{', semi_index + 1)

            if left == -1:
                raise Exception( "Parse URL Error: Expected \'!{\' after \':\'")

            right = user_url.find('}!', left + 2)

            if right == -1:
                raise Exception( "Parse URL Error: Missed right \'}!\'")

            encrypted_ticket_cache = user_url[left + 2 : right]
            user = user_url[0 : left - 2]

    return host, port, path, user, encrypted_ticket_cache;

def hive_connect():
    global dbh

    host, port, options.hdfs_path, user, encrypted  = url_parse (options.agent_env['HVR_FILE_LOC'])

    if user != None:
        options.hive_user = user

    if options.hive_host == '':
        options.hive_host = host

    if options.hive_host is None or options.hive_host == '':
       raise Exception("HVR_HIVE_HOST variable is not defined") 

    if options.hdfs_path == '':
       raise Exception("HDFS Path missed in URL")

    if (encrypted is None or encrypted == '') and options.hvr_hive_krb_principal == '':
        dbh = impala.dbapi.connect(host=options.hive_host,  
                                port=options.hive_port, 
                                auth_mechanism = 'PLAIN',
                                user=options.hive_user, 
                                database=options.hive_database)
    else:
        service_name = options.hive_user 
        if options.hvr_hive_krb_principal != '':
            service_name = options.hvr_hive_krb_principal

        dbh = impala.dbapi.connect(host=options.hive_host,  
                                port=options.hive_port, 
                                auth_mechanism = 'GSSAPI',
                                kerberos_service_name = service_name,
                                #user=hive_user, 
                                database=options.hive_database)
           
def sql_stmt_exec(db, cmd, comment):
    trace("================================")
    trace(comment)
    trace(cmd)

    start = timer() 
    cursor = db.cursor()
    if options.hive_engine is not None:
       cmd_engine = "set hive.execution.engine=" + options.hive_engine
       cursor.execute(cmd_engine)  

    cursor.execute(cmd)
    elapsed = timer() - start
    trace("Elapsed {0:.2f} secs".format(elapsed))
    trace("================================")

def table_name_normalize(name):
    index = name.find(".")
    schema = ''
    if index != -1 :
       schema = name [:index]

    return schema, name[index + 1:]

def base_tables_recreate(policy, suffix):
    global dbh

    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_tbl_names_base = options.agent_env['HVR_BASE_NAMES'].split(":")
    hvr_col_names_base = options.agent_env['HVR_COL_NAMES_BASE'].split(":")

    # re-create base CSV tables
    for name in zip(hvr_tbl_names_base, hvr_tbl_names):
        schema, base_name = table_name_normalize(name[0])
        base_tbl_name_b = base_name + suffix
        tbl_name_b = name[1] + suffix
        cmd = None
        
        if (policy == TablePolicy.TABLE_RECREATE):
            cmd = "DROP TABLE IF EXISTS {0}".format(base_tbl_name_b)
            sql_stmt_exec(dbh, cmd, "Drop table {0}".format(base_tbl_name_b))
            
        cols = hvr_col_names_base[0].split(",")
        hvr_col_names_base.pop(0)
       
        col_names_def = ""
        comma = ""
       
        for value in cols:
            col_names_def = col_names_def + comma + value + " string"
            comma = ","
        
        cmd = ( "CREATE TABLE IF NOT EXISTS " + base_tbl_name_b + "(" + col_names_def + ") "
                "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' "
                "with serdeproperties ( "
                "\"separatorChar\" = \",\", "
                "\"quoteChar\"= \"\\\"\" ) "
                "STORED AS TEXTFILE "
                "LOCATION '" + options.hdfs_path + "/" + tbl_name_b + "'"
               )
        
        sql_stmt_exec(dbh, cmd, "Create table " + base_tbl_name_b)


def hbase_table_recreate(policy):
    global dbh

    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_col_names = options.agent_env['HVR_COL_NAMES'].split(":")
    hvr_tbl_keys = options.agent_env['HVR_TBL_KEYS'].split(":")

    # re-create HBase tables
    for name in hvr_tbl_names:
        if (policy == TablePolicy.TABLE_RECREATE):
            cmd = "DROP TABLE IF EXISTS {0}".format(name)
            sql_stmt_exec(dbh, cmd, "Drop table {0}".format(name))

        cols = hvr_col_names[0].split(",")
        hvr_col_names.pop(0)

        col_names_def = ",".join(value + " string" for value in cols)
        hbase_mapping_def = ",".join(value + ":" + value  for value in cols)

        keys = hvr_tbl_keys[0].split (",")
        hvr_tbl_keys.pop(0)
        
        keys_def = ",".join(key + ":string" for key in keys)

        cmd = ("CREATE TABLE IF NOT EXISTS " + name + "(" +options.hvr_keys + " struct<"+keys_def+">, " + col_names_def+ ") "
               "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' "
               "WITH SERDEPROPERTIES ("
               "\"hbase.columns.mapping\" = \":key," + hbase_mapping_def+ "\")"
               )
               
        sql_stmt_exec(dbh, cmd, "Create table " +name)

def convert_data_into_hbase():
    global dbh

    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_col_names_base = options.agent_env['HVR_COL_NAMES_BASE'].split(":")
    hvr_tbl_names_base = options.agent_env['HVR_BASE_NAMES'].split(":")
    hvr_tbl_keys_base = options.agent_env['HVR_TBL_KEYS_BASE'].split(":")


    for base_tbl_name in hvr_tbl_names_base:
        tbl_name  = hvr_tbl_names[0]
        hvr_tbl_names.pop(0)

        schema, base_name = table_name_normalize(base_tbl_name)

        base_tbl_name_b = base_name + options.table_suffix
        col_names_base = hvr_col_names_base[0].split(",")
        hvr_col_names_base.pop(0)

        columns = ",".join(column for column in col_names_base)

        keys = hvr_tbl_keys_base[0].split (",")
        hvr_tbl_keys_base.pop(0)

        keys_def = ",".join("'" + key + "'," + key for key in keys)

        cmd  = (
                "INSERT OVERWRITE TABLE " + tbl_name + " "
                "SELECT named_struct(" + keys_def + ")," + columns + " "
                "FROM " + base_tbl_name_b
               )
        sql_stmt_exec(dbh, cmd, "Transfer data from "+ base_tbl_name_b + " into HBase " + tbl_name)

        cmd = "DROP TABLE IF EXISTS "+ base_tbl_name_b
        sql_stmt_exec(dbh, cmd, "Drop table "+ base_tbl_name_b)

def convert_data_into_hbase_timekey():
    trace("HBASE process Timekey..")
    convert_data_into_hbase()    

def convert_data_into_hbase_softdelete():
    trace("HBASE process Softdelete..")
    convert_data_into_hbase()    

def main_setup_hbase_timekey():
    trace("hbase timekey setup")
    hive_connect()
    base_tables_recreate(TablePolicy.TABLE_RECREATE, options.table_suffix)
    hbase_table_recreate(options.table_setup)

def main_setup_hbase_softdelete():
    trace("base softdelete setup")
    hive_connect()
    base_tables_recreate(TablePolicy.TABLE_RECREATE, options.table_suffix)
    hbase_table_recreate(options.table_setup)

def main (mode, userarg):
    env_load()
    userargs_parse(userarg)
    if options.hvr_hive_trace > 1:
        env_var_print()

    # re-create tables Hive/HBase if required
    if mode == "refr_write_begin" :
        if options.setup_mode == HiveMode.MODE_HBASE_TIMEKEY:
            main_setup_hbase_timekey()
        elif options.setup_mode == HiveMode.MODE_HBASE_SOFTDELETE:
            main_setup_hbase_softdelete()
        else:
            raise Exception("Invalid agent working mode {0:d}. Check /UserArgs paramater".format(options.setup_mode))

    # move data from CSV based tables into HBase ones
    if mode == "refr_write_end":
        if options.setup_mode == HiveMode.MODE_HBASE_TIMEKEY:
            hive_connect()
            convert_data_into_hbase_timekey()
        elif options.setup_mode == HiveMode.MODE_HBASE_SOFTDELETE:
            hive_connect()
            convert_data_into_hbase_softdelete()
        else:
            raise Exception("Invalid agent working mode {0:d}. Check /UserArgs paramater".format(options.setup_mode))

    # final data convert
    if mode ==  "integ_end":
        if options.setup_mode == HiveMode.MODE_HBASE_TIMEKEY and options.agent_env.get('HVR_FILE_NAMES', '') != "" :
            hive_connect()
            base_tables_recreate(TablePolicy.TABLE_PRESERVE, options.table_suffix)
            convert_data_into_hbase_timekey()
        elif options.setup_mode == HiveMode.MODE_HBASE_SOFTDELETE and options.agent_env.get('HVR_FILE_NAMES', '') != "" :
            hive_connect()
            base_tables_recreate(TablePolicy.TABLE_PRESERVE, options.table_suffix)
            convert_data_into_hbase_softdelete()


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[4:])
        sys.stdout.flush()
        sys.exit(0) 
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush()
        sys.stderr.write("F_JX0D00: {0}\n".format(err))
        trace("{0}\n".format(tb))
        sys.stderr.flush()
        sys.exit(1)
