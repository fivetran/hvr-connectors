#!python

################################################################################
################################################################################
# 
# NAME
#     hvrdatabricksagent.py - HVR Databricks agent plugin script 
#
# SYNOPSIS
#     as agent
#     python hvrdatabricks.py mode loc chn agrs
#   
# DESCRIPTION
#     This script can be used to send table rows into Databricks delta tables
#
# OPTIONS
#     -c <context> - name of the context used in the refresh
#     -d <name>    - name of the SoftDelete column, default is 'is_deleted'
#     -D <name>    - name of the SoftDelete column, default is 'is_deleted'
#                    if specified with "-D", the target table has this column
#     -E <envvar>  - pass an environment variable
#     -i <collist> - if '-r', these (Extra) columns are not in target
#     -o <name>    - name of the hvr_op column, default is 'op_type'
#     -O <name>    - name of the hvr_op column, default is 'op_type'
#                    if specified with "-O", the target table has this column
#     -p - preserve target data during timekey refresh
#     -r - create (re-create) tables during refresh    
#     -t - target is timekey
#     -w - use wasb syntax for files in adls file system
#     -y - skip filestore operations
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
#     HVR_DBRK_DSN               (required, unless overridden by HVR_DBRK_CONNECT_STRING)
#        Provides the DSN for the connection to Databricks
#
#     HVR_DBRK_CONNECT_STRING    (optional)
#        If specified will be used as the connect string for the connection
#        to Databricks, if set HVR_DBRK_DSN will be ignored
#
#     HVR_DBRK_CONNECT_TIMEOUT   (optional)
#        The time, in seconds, to set the timeout in the ODBC connect call.
#
#     HVR_DBRK_DATABASE          (optional)
#        If set the script will apply all changes to this database. 
#
#     HVR_DBRK_FILESTORE_ID      (optional)
#        The access ID for the S3 cloud storage
#
#     HVR_DBRK_FILESTORE_KEY     (optional)
#        The access key for the S3 or BLOB storage
#
#     HVR_DBRK_FILESTORE_REGION  (optional)
#        The region of the S3 cloud storage
#
#     HVR_DBRK_FILESTORE_OPS     (optional)
#        Valid values:
#            'check', 'delete', 'none'
#        By default the script accesses the filestore to:
#            - 'check' that the files passed in HVR_FILE_NAMES are there.  If the connector
#              was interrupted (suspend integrate), then the files might not exist when
#              the connector is restarted
#            - 'check' that only the passed in HVR_FILE_NAMES are in the file location to
#              evaluate whether the burst table can be created as an unmanaged table
#            - 'delete' the files after the table has been integrated
#
#     HVR_DBRK_TIMEKEY           (optional)
#        If set to 'ON', changes are appended to target
#
#     HVR_DBRK_EXTERNAL_LOC      (optional)
#        If specified, and refresh with create table, create an external Delta table.
#
#     HVR_DBRK_FILE_EXPR         (optional)
#        The Integrate /RenameExpression if set
#
#     HVR_DBRK_FILEFORMAT        (optional)
#        By default the script assumes CSV format.
#
#     HVR_DBRK_DELIMITER         (optional)
#        The value of /FieldSeparator from the FileFormat action, if set
#
#     HVR_DBRK_LINE_SEPARATOR    (optional)
#        The value of /LineSeparator from the FileFormat action, if set
#
#     HVR_DBRK_LOAD_BURST_DELAY  (optional)
#        If set, the number of seconds that the script will wait before loading
#        the burst table after creating it.
#
#     HVR_DBRK_UNMANAGED_BURST   (optional)
#        If not set, the script will determine whether it can use an unmanaged table for 
#        the burst table.  If set to 'ON', use an unmanaged table for the burst table
#        with LOCATION pointing to integrate cycle files.  If set to any other value,
#        create a managed for the burst table and load it from the integrate files.
#
#     HVR_DBRK_HVRCONNECT        (required for '-r' option)
#        The connection string for connecting to the HVR repository, in base64. 
#        This is the same string that is to run hvrinit or hvrrefresh from the
#        command line - the 'hubdb' part of the runtime options for many HVR 
#        commands.   
#
#     HVR_DBRK_TBLPROPERTIES     (optional)
#        By default the connector sets the following table properties during refresh:
#            autoOptimize.optimizeWrite = true, autoOptimize.autoCompact = true
#        If this Environment variable is set, the connector will replace the default
#        table properties settings with the configured table properties.  Note that
#        to disable the connector setting any table properties, set this Environment
#        variable to '' or "".
#
#     HVR_DBRK_MULTIDELETE  (advanced,optional, default:'')
#        The agent supports multi-delete operations that are outputted by the
#        SAPXForm module.  A multi-delete operation has an incomplete key.  For
#        the agent to implement the multi-delete using SQL it needs to know 
#        which operations are multi-delete and which columns are missing.
#        If the "-m" option is specified then by default the hvrbigqueryagent
#        will process all deletes for tables BSEG, BSET & BSEM as soft deletes,
#        defining the where clause with all the key columns except PAGENO.
#        The HVR_GBQ_MULTIDELETE env var allows the user to configure this
#        behavior for additional tables if necessary.  
#           <tablename>:<column list>;<tablename>:<column list>; . . 
#        For example:
#           TAB1:COL77;TAB2:COL33
#
#     HVR_DBRK_TRACE             (optional)
#        Enables tracing of the AgentPlugin
#           1 - logs each step being performed
#           2 - added detail such as the exact SQL being run
#           3 - added logging of runtime environment
#           3 - trace decoding & parsing of HVR connection
#
# CHANGE_LOG
#     11/19/2020 RLR: Initial release
#     12/04/2020 RLR: Added support for Azure hosted clusters
#     12/07/2020 RLR: Fixed script to work with an S3 location with /Directory set
#     12/08/2020 RLR: Fixed script to work with an Azure Blob location with /Directory set
#     01/13/2021 RLR: Added the Agent tag to the ODBC connection string
#     02/25/2021 RLR: Fixed typecasting of decimal by removing (p,s) if in type definition
#     03/03/2021 RLR: Added support for TimeKey target
#     03/09/2021 RLR: Added support for an op_type column named something other than 'op_type'
#                     and where the column op_type column is in the target table
#     03/10/2021 RLR: Fixed the configuration for replicate copies - the two required actions are:
#                         ColumnProperties /Name=op_type /Extra /IntegrateExpression={hvr_op} /Datatype=integer
#                         ColumnProperties /Name=is_deleted /Extra /SoftDelete /Datatype=integer
#                     Support for Replicate copy, TimeKey, SoftDelete
#     03/12/2021 RLR: Support a CSV field separator other than comma (which is the default).
#     03/17/2021 RLR: Make the compare for types case-insensitive
#                     Add tracing for filestore operations, add option to disable filestore ops
#     03/26/2021 RLR: Add support for multi-delete operation
#     04/12/2021 RLR: Fixed a case sensitive bug in comparing columns for types
#     04/14/2021 RLR: Added logic to create the target table on refresh
#     04/28/2021 RLR: Fixed a bug in files_in_s3
#     04/28/2021 RLR: Fixed the mapping of decimal type data types in create table
#                     Fixed the casting of decimal type data types
#                     Fixed tracing in get_s3_handles
#     05/11/2021 RLR: Added ability to create a table with a LOCATION
#     05/11/2021 RLR: Added HVR_DBRK_LINE_SEPARATOR to define the line separator
#     05/12/2021 RLR: Set Auto Optimize on tables after they are created
#     05/12/2021 RLR: If the folder that the files are in includes the table name,
#                     create the burst table as en external table pointing to the
#                     folder where the files are.
#     05/13/2021 RLR: Changes/fixes to using a unmanaged table for the burst table
#     05/14/2021 RLR: Fixed tracing in get_s3_handles
#     05/17/2021 RLR: Tested, and fixed issues with, unmanaged table logic with AWS hosted databricks
#     05/19/2021 RLR: Added support for avro & parquet file formats
#                     Allow json files - not tested
#                     Changed HVR_DBRK_MANAGED_BURST to HVR_DBRK_UNMANAGED_BURST and
#                     fixed tracing verbage.
#     05/26/2021 RLR: Throw error if create-table-on-refresh set and running under python2
#                     Added print_raw method
#     05/28/2021 RLR: Get the ODBC connect timeout from an Environment variable
#     06/01/2021 RLR: Alter table set tblproperties during refresh if not set
#     06/02/2021 RLR: Support ADLS gen2
#     06/11/2021 RLR: Changes to support create/recreate Refresh with HVR 6
#     06/14/2021 RLR: Changes to support create/recreate Refresh with HVR 6
#     06/15/2021 RLR: Add sleep after burst table has been created
#                     Minor fixes to unmanaged burst table logic
#     06/16/2021 RLR: Add option to specify database
#
#     06/18/2021 RLR v1.0  Add versioning
#     06/30/2021 RLR v1.1  Fix table_file_name_map for non-default /RenameExpression
#     07/01/2021 RLR v1.2  Escape quote all column names to support column name like class#
#     07/02/2021 RLR v1.3  Issue plutiple COPY INTO commands if # files > 1000
#     07/09/2021 RLR v1.4  Fixed a bug in create table processing ColumnProperties
#                          DatatypeMatch where it would only apply to first column that matched
#     07/09/2021 RLR v1.5  Fixed create table column ordering - respect source column order
#     07/09/2021 RLR v1.6  Provide an Environment variable for customizing table properties
#     07/14/2021 RLR v1.7  Added support for /DatatypeMatch="number[prec=0 && scale=0]" so that
#                          a mathc can be defined for Oracle NUMBER w/out prec or scale
#     07/20/2021 RLR v1.8  Added support for sliced refresh if generated by the hvrslicedrefresh.py
#                          script WITH the '-s' option
#     07/21/2021 RLR v1.9  Fixed a bug processing ColumnProperties actions for create table
#     07/22/2021 RLR v1.10 Use ABFS driver file system when accessing files in ADLS.  Added 
#                          an option to use WASB if desired
#     07/23/2021 RLR v1.11 Fixed throwing "F_JX0D03: list assignment index out of range" checking Python version
#     07/23/2021 RLR v1.12 Use OAuth authentication by default to list and access files in ADLS gen 2
#     07/27/2021 RLR v1.13 Fixed throwing "F_JX0D03: list assignment index out of range" processing target columns
#     07/27/2021 RLR v1.14 Fixed throwing 'F_JX0D03: delete_file() takes 2 positional arguments but # were given'
#     07/28/2021 RLR v1.15 Process ColumnProperties and TableProperties where chn_name='*'
#     07/30/2021 RLR v1.16 Fixed resilience of merge command - only insert ot update if hvr_op != 0
#     07/30/2021 RLR v1.17 Added -E & -i options for refreshing two targets with the same job
#     08/04/2021 RLR v1.18 Fixed (re)create of target table appending rows
#     08/06/2021 RLR v1.19 Fixed regression from v1.18 where create table failed on a managed target table
#                          Added finer controls over what file operations are executed
#
################################################################################
import sys
import traceback
import getopt
import os
import re
import time
import uuid
import subprocess
import json
import pyodbc
from timeit import default_timer as timer

VERSION = "1.18"

class FileStore:
    AWS_BUCKET  = 0
    AZURE_BLOB  = 1
    ADLS_G2     = 2

class FileOps:
    NONE        = 0
    CHECK       = 1
    DELETE      = 2
    ALL         = 3

class RefreshOptions:
    num_slices = None
    slice_num = None
    slices_done = 0
    job_name = None
    source_loc = None
    lock_file = None
    lock_fd = -1
    done_file = None

class Options:
    hvr_home = ''
    hvr_config = ''
    hvr_6 = False
    hub = ''
    mode = ''
    channel = ''
    location = ''
    locgroup = ''
    agent_env = {}
    trace = 0
    filestore = FileStore.AWS_BUCKET
    dsn = None
    connect_string = None
    connect_timeout = 0
    database = None
    channel_export = ''
    hvr_opts = []
    url = ''
    resource = ''
    container = ''
    directory = ''
    folder = ''
    access_id = ''
    secret_key = ''
    region = ''
    file_format = 'csv'
    file_pattern = []
    tblname_in_file_pattern = -1
    delimiter = ','
    line_separator = ''
    load_burst_delay = None
    unmanaged_burst = 'Auto'
    burst_table_set_of_files = False
    external_loc = ''
    use_wasb = False
    auto_optimize = True
    multidelete_map = {}
    context = ''
    optype = 'op_type'
    no_optype_on_target = True
    isdeleted = 'is_deleted'
    no_isdeleted_on_target = True
    ignore_columns = []
    target_is_timekey = False
    truncate_target_on_refresh = True
    filestore_ops = FileOps.ALL
    recreate_tables_on_refresh = False
    set_tblproperties = 'delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true'

class Connections:
    odbc = None
    cursor = None
    s3_client = None
    s3_resource = None
    azstore_service = None

file_counter = 0

options = Options()
refresh_options = RefreshOptions()

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

    python3 = sys.version_info.major == 3
    
def check_hvr6():
    loginpath = os.path.join(options.hvr_home, "bin")
    loginpath = os.path.join(loginpath, "hvrlogin")
    if os.path.exists(loginpath):
        return True
    return False

def print_raw(_msg, tgt= None):
    _msg= re.sub(r'!\{[^}]*\}!', '!{xxxxxxxx}!', _msg)

    if tgt is None:
        tgt= sys.stdout

    tgt.write(_msg)
    tgt.flush()

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
    options.trace = int(os.getenv('HVR_DBRK_TRACE', options.trace))
    file_format = os.getenv('HVR_DBRK_FILEFORMAT', 'csv')
    if file_format.lower() == 'xml':
        raise Exception("Invalid value {0} for {1}; must be one of 'csv','json','parquet','avro'".format(file_format, 'HVR_DBRK_FILEFORMAT'))
    options.file_format = file_format.lower()
    if os.getenv('HVR_DBRK_FILE_EXPR', ''):
        options.tblname_in_file_pattern, options.file_pattern = parse_expression(os.getenv('HVR_DBRK_FILE_EXPR', ''))
    options.delimiter = os.getenv('HVR_DBRK_DELIMITER', ',')
    if len(options.delimiter) != 1:
        raise Exception("Invalid value {0} for {1}; must be one character".format(options.delimiter, 'HVR_DBRK_DELIMITER'))
    options.line_separator = os.getenv('HVR_DBRK_LINE_SEPARATOR', '')
    if len(options.line_separator) > 1:
        raise Exception("Invalid value {0} for {1}; must be one character".format(options.line_separator, 'HVR_DBRK_LINE_SEPARATOR'))
    burst_delay = os.getenv('HVR_DBRK_LOAD_BURST_DELAY', '')
    if burst_delay:
        try:
            options.load_burst_delay = float(burst_delay)
        except Exception as err:
            print("Invalid value '{}' defined for HVR_DBRK_LOAD_BURST_DELAY; must be numeric".format(burst_delay))
    unmanaged_burst = os.getenv('HVR_DBRK_UNMANAGED_BURST', '')
    if unmanaged_burst:
        if unmanaged_burst.upper() == 'ON':
            options.unmanaged_burst = 'On'
        else:
            options.unmanaged_burst = 'Off'
    fileops = os.getenv('HVR_DBRK_FILESTORE_OPS', '')
    if fileops.lower() == 'none':
        options.filestore_ops = FileOps.NONE
    elif fileops.lower() == 'check':
        options.filestore_ops = FileOps.CHECK
    elif fileops.lower() == 'delete':
        options.filestore_ops = FileOps.DELETE
    elif fileops:
        raise Exception("Invalid file operation '{}' defined in HVR_DBRK_FILESTORE_OPS; valid values are 'check','delete','none'".format(fileops))
    options.access_id = os.getenv('HVR_DBRK_FILESTORE_ID', '')
    options.secret_key = os.getenv('HVR_DBRK_FILESTORE_KEY', '')
    options.region = os.getenv('HVR_DBRK_FILESTORE_REGION', '')
    options.dsn = os.getenv('HVR_DBRK_DSN', '')
    options.external_loc = os.getenv('HVR_DBRK_EXTERNAL_LOC', '')
    options.connect_string = os.getenv('HVR_DBRK_CONNECT_STRING', '')
    conn_timeout = os.getenv('HVR_DBRK_CONNECT_TIMEOUT','')
    if conn_timeout:
        try:
            options.connect_timeout = int(conn_timeout)
        except Exception as err:
            print("Invalid value '{}' defined for HVR_DBRK_CONNECT_TIMEOUT; must be integer".format(conn_timeout))
    if os.getenv('HVR_DBRK_TIMEKEY', '').upper() == 'ON':
        options.target_is_timekey = True
    options.database = os.getenv('HVR_DBRK_DATABASE', '')
    tblproperties = os.getenv('HVR_DBRK_TBLPROPERTIES','')
    if tblproperties:
        if tblproperties == "''" or tblproperties == '""':
            options.set_tblproperties = ''
        else:
            options.set_tblproperties = os.getenv('HVR_DBRK_TBLPROPERTIES')
    options.agent_env = load_agent_env()
    get_multidelete_map()

    refresh_options.job_name = os.getenv('HVR_DBRK_SLICE_REFRESH_ID', '')
    if refresh_options.job_name:
        num_slices = os.getenv('HVR_VAR_SLICE_TOTAL', '')
        slice_num = os.getenv('HVR_VAR_SLICE_NUM', '')
        if not num_slices or not slice_num:
            raise Exception("Script must be run as part of a sliced refresh job; HVR_VAR_SLICE_TOTAL and/or HVR_VAR_SLICE_NUM missing")
        basepath= os.path.join(options.hvr_config, "files")
        refresh_options.lock_file = os.path.join(basepath, refresh_options.job_name+'.lock')
        refresh_options.done_file = os.path.join(basepath, refresh_options.job_name+'.done')
        try:
            refresh_options.num_slices = int(num_slices)
            refresh_options.slice_num = int(slice_num)
        except Exception as err:
            raise Exception("Invalid value '{}' or '{}' defined for HVR_VAR_SLICE_TOTAL or HVR_VAR_SLICE_NUM".format(num_slices,slice_num))
        if refresh_options.num_slices < 1 or refresh_options.slice_num < 0 or refresh_options.slice_num > refresh_options.num_slices:
            trace(1, "HVR_DBRK_SLICE_REFRESH_ID set but num slices = {} and slice num = {}; disabling sliced refresh logic".format(num_slices, slice_num))
            refresh_options.job_name = ''

    file_loc = options.agent_env['HVR_FILE_LOC']
    if file_loc:
        if file_loc[:6] == 'wasbs:':
            options.filestore = FileStore.AZURE_BLOB
            options.url = file_loc
        if file_loc[:6] == 'abfss:':
            options.filestore = FileStore.ADLS_G2
            options.url = file_loc
        ind = file_loc.find("@")
        if ind > 0:
            if options.filestore == FileStore.AWS_BUCKET:
                options.container = file_loc[ind+1:]
                if options.container[-1:] == "/":
                    options.container = options.container[:-1]
                if "/" in options.container:
                    options.directory = options.container[options.container.find("/")+1:]
                    options.container = options.container[:options.container.find("/")]
            else:
                options.container = file_loc[8:ind]
                options.resource = file_loc[ind+1:]
                if options.resource[-1:] == "/":
                    options.resource = options.resource[:-1]
                if "/" in options.resource:
                    options.directory = options.resource[options.resource.find("/")+1:]
                    options.resource = options.resource[:options.resource.find("/")]
   
def parse_expression(filename_expression):
    elems = []
    for part in re.split(r'({[^}]*})', filename_expression):
        if not part:
            continue
        elems.append(str(part))
    tablename_part = -1
    for i in range(len(elems)):
        if elems[i] == '{hvr_tbl_name}':
            tablename_part = i
            break
    if tablename_part == -1:
        print("Warning: HVR_DBRK_FILE_EXPR defined, but does not contain {hvr_tbl_name}'")
        return -1, []
    trace(2, "parse {}".format(filename_expression))
    trace(2, "   result {} {}".format(tablename_part, elems))
    return tablename_part, elems

def trace_input():
    """
    """
    trace(3, "============================================")
    trace(3, "Resource: {0}; Bucket/container: {1}, Root folder: {2}".format(options.resource, options.container, options.directory))
    if options.filestore == FileStore.ADLS_G2 and options.use_wasb:
        trace(3, "Use WASB to access files in SQL commands instead of ABFS")
    if options.connect_timeout:
        trace(3, "Connection timeout = {}".format(options.connect_timeout))
    if options.database:
        trace(3, "Use database {}".format(options.database))
    trace(3, "Optype column is {}; column exists on target = {}".format(options.optype, (not options.no_optype_on_target)))
    trace(3, "Isdeleted column is {}; column exists on target = {}".format(options.isdeleted, (not options.no_isdeleted_on_target)))
    trace(3, "Target is timekey = {}".format(options.target_is_timekey))
    if options.file_format == 'csv':
        trace(3, "File format options: format = '{}' delimiter = '{}'  line separator = '{}'".format(options.file_format, options.delimiter, options.line_separator))
    else:
        trace(3, "File format = '{}'".format(options.file_format))
    if options.file_pattern:
        trace(3, "File name elements: ({}) {}".format(options.tblname_in_file_pattern, options.file_pattern))
    trace(3, "Create/recreate target table(s) during refresh = {0}".format(options.recreate_tables_on_refresh))
    if options.recreate_tables_on_refresh and options.context:
        trace(3, "Use context '{}' when processing actions that apply to the table".format(options.context))
    if options.ignore_columns:
        trace(3, "These columns are not in the target table: {}".format(options.ignore_columns))
    trace(3, "Set TBLPROPERTIES during refresh = '{}'".format(options.set_tblproperties))
    if refresh_options.job_name:
        trace(3, "Sliced refresh: total slices={}; slice num={}; slice file={}".format(refresh_options.num_slices, refresh_options.slice_num, refresh_options.done_file))
    trace(3, "Create burst as unmanaged table = '{}'".format(options.unmanaged_burst))
    if options.load_burst_delay:
        trace(3, "Delay {} seconds after creating the burst table, before loading it".format(options.load_burst_delay))

    if not options.recreate_tables_on_refresh:
        trace(3, "Preserve data during refresh = {}".format(not options.truncate_target_on_refresh))
    if options.filestore_ops != FileOps.ALL:
        if options.filestore_ops != FileOps.CHECK:
            trace(3, "Check of files in filestore disabled")
        if options.filestore_ops != FileOps.DELETE:
            trace(3, "Delete of files in filestore disabled")
        if options.filestore_ops == FileOps.NONE:
            trace(3, "All filestore operations disabled")
    trace(3, "============================================")
    env = os.environ
    if python3:
        for key, value  in env.items():
            if key.find('HVR') != -1:
                trace(3, key + " = " + value)
        for key, value  in env.items():
            if key.find('AZURE') != -1:
                if key.find('SECRET') > 0:
                    trace(3, key + " = ..........................")
                else:
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

    options.hvr_home = os.getenv('HVR_HOME', '')
    if not options.hvr_home:
        raise Exception("$HVR_HOME must be defined")
    options.hvr_config = os.getenv('HVR_CONFIG', '')
    if not options.hvr_config:
        raise Exception("$HVR_CONFIG must be defined")
    options.hvr_6 = check_hvr6()

    tracing = 0
    try:
        tracing = int(os.getenv('HVR_DBRK_TRACE', 0))
    except:
        pass

    cmdargs = argv[4]
    if len(cmdargs):
        try:
            list_args = cmdargs.split(" ");
            opts, args = getopt.getopt(list_args,"c:d:D:E:i:o:O:prtwy")
        except getopt.GetoptError:
            raise Exception("Error parsing command line arguments '" + cmdargs + "' due to invalid argument or invalid syntax")
    
        for opt, arg in opts:
            if opt == '-c':
                options.context = arg
            elif opt == '-d':
                options.isdeleted = arg
            elif opt == '-D':
                options.isdeleted = arg
                options.no_isdeleted_on_target = False
            elif opt == '-E':
                try:
                    ev = arg.split('=')
                    if tracing > 1:
                        print("Add to environment: {}={}".format(arg, ev))
                    os.environ[ev[0]] = ev[1]
                except Exception as err:
                    print("Failed {} putting {} in the environment".format(err, arg))
            elif opt == '-i':
                options.ignore_columns = arg.split(',')
            elif opt == '-o':
                options.optype = arg
            elif opt == '-O':
                options.optype = arg
                options.no_optype_on_target = False
            elif opt == '-p':
                options.truncate_target_on_refresh = False
            elif opt == '-r':
                options.recreate_tables_on_refresh = True
            elif opt == '-t':
                options.target_is_timekey = True
            elif opt == '-w':
                options.use_wasb = True
            elif opt == '-y':
                options.filestore_ops = FileOps.NONE

    if options.recreate_tables_on_refresh: 
        if  not options.truncate_target_on_refresh:
            # both -p and -r set
            raise Exception("The '-p' and '-r' options cannot both be set")
        options.truncate_target_on_refresh = False

    head, tail = os.path.split(argv[0])
    if tracing > 1 and (options.mode == "refr_write_end" or options.mode == "integ_end"):
        print("{0}: VERSION {1}".format(tail, VERSION))
    if tracing > 1:
        print("{0} called with {1} {2} {3} {4}".format(tail, options.mode, options.channel, options.location, cmdargs))

##### Sliced refresh functions #############################################

def cleanup_lock_file():
    if refresh_options.lock_file:
        try:
            if os.path.exists(refresh_options.lock_file):
                os.remove(refresh_options.lock_file)
        except():
            pass

def cleanup_job_files():
    cleanup_lock_file()
    if options.trace < 5:
        try:
            if os.path.exists(refresh_options.done_file):
                os.remove(refresh_options.done_file)
        except():
            pass

def lock(lockfile):
    trace(1, "Lock {}".format(lockfile))
    if os.path.exists(lockfile):
        trace(2, "Lockfile exists, must wait until removed")
    sleep_time= 0
    while refresh_options.lock_fd < 0:
        try:
            refresh_options.lock_fd = os.open(lockfile, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_TRUNC)
        except (IOError, OSError):
            pass
        if refresh_options.lock_fd < 0:
            sleep_time += 1
            time.sleep(1)
    trace(2, "File {} locked after {} seconds".format(lockfile, sleep_time))

def unlock(lockfile):
    if refresh_options.lock_fd < 0:
        return
    if lockfile is None:
        basepath= os.path.join(options.hvr_config, "files")
        lockfile= os.path.join(basepath, refresh_options.lock_file)
    trace(1, "Unlock {}".format(lockfile))
    os.close(refresh_options.lock_fd)
    refresh_options.lock_fd = -1
    try:
        os.remove(lockfile)
    # The file is already deleted and that's what we want.
    except OSError:
        pass
    trace(2, "File {} exists? {}".format(lockfile, os.path.exists(lockfile)))

def check_donefile(donefile):
    if not os.path.exists(donefile):
        with open(donefile, "w") as f:
            done_str = '0' * refresh_options.num_slices
            f.write(done_str)
            f.close()

def get_slices_done():
    num_complete = 0
    check_donefile(refresh_options.done_file)
    with open(refresh_options.done_file, "r+") as f:
        done_str = f.readline()
        done_list= list(done_str)
        done_list[refresh_options.slice_num] = '1'
        num_complete = done_list.count('1')
        done_str= ''.join(done_list)
        trace(1, "Done string = {}".format(done_str))
        trace(1, "{} of {} slices complete".format(num_complete, refresh_options.num_slices))
        f.seek(0)
        f.write(done_str)
    return num_complete

##### HVR 5 functions ############################################################

HVR_ACTION_COLS= ['chn_name', 'grp_name', 'tbl_name', 'act_name', 'act_parameters']
A_CHN= 0
A_GRP= 1
A_TBL= 2
A_ACT= 3
A_PRM= 4

HVR_CONFIG_ACTION_COLS= ['chn_name', 'grp_name', 'tbl_name', 'loc_name', 'act_name', 'act_parameters']
C_CHN= 0
C_GRP= 1
C_TBL= 2
C_LOC= 3
C_ACT= 4
C_PSTR= 5
C_PRM= 6

HVR_COLUMN_COLS= ['chn_name', 'tbl_name', 'col_sequence', 'col_name', 'col_key', 'col_datatype', 'col_length', 'col_nullable']

G_GRP= 0
G_LOC= 1

def hvr_split_line():
    # Returns random split line which is safe-printable by HVR
    # Aim is to prevent potential HVR_xxx_TRACE messages from preventing JSON parsing
    return '---HVR-{0}---'.format(uuid.uuid4())

class ScriptError(Exception):
    def __init__(self, message, exc_caught= None):
        if exc_caught is not None:
            print(exc_caught)
        print(message)
        exit(1)

def hvr_exec(script, split=None, reason="<unknown>"):
    cmd= '{0}/bin/hvr'.format(options.hvr_home)

    try:
        p= subprocess.Popen([cmd],
                shell=False,
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE,
                encoding='utf8')
        stdoutdata, _x= p.communicate(script)
        ret= p.wait()

        if split is not None:
            stdoutdata2= []
            in_output= False
            for line in stdoutdata.splitlines(True):
                if line.rstrip() == split:
                    in_output= not in_output
                elif in_output:
                    stdoutdata2.append(line)
                elif line.strip() != '':
                    # bypass HVR traces to stdout
                    print_raw(line)
            stdoutdata= ''.join(stdoutdata2).rstrip()

        if ret != 0:
            raise ScriptError( ("Child process '{0}' for '{1}' returned with exit "
                               "code {2}").format(cmd, reason, ret))
        return stdoutdata

    except Exception as err:
        raise ScriptError( ("The previous error occurred while executing '{0}' "
                           "for '{1}'.").format(cmd, reason), err)

def hvr_str(x):
    return json.dumps(x)

def hvr_list(x):
    # x should be list or dict
    return "@(Json {x})".format(x= hvr_str(json.dumps(x)))

def to_json(x):
    return json.dumps(x, indent=4, sort_keys=True)

def from_json(s, reason='<unknown>'):
    try:
        return json.loads(s)

    except Exception as e:
            raise ScriptError( ("The previous error occurred during parsing a JSON "
                               "fragment for {0}: '{1}'").format(reason, s), e)

def get_hub_name():
    if options.hvr_6:
        return 'Invalid call for HVR 5.x'
    split= hvr_split_line()
    hvr_script= '''
        Prototype $script "[-h class<str>] [-u user<str>] -- hub<str>" \\
            {script_args}
        Set hub_dbnorm $(DbNorm *@(HubClass *@optlist_h) $hub)
        Echo -v -- {split} $(Json ($hub_dbnorm) ) {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts))

    res= from_json(hvr_exec(hvr_script, split, "get-hubname"),
            "get-hubname output")

    return res[0]

def get_table_basename(tablename):
    if options.hvr_6:
        return tablename
    split= hvr_split_line()
    hvr_script= '''

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set name @(Select \\
                -w "tbl_name='{table_hvrid}' and chn_name='{channel}'" \\
                -o (tbl_base_name) \\
                $db hvr_table)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@name)) \\
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            channel=options.channel,
            table_hvrid=tablename)

    res= from_json(hvr_exec(hvr_script, split, "valid-table-name"),
            "valid-table-name output")

    if not len(res[0]):
        return ""
    return res[0][0][0]

def get_table_columns(tablename):
    if options.hvr_6:
        return []
    split= hvr_split_line()
    hvr_script= '''
        Set hvr_column_colnames {hvr_column_colnames}

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set columns @(Select \\
                -o @hvr_column_colnames \\
                -w "tbl_name='{refresh_table}' and chn_name='{channel}'" \\
                $db hvr_column)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@columns)) \
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            hvr_column_colnames= hvr_list(HVR_COLUMN_COLS),
            channel=options.channel,
            refresh_table=tablename)

    res= from_json(hvr_exec(hvr_script, split, "table_columns"),
            "table_columns output")
    return res[0]

def get_group_locations():
    split= hvr_split_line()
    hvr_script= '''

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set hvr_grplocs @(Select \\
                -o (grp_name loc_name) \\        # -o=Fetch columns cols
                -w "chn_name='{channel}'"  \\
                $db hvr_loc_group_member)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@hvr_grplocs)) \
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            channel=options.channel)

    res= from_json(hvr_exec(hvr_script, split, "group_locations"),
            "group_locations output")

    return res[0]

def get_table_prop_actions():
    split= hvr_split_line()
    hvr_script= '''
        Set hvr_action_colnames {hvr_action_colnames}

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set hvr_action_rows @(Select \\
                -o (*@hvr_action_colnames) \\           # -o=Fetch columns cols
                -w "(chn_name='*' or chn_name='{channel}') and act_name='TableProperties'"  \\
                $db hvr_action)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@hvr_action_rows)) \\
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            hvr_action_colnames= hvr_list(HVR_ACTION_COLS),
            channel=options.channel)

    res= from_json(hvr_exec(hvr_script, split, "all-props"),
            "all-props output")

    table_props = []
    for act in res[0]:
        if act[A_GRP] != '*' and act[A_GRP] != options.locgroup:
            continue
        table_props.append(act)
    return table_props
    
def get_config_table_prop_actions():
    split= hvr_split_line()
    hvr_script= '''
        Set hvr_config_action_colnames {hvr_config_action_colnames}

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set hvr_config_action_rows @(Select \\
                -o (*@hvr_config_action_colnames) \\           # -o=Fetch columns cols
                -w "(chn_name='*' or chn_name='{channel}') and act_name='TableProperties'"  \\
                $db hvr_config_action)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@hvr_config_action_rows)) \\
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            hvr_config_action_colnames= hvr_list(HVR_CONFIG_ACTION_COLS),
            channel=options.channel)

    res= from_json(hvr_exec(hvr_script, split, "all-config-props"),
            "all-config-props output")

    table_props = []
    for act in res[0]:
        if act[C_LOC] != '*' and act[C_LOC] != options.location:
            continue
        if act[C_GRP] != '*' and act[C_GRP] != options.locgroup:
            continue
        table_props.append(act)
    return table_props
    
def get_table_properties():
    tbl_props = merge_action_with_config_action(get_table_prop_actions(), get_config_table_prop_actions())
    for prop in tbl_props:
        prop.append(param_str_to_dict(prop[C_PSTR]))
    return tbl_props
    
def get_column_prop_actions():
    split= hvr_split_line()
    hvr_script= '''
        Set hvr_action_colnames {hvr_action_colnames}

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set hvr_action_rows @(Select \\
                -o (*@hvr_action_colnames) \\           # -o=Fetch columns cols
                -w "(chn_name='*' or chn_name='{channel}') and act_name='ColumnProperties'"  \\
                $db hvr_action)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@hvr_action_rows)) \\
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            hvr_action_colnames= hvr_list(HVR_ACTION_COLS),
            channel=options.channel)

    res= from_json(hvr_exec(hvr_script, split, "all-props"),
            "all-props output")

    column_props = []
    for act in res[0]:
        if act[A_GRP] != '*' and act[A_GRP] != options.locgroup:
            continue
        column_props.append(act)
    return column_props
    
def get_config_column_prop_actions():
    split= hvr_split_line()
    hvr_script= '''
        Set hvr_config_action_colnames {hvr_config_action_colnames}

        Prototype -p "" $script \\
                '[-h hub_class<str>] [-u user<str>] -- hub<str>' \\
                {script_args}

        Set db $(DbConnect *@(HubClass *@optlist_h) *@optlist_u $hub)

        Set hvr_config_action_rows @(Select \\
                -o (*@hvr_config_action_colnames) \\           # -o=Fetch columns cols
                -w "(chn_name='*' or chn_name='{channel}') and act_name='ColumnProperties'"  \\
                $db hvr_config_action)

        # Echo header,json,footer together to avoid HVR_xxx_TRACE in between
        Echo -v -- {split} \\
                    $(Json (@hvr_config_action_rows)) \\
                    {split}
    '''.format(split= hvr_str(split), script_args= hvr_list(options.hvr_opts),
            hvr_config_action_colnames= hvr_list(HVR_CONFIG_ACTION_COLS),
            channel=options.channel)

    res= from_json(hvr_exec(hvr_script, split, "all-config-props"),
            "all-config-props output")

    column_props = []
    for act in res[0]:
        if act[C_LOC] != '*' and act[C_LOC] != options.location:
            continue
        if act[C_GRP] != '*' and act[C_GRP] != options.locgroup:
            continue
        column_props.append(act)
    return column_props
    
def get_column_properties():
    col_props = merge_action_with_config_action(get_column_prop_actions(), get_config_column_prop_actions())
    for prop in col_props:
        prop.append(param_str_to_dict(prop[C_PSTR]))
    return col_props
    
def convert_action_into_configaction(act):
    return [act[0], act[1], act[2], '*', act[3], act[4]]

def merge_action_with_config_action(actlist, config_actlist):
    show_actions("hvr_action", actlist)
    show_actions("hvr_config_action", config_actlist)
    for act in actlist:
        config_actlist.append(convert_action_into_configaction(act))
    show_actions("combined", config_actlist)
    return config_actlist

##### HVR 6 functions ############################################################

def get_key_value(line):
    key = value = ''
    s1 = line.find('"')
    if s1 >= 0:
        s2 = line.find('"', s1+1)
        if s1 >= 0 and s2 > s1:
            key = line[s1+1:s2]
            if line[s2+1] == ':':
                value = line[s2+3:]
                if value != "{" and value != "]":
                    if value[-1] == ',':
                        value = value[:-1]
                    if value[0] == '"':
                        value = value[1:-1]
    else:
        s1 = line.find('{')
        if s1 >= 0:
            value = line[s1:s1+1]
        else:
            s1 = line.find('}')
            if s1 >= 0:
                value = line[s1:s1+1]
    return key, value

def get_list(line):
    retval = []
    s1 = line.find('"')
    while s1 >= 0 and s1 < len(line):
        s2 = line.find('"', s1+1)
        retval.append(line[s1+1:s2])
        s1 = line.find('"', s2+1)
    return retval

def temp_export_file_name():
    hvr_config = os.getenv('HVR_CONFIG', '')
    if not hvr_config:
        raise Exception("$HVR_CONFIG must be defined")
    tempname = "{0}_{1}_{2}.export.json".format(str(time.time()), options.hub, options.channel)
    tempexpfile = os.path.join(hvr_config, "tmp")
    tempexpfile = os.path.join(tempexpfile, tempname)
    return tempexpfile

def get_channel_export():
    tempexpfile = temp_export_file_name()
    cmd = []
    cmd.append("hvrdefinitionexport")
    cmd.append("-c{}".format(options.channel))
    for opt in options.hvr_opts:
        cmd.append(opt)
    cmd.append(tempexpfile)
    trace(2, "{}".format(cmd))
    try:
        rval = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        if options.trace >= 2 and len(rval):
            print("return from run command: {}".format(rval))
    except subprocess.CalledProcessError as e:
        if options.trace >= 2:
            print("{}".format(str(e)))
        raise Exception("Error getting channel export")
    return tempexpfile

class Options:
    NONE = 0
    CHANNEL = 1
    TABLE = 2
    COLUMN = 3

class ChannelInfo:
    GENERAL = 0
    TABLES = 1
    COLUMN = 2
    LOCATION_GROUPS = 3
    ACTIONS = 4

def hvr6_init_createtable_info():
    global g_table_props
    global g_column_props

    g_table_props = []
    g_column_props = []
    channel = False
    channelinfo = ChannelInfo.GENERAL
    thisgroup = ''
    grp_locations = {}
    action = [options.channel, '', '', '', '', '', {}]

    with open(options.channel_export, "r") as f:
        for line in f:
            key, value = get_key_value(line[:-1])
            if not key:
                if channelinfo == ChannelInfo.ACTIONS:
                    if action[C_GRP] == options.location:
                        action[C_LOC] = options.location
                        action[C_GRP] = ''
                    if options.location == action[C_LOC] or options.locgroup == '*' or options.locgroup == action[C_GRP]:
                        if action[C_ACT] == 'TableProperties':
                            g_table_props.append(action)
                        if action[C_ACT] == 'ColumnProperties':
                            g_column_props.append(action)
                    action = [options.channel, '', '', '', '', '', {}]
                continue;
            if channel and channelinfo == ChannelInfo.LOCATION_GROUPS:
                if not thisgroup:
                    thisgroup = key
                elif key == "members":
                    grp_locations[thisgroup] = get_list(value)
                    thisgroup = ''
            if channel and key == "loc_groups":
                channelinfo = ChannelInfo.LOCATION_GROUPS
                thisgroup = ''
            if channel and channelinfo == ChannelInfo.ACTIONS:
                if key == "type":
                    action[C_ACT] = value
                elif key == "loc_scope":
                    action[C_GRP] = value
                elif key == "table_scope":
                    action[C_TBL] = value
                elif key != "params" and value:
                    action[C_PRM][key] = value
            if channel and key == "actions":
                channelinfo = ChannelInfo.ACTIONS
                inactions = True
                for grp,locs in grp_locations.items():
                    if options.location in locs:
                        if options.locgroup:
                            options.locgroup = '*'
                        else:
                            options.locgroup = grp
            if key == "channel":
                channel = (value == options.channel)

    print("grplocs: {}".format(grp_locations))
    
    show_actions("TableProperties", g_table_props)
    show_actions("ColumnProperties", g_column_props)

def hvr6_get_table_info(tablename):
    channel = False
    channelinfo = ChannelInfo.GENERAL
    thistable = 0

    targetname = get_target_basename(tablename)
    target_columns = []
    column = []
    with open(options.channel_export, "r") as f:
        for line in f:
            key, value = get_key_value(line[:-1])
            if thistable:
                if value == '{':
                    thistable = thistable + 1
                elif value == '}':
                    thistable = thistable - 1
            if channel and key == "tables":
                channelinfo = ChannelInfo.TABLES
            if channel and key == "actions":
                channelinfo = ChannelInfo.ACTIONS
                thistable = 0
            if channel and thistable:
                if not targetname and key == "base_name":
                    targetname = value
                if thistable == 2 and column:
                    target_columns.append(column)
                    column = []
                if not key:
                    continue
                if thistable == 3:
                    if not column:
                        column = [key, key, '0', '', '0', '0']
                    else:
                        if key == "data_type":
                            column[3] = value
                        if key == "key":
                            column[2] = value
                if thistable == 4:
                    if key == "bytelen":
                        column[4] = value
                    if key == "charlen":
                        charlen = column[4]
                        if charlen:
                            charlen += ','
                        charlen += value
                        column[4] = charlen
                    if key == "nullable" and value == "true":
                        column[5] = '1'
            if channel and channelinfo == ChannelInfo.TABLES and key == tablename:
                thistable = 1
            if key == "channel":
                channel = (value == options.channel)
    return targetname, target_columns

#### Create table ###############################################################

INT1_TYPES= ['byteint','integer1','tinyint','tinyint signed','tinyint unsigned']
INT2_TYPES= ['integer2','smallint','smallint unsigned','unsigned smallint']
INT3_TYPES= ['mediumint','mediumint unsigned']
INT4_TYPES= ['int','int unsigned','integer','integer4','unsigned int']
INT8_TYPES= ['bigint','bigint unsigned','integer8','unsigned bigint']
DEC_TYPES= ['decimal','money','money (ingres)','number','numeric','numeric (db2i)','smallmoney']
REAL4_TYPES= ['binary_float','float4','real']
REAL8_TYPES= ['binary_double','double','float','float8']
CHAR_TYPES= ['c','char','nchar','unichar']
VARCHAR_TYPES= ['nvarchar','nvarchar2','univarchar','varchar','varchar2','varchar (sybase)']
VERY_LARGE_CHAR= ['clob','dbclob','json','jsonb','long char','long nvarchar (db2)','long nvarchar','long varchar (db2)','long varchar','long','nclob','ntext','nvarchar(max)','text (ingres)','text (sqlserver)','text (sybase)','unitext','varchar(max)','xml','db2 xml']
BYTE_TYPES= ['binary','byte varying','byte','raw','varbinary (sybase)','varbinary','varbinary(max)','varbyte']
BLOB_TYPES= ['bfile','blob','image (sybase)','image','long byte','long raw','long varbinary']
DATE_TYPES= ['ansidate','date (mysql)','date (sybase)','postgres date']
TIME_TYPES= ['time','time (mysql)','time (sybase)','time2','time with local time zone','time with time zone']
DATETIME_TYPES= ['date','datetime','datetime2','datetime (bigquery)','datetime (mysql)','datetime (sybase)','datetimeoffset','ingresdate','smalldatetime','db2 timestamp with time zone','postgres timestamp','postgres timestamp with time zone','timestamp','timestamp (bigquery)','timestamp (db2)','timestamp (ingres)','timestamp (mysql)','timestamp (oracle)','timestamp (sqlserver)','timestamp (sybase)','timestamp with local time zone','timestamp with local tz (oracle)','timestamp with time zone','timestamp with tz (oracle)','epoch'] 
INTERVAL_TYPES= ['interval day to second','interval day to second (ingres)','interval year to month','interval year to month (ingres)']
BOOL_TYPES= ['bit','bit (mysql)','bool','boolean']

def breakup_length(lenval):
    if not lenval:
        return '',''
    comma = lenval.find(',')
    if comma < 0:
        return lenval,''
    return lenval[:comma],lenval[comma+1:]

def remove_identity(ctype):
    if len(ctype) > len(' identity'):
        if ctype[-9:] == ' identity':
            return ctype[:len(ctype)-9]
    return ctype

def get_decimal_type(dtype, precision, scale):
    p = s = 0
    if precision:
        p = int(precision)
    if scale:
        s = int(scale)
    if p > 38 or s > 37:
        return 'DOUBLE'
    if s <= 0:
        return 'DECIMAL({})'.format(precision)
    return 'DECIMAL({},{})'.format(precision,scale)

def databricks_datatype(col):
    name = col[1]
    ctype = col[3]
    bp,cs = breakup_length(col[4])
    nullable = col[5]
    ctype = remove_identity(ctype)
#    if ctype in g_replacement_types.keys():
#        return g_replacement_types[ctype]
    if ctype in INT1_TYPES:
        return 'BYTE'
    if ctype in INT2_TYPES:
        return 'SHORT'
    if ctype in INT3_TYPES:
        return 'INTEGER'
    if ctype in INT4_TYPES:
        return 'INTEGER'
    if ctype in INT8_TYPES:
        return 'LONG'
    if ctype in REAL4_TYPES:
        return 'FLOAT'
    if ctype in REAL8_TYPES:
        return 'DOUBLE'
    if ctype in CHAR_TYPES:
        return 'STRING'
    if ctype in VARCHAR_TYPES:
        return 'STRING'
    if ctype in VERY_LARGE_CHAR:
        return 'STRING'
    if ctype in BYTE_TYPES:
        return 'BINARY'
    if ctype in BLOB_TYPES:
        return 'BINARY'
    if ctype == 'number' and col[4] == '0':
        return 'DOUBLE'
    if ctype == 'money (ingres)':
        return 'DECIMAL(14,2)'
    if ctype == 'money':
        return 'DECIMAL(19,4)'
    if ctype == 'smallmoney':
        return 'DECIMAL(10,4)'
    if ctype in DEC_TYPES:
        return get_decimal_type(ctype, bp, cs)
    if ctype in TIME_TYPES:
        return 'STRING'
    if ctype in DATE_TYPES:
        return 'DATE'
    if ctype in DATETIME_TYPES:
        return 'TIMESTAMP'
    if ctype in INTERVAL_TYPES:
        return 'STRING'
    if ctype in BOOL_TYPES:
        return 'BYTE'
    if ctype == 'rowid' or ctype == 'urowid':
        return 'STRING'
    if ctype == 'uniqueidentifier':
        return 'STRING'
    if ctype == 'decfloat':
        return 'STRING'
    if ctype == 'graphic' or ctype == 'vargraphic':
        return 'STRING'
    if ctype == 'rowversion':
        return 'BINARY'
    raise Exception("Mapping unknown for '{}'".format(ctype))

def get_external_loc(table):
    if options.external_loc.find('{hvr_tbl_name}') > 0:
        return options.external_loc.replace('{hvr_tbl_name}', table)
    return options.external_loc

def target_create_table(table, columns):
    create_sql = "CREATE OR REPLACE TABLE {} (".format(table)
    sep = ' '
    for col in columns:
        create_sql += "{} `{}` {}".format(sep, col[1], databricks_datatype(col))
        sep = ','
    create_sql += ") USING DELTA"
    if options.external_loc:
        create_sql += " LOCATION '{}'".format(get_external_loc(table))
    if options.set_tblproperties:
        create_sql += " TBLPROPERTIES ({})".format(options.set_tblproperties)
    return create_sql

def param_str_to_dict(param_str):
    import shlex
    param_dict = {}
    if param_str:
        opts = shlex.split(param_str)
        for opt in opts:
            option = opt[1:]
            if not '=' in option:
                param_dict[option] = ''
            else:
                sep = option.find('=')
                param_dict[option[:sep]] = option[sep+1:]
    return param_dict

def get_property(params, prop_name):
    if prop_name in params.keys():
        return params[prop_name]
    return ''

def get_sequence(col):
    return col[2]

def target_columns(table):
#  HVR_COLUMN_COLS= ['chn_name', 'tbl_name', 'col_sequence', 'col_name', 'col_key', 'col_datatype', 'col_length', 'col_nullable']
    repo_columns = get_table_columns(table)
    target_cols = []
    repo_columns.sort(key=get_sequence)
    for col in repo_columns:
        target_cols.append([col[3], col[3], col[4], col[5], col[6], col[7]])
    return target_cols

def process_datatype_match(datatype_match):
    if datatype_match and datatype_match[-1] == "]" and "[" in datatype_match:
        ed = datatype_match.find('[')
        if datatype_match[ed+1:-1] == "prec=0 && scale=0":
            return datatype_match[:ed], '0'
        trace(2, "Datatype match {}; attributes specified {}; not processed; expecting '{}'".format(datatype_match, datatype_match[ed+1:-1], "prec=0 && scale=0"))
    return datatype_match, None

def remove_column(params, columns):
    colname = get_property(params, 'Name')
    trace(3, "Process Column properties, remove '{}'".format(colname))
    newlist = []
    for col in columns:
        if col[0] != colname:
            newlist.append(col)
    return newlist
            
def add_column(params, columns):
    colname = get_property(params, 'Name')
    trace(3, "Process Column properties, add '{}'".format(colname))
    if colname and options.isdeleted == colname and options.no_isdeleted_on_target:
        return
    if colname and options.optype == colname and options.no_optype_on_target:
        return
    if colname and colname in options.ignore_columns:
        return
    basename = get_property(params, 'BaseName')
    if not basename:
        basename = colname
    dtype = get_property(params, 'Datatype')
    length = get_property(params, 'Length')
    precision = get_property(params, 'Precision')
    scale = get_property(params, 'Scale')
    if precision:
        length = "{}".format(precision)
        if scale:
            length += ",{}".format(scale)
    if not length:
        length = '0'
    nullable = '0'
    if 'Nullable' in params.keys():
        nullable = '1'
    key = '0'
    if 'Key' in params.keys():
        key = '1'
    columns.append([colname, basename, key, dtype, length, nullable])

def modify_column(params, columns):
    colname = get_property(params, 'Name')
    dtmatch, def_ps = process_datatype_match(get_property(params, 'DatatypeMatch'))

    if colname:
        trace(3, "Process Column properties, modify column '{}'".format(colname))
    if dtmatch:
        trace(3, "Process Column properties, match datatype '{}'".format(dtmatch))
    for col in columns:
        if col[0] == colname or dtmatch == col[3]:
            if dtmatch and def_ps and def_ps != col[4]:
                continue
            if 'BaseName' in params.keys():
                col[1] = get_property(params, 'BaseName')
            if 'Datatype' in params.keys():
                col[3] = get_property(params, 'Datatype')
                len_pre_scl = ''
                if 'Precision' in params.keys():
                    len_pre_scl = "{}".format(get_property(params, 'Precision'))
                    if 'Scale' in params.keys():
                        len_pre_scl += ",{}".format(get_property(params, 'Scale'))
                elif 'Length' in params.keys():
                    len_pre_scl = get_property(params, 'Length')
                col[4] = len_pre_scl
            if 'Nullable' in params.keys():
                col[5] = '1'
            if 'Key' in params.keys():
                col[2] = '1'
            if colname:
                return

def apply_column_property(table, prop, columns):
    params = prop[C_PRM]
    if options.context:
        context = get_property(params, 'Context')
        if context and context != options.context:
            return columns
    if 'Absent' in params.keys():
        columns = remove_column(params, columns)
    elif 'Extra' in params.keys():
        add_column(params, columns)
    else:
        modify_column(params, columns)
    
    return columns

def show_actions(actname, actlist):
    trace(2, "{}({})".format(actname, len(actlist)))
    if options.trace > 1:
        for act in actlist:
            print("   {}".format(act))

def show_columns(columns):
    if options.trace > 1:
        for col in columns:
            print("   {}".format(col))

def get_target_basename(table):
    for prop in g_table_props:
        if prop[C_TBL] == table:
            if options.context:
                context = get_property(prop[C_PRM], 'Context')
                if context and context != options.context:
                    continue
            basename = get_property(prop[C_PRM], 'BaseName')
            if basename:
                return basename
    return ''

def get_target_tablename(table):
    basename = get_target_basename(table)
    if basename:
        return basename
    return get_table_basename(table)

def hvr5_init_createtable_info():
    global g_table_props
    global g_column_props

    grp_locations = get_group_locations()

    for grp_loc in grp_locations:
        if options.location == grp_loc[G_LOC]:
            if options.locgroup:
                options.locgroup = '*'
            else:
                options.locgroup = grp_loc[G_GRP]

    g_table_props = get_table_properties()
    show_actions("TableProperties", g_table_props)
    
    g_column_props = get_column_properties()
    show_actions("ColumnProperties", g_column_props)
    
def init_createtable_info():
    trace(2, "init_createtable_info")
    if options.hvr_6:
        hvr6_init_createtable_info()
    else:
        hvr5_init_createtable_info()

def initialize_hvr_connect():
    import base64

    if not python3:
        raise Exception("Create-table-on-refresh ('-r' option) requires Python 3")
    hvr_connect = os.getenv('HVR_DBRK_HVRCONNECT', '')
    if not hvr_connect:
        raise Exception("HVR connection string required for 'create on refresh' option'")
    trace(4, "Encoded connect string = {}".format(hvr_connect))
    try:
        hvr_connect = base64.b64decode(hvr_connect)
        hvr_connect = hvr_connect.decode("utf-8")
    except Exception as ex:
        print("Exception decoding HVR_DBRK_HVRCONNECT: {}".format(ex))
        raise Exception("Invalid Base64 string in HVR_DBRK_HVRCONNECT; cannot decode")
    trace(4, "Decoded connect string = {}".format(hvr_connect))
    args = hvr_connect.split(' ')
    for a in range(0, len(args)):
        if args[a].startswith("'"):
            astr = args[a]
            args[a] = astr[1:-1]
    for arg in args:
        if arg[0] == "'":
            arg = arg[1:-1]
    for arg in args:
        trace(4, "  {}".format(arg))
    a = 0
    while True:
         if not args[a].startswith('-'):
             break
         opt = args[a][1]
         if not options.hvr_6 and opt != 'h' and opt != 'u':
             print("Invalid option {} found as part of the Hub login options", args[a])
             options.hvr_opts = []
             return
         if len(args[a]) == 2:
             options.hvr_opts.append(args[a] + args[a+1])
             a = a + 1
         else:
             options.hvr_opts.append(args[a])
         a = a + 1
    options.hvr_opts.append(args[a])
    if not options.hvr_6:
        trace(4, "HVR connect using = {}".format(options.hvr_opts))
        hubname = get_hub_name()
        trace(2, "Connection to HVR valid, hubdb = {}".format(hubname))
    else:
        options.hub = options.hvr_opts[-1]
        trace(2, "Connection to HVR valid, hubdb = {}".format(options.hvr_opts[-1]))
        options.channel_export = get_channel_export()
        trace(3, "Channel export: {}".format(options.channel_export))

##### Main function ############################################################

def get_multidelete_map():
    envvar = os.getenv('HVR_DBRK_MULTIDELETE', '')
    if not envvar:
        return
    keys_per_table = envvar.split(";")
    multidelete_map = {}
    for val in keys_per_table:
        tabend = val.find(':')
        if tabend < 0 or tabend+1>=len(val):
            raise Exception("Value " + envvar + " for HVR_DBRK_MULTIDELETE is invalid. Format is table:keylist;table:keylist")
        elif tabend + 1 < len(val):
            multidelete_map[val[:tabend]] = val[tabend+1:]

    if len(multidelete_map):
        options.multidelete_map = multidelete_map
        trace(2, "Key columns to ignore in multi-delete")
        for key in multidelete_map.keys():
            trace(2, "   " + key + ": " + multidelete_map[key])

def multidelete_table(tablename):
    if tablename in ['bseg','bset','bsem']:
        return True
    if tablename in options.multidelete_map:
        return True
    return False

def unused_keys_in_multidelete(tablename):
    if tablename in options.multidelete_map:
        return options.multidelete_map[tablename]
    return 'pageno'

def file_for_table(tablename, filename, fileext):
    if filename[-len(fileext):] != fileext:
        raise Exception("Expected extension {} not found in {}".format(options.file_format, filename))
    if options.file_pattern:
        name = filename
        if options.tblname_in_file_pattern > 0:
            loc = name.find(options.file_pattern[options.tblname_in_file_pattern-1])
            if loc >= 0:
                name = name[loc:]
                name = name[len(options.file_pattern[options.tblname_in_file_pattern-1]):]
        loc = name.find(options.file_pattern[options.tblname_in_file_pattern+1])
        if loc > 0:
            name = name[:loc]
        if name == tablename:
            return True
    name = filename[filename.find("-")+1:]
    if name[-len(fileext):] == fileext:
        name = name[:-len(fileext)]
        if name == tablename:
            return True
    return False

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
    suffix = "." + options.file_format
    for item in zip(hvr_base_names, hvr_tbl_names, hvr_col_names, hvr_tbl_keys):
        tbl_map[item] = []
        num_rows[item] = 0
        if files :
            pop_list = []
            for idx, f in enumerate(files):
                if file_for_table(item[1], f, suffix):
                    file_path = prefix_directory(f)
                    tbl_map[item].append(file_path)
                    pop_list.append(idx)
                    num_rows[item] += int(rows[idx])
            # Pop files from list from high index to low to maintain index sanity
            for idx in reversed(pop_list):
                files.pop(idx)
                rows.pop(idx)

    if files :  
        raise Exception ("Cannot associate filenames in $HVR_FILE_NAMES with their tables; please set HVR_DBRK_FILE_EXPR to Integrate /RenameExpression")

    return tbl_map, num_rows

def prefix_directory(path):
    if options.directory:
        return options.directory + '/' + path
    return path

#
# Functions that interact with the S3 bucket where integrate put the files
#
def get_s3_handles():
    if options.filestore != FileStore.AWS_BUCKET:
        return
    try:
        import boto3
        if not options.access_id:
            trace(4, "Get S3 handle: boto3.resource('s3')")
            Connections.s3_resource = boto3.resource('s3')
        else:
            if options.region:
                trace(4, "Get S3 handle: boto3.resource('s3', {0}, {1}, {2})".format(options.access_id, options.secret_key, options.region))
                Connections.s3_resource = boto3.resource('s3', aws_access_key_id=options.access_id, aws_secret_access_key=options.secret_key, region_name=options.region)
            else:
                trace(4, "Get S3 handle: boto3.resource('s3', {0}, {1})".format(options.access_id, options.secret_key))
                Connections.s3_resource = boto3.resource('s3', aws_access_key_id=options.access_id, aws_secret_access_key=options.secret_key)
    except Exception as ex:
        print("Failed creating resource service client for s3")
        raise ex
    try:
        import boto3
        if not options.access_id:
            trace(4, "Get S3 handle: boto3.client('s3')")
            Connections.s3_client = boto3.client('s3')
        else:
            if options.region:
                trace(4, "Get S3 handle: boto3.client('s3', {0}, {1}, {2})".format(options.access_id, options.secret_key, options.region))
                Connections.s3_client = boto3.client('s3', aws_access_key_id=options.access_id, aws_secret_access_key=options.secret_key, region_name=options.region)
            else:
                trace(4, "Get S3 handle: boto3.client('s3', {0}, {1})".format(options.access_id, options.secret_key))
                Connections.s3_client = boto3.client('s3', aws_access_key_id=options.access_id, aws_secret_access_key=options.secret_key)
    except Exception as ex:
        print("Failed creating service client for s3")
        raise ex

def files_in_s3(folder, file_list):
    kwargs = {'Bucket': options.container}
    kwargs['Prefix'] = folder
    trace(2, "Look for files in S3 {0}/{1}".format(options.container, folder))

    files_in_list = 0
    files_not_in_list = 0
    objs = []
    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        trace(4, "Look for files in S3: Connections.s3_client.list_objects_v2({0})".format(kwargs))
        resp = Connections.s3_client.list_objects_v2(**kwargs)

        try:
            contents = resp['Contents']
        except KeyError:
            return 0

        for obj in contents:
            key = obj['Key']
            if key.startswith(folder) and key.endswith(options.file_format):
                objs.append(obj)

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

    if objs:
        for obj in objs:
            if obj['Key'] in file_list:
                files_in_list += 1
            else:
                files_not_in_list += 1

    return files_in_list,files_not_in_list

def delete_files_from_s3(file_list):
    if options.filestore != FileStore.AWS_BUCKET:
        return
    for name in file_list:
        try:
            trace(4, "Get object: Connections.s3_resource.Object({0}, {1})".format(options.container, name))
            obj = Connections.s3_resource.Object(options.container, name)
            trace(2, "Delete {0} from {1}".format(name, options.container))
            obj.delete()
        except Exception as ex:
            print("Failed deleting {0} from s3:{1}", name, options.container)
            raise ex

#
# Functions that interact with the Azure container where integrate put the files
#
def get_azblob_handles():
    url = "https://{0}.blob.core.windows.net/".format(options.resource)
    trace(4, "Get handle to Azure BlobStore: BlobServiceClient(account_url={0}, credential={1}".format(url, options.secret_key))
    try:
        from azure.storage.blob import BlobServiceClient
        Connections.azstore_service = BlobServiceClient(account_url=url, credential=options.secret_key)
    except Exception as ex:
        print("Failed getting a service handle using {}".format(url))
        raise ex

def files_in_azblob(folder, file_list):
    files_in_list = 0
    files_not_in_list = 0
    client = None
    trace(4, "Get container client: Connections.azstore_service.get_container_client({})".format(options.container))
    try:
        client = Connections.azstore_service.get_container_client(options.container)
    except Exception as ex:
        print("Failed getting container client for {}".format(options.container))
        raise ex

    trace(4, "List Blobs: client.list_blobs(name_starts_with={})".format(folder))
    try:
        blob_list = client.list_blobs(name_starts_with=folder)
    except Exception as ex:
        print("Failed getting list of blobs in {0}/{1}".format(options.container, folder))
        raise ex

    for blob in blob_list:
        trace(3, "  {}".format(blob.name))
        if blob.name in file_list:
            files_in_list += 1
        elif blob.name != folder:
            files_not_in_list += 1

    return files_in_list,files_not_in_list

def delete_files_from_azblob(file_list):
    client = None
    trace(4, "Get container client: Connections.azstore_service.get_container_client({})".format(options.container))
    try:
        client = Connections.azstore_service.get_container_client(options.container)
    except Exception as ex:
        print("Failed getting container client for {}".format(options.container))
        raise ex

    trace(4, "Delete from client: client.delete_blobs({})".format(file_list))
    try:
        client.delete_blobs(*file_list)
    except Exception as ex:
        print("Failed deleting blobs {}".format(file_list))
        raise ex

#
# Functions that interact with the Azure ADLS G2 where integrate put the files
#
def get_azdfs_handle_using_azure_identity():
    url = "https://{0}.dfs.core.windows.net/".format(options.resource)
    trace(4, "Get handle to Azure ADLS fs using DefaultAzureCredential: DataLakeServiceClient(account_url={0})".format(url))
    try:
        from azure.identity import DefaultAzureCredential
        from azure.storage.filedatalake import DataLakeServiceClient
        azid = DefaultAzureCredential()
        Connections.azstore_service = DataLakeServiceClient(account_url=url, credential=azid)
    except Exception as ex:
        print("Failed getting a service handle using {}".format(url))
        raise ex

def get_azdfs_handle_using_access_keys():
    url = "https://{0}.dfs.core.windows.net/".format(options.resource)
    trace(4, "Get handle to Azure ADLS fs: DataLakeServiceClient(account_url={0}, credential={1})".format(url, options.secret_key))
    try:
        from azure.storage.filedatalake import DataLakeServiceClient
        Connections.azstore_service = DataLakeServiceClient(account_url=url, credential=options.secret_key)
    except Exception as ex:
        print("Failed getting a service handle using {}".format(url))
        raise ex

def get_azdfs_handles():
    if not options.secret_key:
        get_azdfs_handle_using_azure_identity()
    else:
        get_azdfs_handle_using_access_keys()

def files_in_azdfs(folder, file_list):
    files_in_list = 0
    files_not_in_list = 0
    client = None
    trace(4, "Get container client: Connections.azstore_service.get_file_system_client({})".format(options.container))
    try:
        client = Connections.azstore_service.get_file_system_client(options.container)
    except Exception as ex:
        print("Failed getting container client for {}".format(options.container))
        raise ex

    trace(4, "List files: client.get_paths(), filter by {}".format(folder))
    try:
        path_list = client.get_paths()
    except Exception as ex:
        print("Failed getting list of files in {0}/{1}".format(options.container, folder))
        raise ex

    state_files = 0
    for file_path in path_list:
        if file_path.name.startswith('_hvr_state/'):
            state_files += 1
        if folder:
            if file_path.name == folder or not file_path.name.startswith(folder):
                continue
        trace(3, "  {}".format(file_path.name))
        if file_path.name in file_list:
            files_in_list += 1
        else:
            files_not_in_list += 1

    if state_files:
        trace(3, "{} files in _hvr_state".format(state_files))
    return files_in_list,files_not_in_list

def delete_files_from_azdfs(file_list):
    client = None
    trace(4, "Get container client: Connections.azstore_service.get_file_system_client({})".format(options.container))
    try:
        client = Connections.azstore_service.get_file_system_client(options.container)
    except Exception as ex:
        print("Failed getting container client for {}".format(options.container))
        raise ex

    for fname in file_list:
        trace(4, "Delete from client: client.delete_file({})".format(fname))
        try:
            client.delete_file(fname)
        except Exception as ex:
            print("Failed deleting file {}".format(fname))
            raise ex

#
# Functions that interact with file store where integrate put the files
#
def get_filestore_handles():
    if options.filestore_ops == FileOps.NONE:
        return
    if options.filestore == FileStore.ADLS_G2:
        get_azdfs_handles()
    elif options.filestore == FileStore.AZURE_BLOB:
        get_azblob_handles()
    else:
        get_s3_handles()

def files_found_in_filestore(table, file_list):
    if options.filestore_ops != FileOps.CHECK and options.filestore_ops != FileOps.ALL:
        return True
    folder = file_list[0]
    if '/' in folder:
        loc = folder.rfind('/')
        folder = folder[:loc]
    else:
        folder = ''
    options.folder = folder
    trace(1, "Verify files for '{0}' in '{1}', folder '{2}'".format(table, options.container, options.folder))

    if options.filestore == FileStore.ADLS_G2:
        files_in_list,files_not_in_list = files_in_azdfs(options.folder, file_list)
    elif options.filestore == FileStore.AZURE_BLOB:
        files_in_list,files_not_in_list = files_in_azblob(options.folder, file_list)
    else:
        files_in_list,files_not_in_list = files_in_s3(options.folder, file_list)

    trace(3, "File check: files_in_list = {}; files_not_in_list = {}".format(files_in_list,files_not_in_list))
    if files_in_list == 0:
        trace(1, "Skipping table {0}; no files in {1}".format(table, options.folder))
        return False
    if files_in_list < len(file_list):
        raise Exception("Not all files in HVR_FILE_NAMES found in {0} for {1}".format(options.folder, table))

    # validate and/or set unmanaged burst table logic
    options.burst_table_set_of_files = False
    if options.unmanaged_burst == 'On':
        if files_not_in_list:
            trace(1, "Files in {0} do not match files in list for table; cannot use performant burst logic".format(options.folder))
        else:
            options.burst_table_set_of_files = True
    if options.unmanaged_burst == 'Auto':
        # if table name is in the folder name, then assume that RenameExpression separates files into separate folders by tablename
        trace(3, "Table name '{}' in folder '{}' = {}".format(table, options.folder, table in options.folder))
        if table in options.folder:
            if files_not_in_list == 0:
                options.burst_table_set_of_files = True
            else:
                trace(1, "Files in {0} do not match files in list for table; cannot use performant burst logic".format(options.folder))
    trace(1, "Use performant unmanaged table for burst = {}".format(options.burst_table_set_of_files))
    return True

def delete_files_from_filestore(file_list):
    if options.filestore_ops != FileOps.DELETE and options.filestore_ops != FileOps.ALL:
        return
    trace(1, "Delete files from {0}".format(options.container))
    if options.filestore == FileStore.ADLS_G2:
        delete_files_from_azdfs(file_list)
    elif options.filestore == FileStore.AZURE_BLOB:
        delete_files_from_azblob(file_list)
    else:
        delete_files_from_s3(file_list)

#
# ODBC functions interacting with Databricks
#
def get_databricks_handles():
    if options.connect_string:
        connect_string = options.connect_string
    else:
        connect_string = "DSN={}".format(options.dsn)
    connect_string += ";UserAgentEntry=HVR"
    try:
        if options.connect_timeout:
            trace(3, "ODBC connect using '{}'; timeout = {} seconds".format(connect_string, options.connect_timeout))
            Connections.odbc = pyodbc.connect(connect_string, autocommit=True, timeout=options.connect_timeout)
        else:
            trace(3, "ODBC connect using '{}'".format(connect_string))
            Connections.odbc = pyodbc.connect(connect_string, autocommit=True)
        Connections.cursor = Connections.odbc.cursor()
    except pyodbc.Error as ex:
        print("Failed to connect using connect string '{}'".format(connect_string))
        raise ex
    if options.database:
        set_database()

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

def set_database():
    set_sql = "USE {}".format(options.database)
    trace(1, set_sql)
    execute_sql(set_sql, 'Use')

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
    if options.no_optype_on_target or options.no_isdeleted_on_target:
        cols = ''
        if options.no_optype_on_target:
            cols += "{} integer".format(options.optype)
        if options.no_isdeleted_on_target:
            if cols:
                cols += ", "
            cols += "{} integer".format(options.isdeleted)
        alter_sql = 'ALTER TABLE {0} ADD COLUMN ({1})'.format(burst_table_name, cols)
        trace(1, "Altering table " + burst_table_name)
        execute_sql(alter_sql, 'Alter')
    if options.load_burst_delay:
        time.sleep(options.load_burst_delay)

def get_col_types(base_name, columns):
    hvr_columns = []
    for col in columns:
        hvr_columns.append(col.lower())
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
            colname = col[0].lower()
            trace(3, "  {0}={1}".format(colname, col))
            if len(col) > 1 and colname in hvr_columns:
                col_types[colname] = col[1]
    except pyodbc.Error as ex:
        print("Desc SQL failed: {1}".format(sql_stmt))
        raise ex
    trace(2, "Column types: {}".format(col_types))
    return hvr_columns, col_types

def set_table_properties(base_name):
    alter_sql = "ALTER TABLE {} SET TBLPROPERTIES ({})".format(base_name, options.set_tblproperties)
    trace(1, "Set TBLPROPERTIES on {}:{}".format(base_name, options.set_tblproperties))
    execute_sql(alter_sql, 'Alter')

def get_create_table_ddl(hvr_table, target_name, columns):
    if not columns:
        raise Exception("No columns found in the repository for table {}".format(hvr_table))
    show_columns(columns)
    for colprop in g_column_props:
       if colprop[C_TBL] == "*" or colprop[C_TBL] == hvr_table:
           columns = apply_column_property(hvr_table, colprop, columns)
    trace(2, '')
    show_columns(columns)
    return target_create_table(target_name, columns)

def recreate_target_table(hvr_table):
    #  get the create table DDL - only drop when successful
    if options.hvr_6:
        target_name, columns = hvr6_get_table_info(hvr_table)
    else:
        target_name = get_target_tablename(hvr_table)
        columns = target_columns(hvr_table)
    create_sql = get_create_table_ddl(hvr_table, target_name, columns)
    trace(1, "Creating table " + target_name)
    execute_sql(create_sql, 'Create')

#
# Process the data
#
def do_multi_delete(burst_table, target_table, columns, keys):
    md_keys = keys[:]
    unused = unused_keys_in_multidelete(target_table)
    print("unused keys {}".format(unused))
    if unused and unused in md_keys:
        md_keys.remove(unused)
    selkeys = ''
    dml = 'Delete'
    for key in md_keys:
        if selkeys:
            selkeys += ","
        selkeys += key
    if options.no_isdeleted_on_target:
        sql = "DELETE FROM "
    else:
        sql = "UPDATE "
        dml = 'Update'
    sql += "{0} AS t SET {1} = 1 WHERE EXISTS (SELECT {2} FROM {3} WHERE ".format(target_table, options.isdeleted, selkeys, burst_table)
    for key in md_keys:
        sql += " t.{0} = {0} AND".format(key)
    sql += " {} = 8)".format(options.optype)

    trace(1, "{0} multi-deletes in {1}".format(dml, target_table))
    execute_sql(sql, dml)

def merge_into_target_from_burst(burst_table, target_table, columns, keylist):
    if options.no_optype_on_target:
        if options.optype in columns:
            columns.remove(options.optype)
    if options.no_isdeleted_on_target:
        if options.isdeleted in columns:
            columns.remove(options.isdeleted)
    keys = keylist.split(',')
    if options.optype in keys:
        keys.remove(options.optype)
    if options.isdeleted in keys:
        keys.remove(options.isdeleted)

    skip_clause = ''
    if multidelete_table(target_table):
        do_multi_delete(burst_table, target_table, columns, keys)
        skip_clause = 'AND b.op_type != 8'

    merge_sql = "MERGE INTO {0} a USING {1} b".format(target_table, burst_table)
    merge_sql += " ON"
    for key in keys:
        merge_sql += " a.`{0}` = b.`{0}` AND".format(key)
    merge_sql = merge_sql[:-4]
    if options.no_isdeleted_on_target:
        merge_sql += " WHEN MATCHED AND b.{} = 0 THEN DELETE".format(options.optype)
    merge_sql += " WHEN MATCHED AND b.{0} != 0 {1} THEN UPDATE".format(options.optype, skip_clause)
    merge_sql += "  SET"
    for col in columns:
        merge_sql += " a.`{0}` = b.`{0}`,".format(col)
    merge_sql = merge_sql[:-1]
    merge_sql += " WHEN NOT MATCHED AND b.{0} != 0 {1} THEN INSERT".format(options.optype, skip_clause)
    merge_sql += "  ("
    for col in columns:
        merge_sql += "`{0}`,".format(col)
    merge_sql = merge_sql[:-1]
    merge_sql += ") VALUES ("
    for col in columns:
        merge_sql += "b.`{0}`,".format(col)
    merge_sql = merge_sql[:-1]
    merge_sql += ")"

    trace(1, "Merging changes from {0} into {1}".format(burst_table, target_table))
    execute_sql(merge_sql, 'Merge')

def define_burst_table(stage_table, target_table, columns, file_list):
    hvr_columns, col_types = get_col_types(target_table, columns)
    stage_sql = ''
    stage_sql += "CREATE TABLE {0} ".format(stage_table)
    stage_sql += "("
    for col in hvr_columns:
        if col in col_types:
            stage_sql += "`{0}` {1},".format(col, col_types[col])
        elif col == options.optype or col == options.isdeleted:
            stage_sql += "`{0}` int,".format(col)
        else:
            stage_sql += "`{0}` string,".format(col)
    stage_sql = stage_sql[:-1]
    stage_sql += ") using {} ".format(options.file_format)
    if options.filestore == FileStore.AZURE_BLOB or (options.filestore == FileStore.ADLS_G2 and options.use_wasb):
        stage_sql += " LOCATION 'wasbs://{0}@{1}.blob.core.windows.net/{2}'".format(options.container, options.resource, options.folder)
    elif options.filestore == FileStore.ADLS_G2:
        stage_sql += " LOCATION 'abfss://{0}@{1}.dfs.core.windows.net/{2}'".format(options.container, options.resource, options.folder)
    else:
        stage_sql += " LOCATION 's3://{0}/{1}' ".format(options.container, options.folder)
    if options.file_format == 'csv':
        if options.line_separator:
            stage_sql += ' OPTIONS (header "true", delimiter "{}", lineSep "{}")'.format(options.delimiter, options.line_separator)
        else:
            stage_sql += ' OPTIONS (header "true", delimiter "{}")'.format(options.delimiter)
    trace(1, "Creating unmanaged burst table {0}".format(stage_table))
    execute_sql(stage_sql, 'Create')

def do_copy_into_sql(load_table, target_table, hvr_columns, col_types, file_list):
    copy_sql = ''
    copy_sql += "COPY INTO {0} FROM ".format(load_table)
    copy_sql += "(SELECT "
    for col in hvr_columns:
        if col in col_types:
            type_func = col_types[col]
            if '(' in type_func:
                copy_sql += "CAST(`{0}` as {1}),".format(col, type_func)
            else:
                copy_sql += "{0}(`{1}`),".format(type_func, col)
        else:
            copy_sql += "`{0}`,".format(col)
    copy_sql = copy_sql[:-1]
    if options.filestore == FileStore.AZURE_BLOB or (options.filestore == FileStore.ADLS_G2 and options.use_wasb):
        copy_sql += " FROM 'wasbs://{0}@{1}.blob.core.windows.net/') ".format(options.container, options.resource)
    elif options.filestore == FileStore.ADLS_G2:
        copy_sql += " FROM 'abfss://{0}@{1}.dfs.core.windows.net/') ".format(options.container, options.resource)
    else:
        copy_sql += " FROM 's3://{0}') ".format(options.container)
    copy_sql += "FILEFORMAT = {} ".format(options.file_format.upper())
    copy_sql += "FILES = ("
    for fname in file_list:
        copy_sql += "'{0}',".format(fname)
    copy_sql = copy_sql[:-1]
    copy_sql += ") "
    if options.file_format == 'csv':
        if options.line_separator:
            copy_sql += "FORMAT_OPTIONS('header' = 'true' , 'inferSchema' = 'true', 'delimiter' = '{}', 'lineSep' = '{}') ".format(options.delimiter, options.line_separator)
        else:
            copy_sql += "FORMAT_OPTIONS('header' = 'true' , 'inferSchema' = 'true', 'delimiter' = '{}') ".format(options.delimiter)
    copy_sql += "COPY_OPTIONS ('force' = 'false')"

    trace(1, "Copying from the file store into " + load_table)
    execute_sql(copy_sql, 'Copy')

def copy_into_delta_table(load_table, target_table, columns, file_list):
    MAX_COPY_FILES = 1000
    hvr_columns, col_types = get_col_types(target_table, columns)
    if len(file_list) <= MAX_COPY_FILES:
        do_copy_into_sql(load_table, target_table, hvr_columns, col_types, file_list)
        return
    for slice in range(0, 1+int(len(file_list)/MAX_COPY_FILES)):
        trace(3, "COPY INTO files {} to {}".format(slice, (slice*MAX_COPY_FILES), (slice+1)*MAX_COPY_FILES-1))
        do_copy_into_sql(load_table, target_table, hvr_columns, col_types, file_list[(slice*MAX_COPY_FILES):(slice+1)*MAX_COPY_FILES])

def process_table(tab_entry, file_list, numrows):
    global file_counter
    
    target_table = tab_entry[0]
    columns = tab_entry[2].split(",")
    load_table = target_table

    t = [0,0,0,0,0,0,0]
    t[0] = timer()
    # if refreshing an empty table, or table already processed, then skip this table
    if len(file_list) == 0 or not files_found_in_filestore(tab_entry[1], file_list):
        return

    t[1] = timer()
    use_burst_logic = options.mode == "integ_end" and not options.target_is_timekey
    if use_burst_logic:
        load_table += '__bur'
    else:
        if options.no_optype_on_target:
            if options.optype in columns:
                columns.remove(options.optype)
        if options.no_isdeleted_on_target:
            if options.isdeleted in columns:
                columns.remove(options.isdeleted)
        for ignore in options.ignore_columns:
            if ignore in columns:
                columns.remove(ignore)

    if use_burst_logic:
        drop_table(load_table)
        if options.burst_table_set_of_files:
            define_burst_table(load_table, target_table, columns, file_list)
        else:
            create_burst_table(load_table, target_table)
    else:
        if options.mode == "refr_write_end":
            if refresh_options.job_name and refresh_options.slices_done > 1:
                trace(1, "First slice has already refreshed, disabling create/truncate target table")
                options.recreate_tables_on_refresh = False
                options.truncate_target_on_refresh = False
                options.set_tblproperties = ''
            if options.recreate_tables_on_refresh:
                # pass the HVR table name - the repository will provide the base_name
                recreate_target_table(tab_entry[1])
            else:
                if options.truncate_target_on_refresh:
                    truncate_table(target_table)
                if options.set_tblproperties:
                    set_table_properties(target_table)
    t[2] = timer()

    if not use_burst_logic or not options.burst_table_set_of_files:
        copy_into_delta_table(load_table, target_table, columns, file_list)
    t[3] = timer()

    if use_burst_logic:
        merge_into_target_from_burst(load_table, target_table, columns, tab_entry[3])
        t[4] = timer()
        drop_table(load_table)
        t[5] = timer()
    else:
        t[4] = t[3]
        t[5] = t[3]

    file_counter += len(file_list)
    delete_files_from_filestore(file_list)
    t[6] = timer()
    trace(3, "All times: {0:.2f}:  {1:.2f} {2:.2f} {3:.2f} {4:.2f} {5:.2f} {6:.2f}".format(t[6]-t[0], t[1]-t[0], t[2]-t[1], t[3]-t[2], t[4]-t[3], t[5]-t[4], t[6]-t[5]))
    if use_burst_logic:
        if options.burst_table_set_of_files:
            trace(0, "Merged {0} changes into '{1}' in {2:.2f} seconds:"
                     " verify files: {3:.2f}s,"
                     " create burst: {4:.2f}s,"
                     " merge into target: {5:.2f}s,"
                     " drop burst: {6:.2f}s".format(numrows, target_table, t[6]-t[0], t[1]-t[0], t[2]-t[1], t[4]-t[3], t[5]-t[4]))
        else:
            trace(0, "Merged {0} changes into '{1}' in {2:.2f} seconds:"
                     " verify files: {3:.2f}s,"
                     " create burst: {4:.2f}s,"
                     " copy into burst: {5:.2f}s,"
                     " merge into target: {6:.2f}s,"
                     " drop burst: {7:.2f}s".format(numrows, target_table, t[6]-t[0], t[1]-t[0], t[2]-t[1], t[3]-t[2], t[4]-t[3], t[5]-t[4]))
    else:
        if options.mode == "refr_write_end":
            init_clause = ''
            if options.truncate_target_on_refresh:
                init_clause = " truncate target: {0:.2f}s,".format(t[2]-t[1])
            if options.recreate_tables_on_refresh:
                init_clause = " create targe: {0:.2f}s,".format(t[2]-t[1])
            trace(0, "Refresh of '{0}', {1} rows, took {2:.2f} seconds:"
                     "{3}"
                     " copy into target: {4:.2f}s".format(target_table, numrows, t[6]-t[0], init_clause, t[3]-t[2]))
        else:
            trace(0, "Copy of {0} rows into {1} took {2:.2f} seconds".format(numrows, target_table, t[5]-t[0]))

def process_tables():
    tbl_map, num_rows = table_file_name_map()
    try:
        for t in tbl_map:
            process_table(t, tbl_map[t], num_rows[t])
    finally:        
        Connections.cursor.close()
        Connections.odbc.close()
        pass

def process(argv):
    version_check()
    process_args(argv)
    env_load()
    trace_input()

    if ((options.mode == "refr_write_end" or options.mode == "integ_end") and
         os.getenv('HVR_FILE_NAMES') != ''):

        if refresh_options.job_name:
            lock(refresh_options.lock_file)
            refresh_options.slices_done = get_slices_done()
        if options.mode == "refr_write_end" and options.recreate_tables_on_refresh:
            initialize_hvr_connect()
            init_createtable_info()
        get_databricks_handles()
        get_filestore_handles()
        process_tables()
        if refresh_options.job_name:
            unlock(refresh_options.lock_file)
            if refresh_options.slices_done == refresh_options.num_slices:
                cleanup_job_files()

        if (file_counter > 0) :
            print("Successfully processed {0:d} file(s)".format(file_counter))

    if options.channel_export:
        os.remove(options.channel_export)

if __name__ == "__main__":
    try:
        process(sys.argv)
        sys.stdout.flush() 
        sys.exit(0) 
    except Exception as err:
        unlock(None)
        cleanup_lock_file()
        sys.stdout.flush() 
        sys.stderr.write("F_JX0D03: {0}\n".format(err))
        sys.stderr.flush()
        sys.exit(1);

