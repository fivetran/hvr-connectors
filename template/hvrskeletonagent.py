#!python
# Copyright (c) 2000-2021 HVR Software bv

################################################################################
# 
# NAME
#     hvrskeletonagent.py
#
# SYNOPSIS
#     as agent
#     python hvrskeletonagent.py mode loc chn
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

class Options:
    mode = ''
    channel = ''
    location = ''
    agent_env = {}
    trace = 0

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
    options.trace = 3
    options.agent_env = load_agent_env()
   
def env_var_print():
    """
    """
    trace(3, "*********  {}  *********".format(options.mode))
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

def process_table(tab_entry, file_list, numrows):
    pass

def process_tables():
    tbl_map, num_rows = table_file_name_map()
    for t in tbl_map:
        process_table(t, tbl_map[t], num_rows[t])

def process(argv):
    version_check()
    process_args(argv)
    env_load()
    env_var_print()

    if ((options.mode == "refr_write_end" or options.mode == "integ_end") and
         os.getenv('HVR_FILE_NAMES', '') != ''):

        process_tables()

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

