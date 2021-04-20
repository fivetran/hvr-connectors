#!python


# HVR 5.7.5/4 (windows-x64-64bit) hvrcouchbasedbagent.py@37998 2020-01-28
# Copyright (c) 2000-2020 HVR Software bv

################################################################################
#
#     HVR 5.7.5/4 (windows-x64-64bit)
#     Copyright (c) 2000-2020 HVR Software bv
#
################################################################################
# 
# NAME
#     hvrcouchbasedbagent.py - HVR Couchbase integrate agent
#
# SYNOPSIS
#     as agent
#     python hvrcouchbasedbagent.py mode loc chn agrs
#   
# DESCRIPTION
#
#     This script can be used to send table rows into Couchbase
#
#
# OPTIONS
#
# AGENT OPERATION
#
#     MODES
#
#     refr_write_end
#     integ_end
#        Send data files into Couchbase
#        Drop staging tables 
#
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#          
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_COUCHBASE_DATABASE       (required)
#        This variable sets a Couchbase database name
#      
#     HVR_COUCHBASE_HOST        (required)
#        This variable sets a Couchbase host name
#
#     HVR_COUCHBASE_PORT        (optional, default: 27017)
#        This variable sets a Couchbase port
#
#     HVR_COUCHBASE_USER        (optional, default: Administrator)
#        This variable sets a Couchbase User
#
#     HVR_COUCHBASE_PASSWORD        (optional, default: )
#        This variable sets a Couchbase password
#
#     HVR_COUCHBASE_COLLECTION  (required)
#        If set all data are delivered into collection, 
#        Support special substitutions: {hvr_tbl_name}, {hvr_base_name} and {hvr_schema}
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
#        This  variable contains list of files transfered into Couchbase.
#        If empty - intergarion process is omitted
#
#     HVR_COUCHBASE_TRACE        (advanced,optional, default:0)
#         See the DIAGNOSTICS section.
#
#
################################################################################

import sys
import getopt
import os
import traceback
import json
from importlib import reload

from enum import Enum

# needed for any cluster connection
from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator

# needed to support SQL++ (N1QL) query
from couchbase.cluster import QueryOptions

# json files are encoded in UTF-8 by default
reload(sys)


# sys.setdefaultencoding("utf-8")


class SetupMode(Enum):
    MODE_NOT_DEFINED = 0
    MODE_TIMEKEY = 1
    MODE_SOFTDELETE = 2


class Options:
    # env variables
    port = 8091
    host = ''
    database = ''
    user = 'Administrator'
    password = ''
    collection = ''
    trace = 0
    recreate = False
    mode = SetupMode.MODE_TIMEKEY
    soft_column = ''
    agent_env = None


file_counter = 0
options = Options()


##### Support functions and classes ############################################

def trace(msg):
    if options.trace >= 1:
        print(msg)
        sys.stdout.flush()


def load_agent_env():
    agent_env = {}

    if 'HVR_LONG_ENVIRONMENT' in os.environ:
        hvr_long_environment = os.environ['HVR_LONG_ENVIRONMENT']
        try:
            with open(hvr_long_environment, "r") as f:
                long_env = json.loads(f.read())

            for k, v in long_env.items():
                agent_env[str(k)] = str(v)

        except Exception as e:
            sys.stderr.write(("W_JX0E00: Warning An error occured while "
                              "processing $HVR_LONG_ENVIRONMENT file "
                              "'{}'. Will continue without processing this "
                              "file. Error: {} {}").format(
                hvr_long_environment,
                str(e),
                traceback.format_exc()))

    for k, v in os.environ.items():
        k = str(k)
        if k not in agent_env:
            agent_env[k] = str(v)

    return agent_env


def env_load():
    options.trace = int(os.getenv('HVR_COUCHBASE_TRACE', options.trace))
    options.port = int(os.getenv('HVR_COUCHBASE_PORT', options.port))
    options.host = os.getenv('HVR_COUCHBASE_HOST', options.host)
    options.database = os.getenv('HVR_COUCHBASE_DATABASE', options.database)
    options.user = os.getenv('HVR_COUCHBASE_USER', options.user)
    options.password = os.getenv('HVR_COUCHBASE_PASSWORD', options.password)
    options.collection = os.getenv('HVR_COUCHBASE_COLLECTION', options.collection)
    options.agent_env = load_agent_env()

    if (options.host == '' or options.database == '' or options.collection == ''):
        raise Exception(
            "HVR_COUCHBASE_HOST and HVR_COUCHBASE_DATABASE and HVR_COUCHBASE_COLLECTION enviroment variables must be defined")

    return options


def env_var_print():
    trace("============================================")
    for key, value in os.environ.items():
        if key.find('HVR') != -1:
            trace(key + " = " + value)
    trace("============================================")


##### Main functions ###########################################################
def table_name_normalize(name):
    index = name.find(".")
    schema = ''
    if index != -1:
        schema = name[:index]

    return schema, name[index + 1:]


def table_file_name_map():
    # build search map

    hvr_tbl_names = options.agent_env['HVR_TBL_NAMES'].split(":")
    hvr_base_names = options.agent_env['HVR_BASE_NAMES'].split(":")
    hvr_col_names = options.agent_env['HVR_COL_NAMES'].split(":")
    hvr_tbl_keys = options.agent_env['HVR_TBL_KEYS'].split(":")
    files = options.agent_env['HVR_FILE_NAMES'].split(":")
    suffix_len = len(".json")

    tbl_map = {}
    for item in zip(hvr_base_names, hvr_tbl_names, hvr_col_names, hvr_tbl_keys):
        tbl_map[item] = []
        pop_list = []
        for idx, f in enumerate(files):
            name = f[f.find("-") + 1:]
            if name[-suffix_len:] == ".json":
                name = name[:-suffix_len]
            if name == item[1]:
                tbl_map[item].append(f)
                pop_list.append(idx)
        # Pop files from list from high index to low to maintain index sanity
        for idx in reversed(pop_list):
            files.pop(idx)

    if files:
        raise Exception("$HVR_FILE_NAMES contains unexpected list of files: {0}".format(files))

    return tbl_map


def file_loc_processing():
    global file_counter
    # get a reference to our cluster
    cluster = Cluster('couchbase://' + options.host, ClusterOptions(
        PasswordAuthenticator(options.user, options.password)))

    file_loc = options.agent_env['HVR_FILE_LOC']
    tbl_map = table_file_name_map()

    client = cluster.bucket(options.database)
    db = cb_coll = client.default_collection()

    for t in tbl_map:
        base_name = table_name_normalize(t[0])
        hvr_schema = base_name[0]
        hvr_base_table = base_name[1]
        collection = options.collection.format(hvr_schema=hvr_schema,
                                               hvr_base_name=hvr_base_table,
                                               hvr_tbl_name=t[1])
        keys = t[3].split(",")
        # if collection is absent - no any error
        #       if options.recreate :
        #           trace("Dropping collection '{0}'".format(collection))
        #           db.drop_collection(collection)

        for name in tbl_map[t]:
            full_name = file_loc + '/' + name
            trace("Reading and parsing file '" + full_name + "' ... ")
            try:
                with open(full_name) as json_file:
                    for line in json_file:
                        json_obj = json.loads(line)
                        try:
                            #if options.mode == SetupMode.MODE_SOFTDELETE :
                            #added new pseudo column
                            column_id = ''
                            for column in keys:
                                column_id = column_id + "::" +str(json_obj[column])
                            #trace("key will equal:" + str(column_id.replace('::', "", 1)) + " /n")
                            mykey= str(hvr_base_table) + str(column_id)
                            trace("key will equal:" + mykey + " /n")
                            result = db.upsert(mykey, str(json_obj))
                            #trace(str(result.cas) + " mykey:" + str(mykey) + " data:" + str(json_obj))
                        except Exception as e:
                            trace(e)
                # remove successfully transmitted file
                os.remove(full_name)
                file_counter = file_counter + 1
            except IOError as err:
                raise Exception("Couldn't open file " + full_name)


def file_loc_cleanup():
    file_loc = options.agent_env['HVR_FILE_LOC']
    files = os.listdir(file_loc)
    for name in files:
        if (name == "." or name == ".." or os.path.isdir(name) == True):
            continue

        full_name = file_loc + "/" + name
        os.remove(full_name)


def userargs_parse(cmdargs):
    try:
        list_args = cmdargs[0].split(" ");
        opts, args = getopt.getopt(list_args, "rs:")
    except getopt.GetoptError:
        raise Exception("Couldn't parse command line agruments")

    for opt, arg in opts:
        if opt == '-r':
            options.recreate = True
        if opt == '-s':
            options.mode = SetupMode.MODE_SOFTDELETE
            options.soft_column = arg

    return options


def main(argv, userarg):
    env_load()
    userargs_parse(userarg)
    if options.trace >= 1:
        env_var_print()

    if ((argv == "refr_write_end" or argv == "integ_end") and
            options.agent_env.get('HVR_FILE_NAMES', '') != ''):
        file_loc_processing()
        if file_counter > 0:
            print("Successfully transmitted {0:d} file(s)".format(file_counter))

    if argv == "refr_write_begin":
        file_loc_cleanup()


if __name__ == "__main__":
    try:
        main(sys.argv[1], sys.argv[4:])
        sys.stdout.flush()
        sys.exit(0)
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush()
        sys.stderr.write("F_JX0D01: {0}\n".format(err))
        trace("{0}\n".format(tb))
        sys.stderr.flush()
        sys.exit(1)
