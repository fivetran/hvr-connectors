#!python
# $Id$
# Copyright (c) 2000-2017 HVR Software bv

################################################################################
#
#     &{return $DEF{HVR_VER_LONG}}&
#     &{return $DEF{HVR_COPYRIGHT}}&
#
################################################################################
#
# NAME
#     hvrhiveorcagent.py - HVR Hive ORC DB integrate agent
#
# SYNOPSIS
#     as agent
#     python hvrhiveorcagent.py mode loc chn args
#
# DESCRIPTION
#
#     This script can be used to convert CDC data integrated to Hive/CSV tables
#     into Hive/ORC tables.
#     This was created as a one off proof of concept rather than intended for 
#     production purposes. It has some limitations:
#       - The target ORC tables are assumed to be precreated
#       - Only username/password authentication to Hive is supported 
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
#        Copies CSV formatted Hive tables to ORC formatted Hive tables.
#
#     integ_end
#        Copies CSV formatted Hive tables to ORC formatted Hive tables.
#
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required for correct operation of
#     the agent, or influence the behavior of the agent:
#
#     HVR_HIVE_ORC_DB        (required)
#        This variable sets the Hive database name.
#
#     HVR_HIVE_ORC_USER      (required)
#        This variable sets the password used to access the Hive database.
#
#     HVR_HIVE_ORC_PASSWD    (optional)
#        This variable sets the password used to access the Hive database.
#
#     HVR_HIVE_ORC_SUFFIX    (optional)
#        This is the suffix added to the table name for the ORC tables.
#        The default suffix is '_ORC'
#
#     HVR_FILE_LOC           (required, but not from user parameter)
#        This varibale sets a location of files to be sent
#
#     HVR_TBL_NAMES          (required, but not from user parameter)
#        This  variable contains list of tables
#
#     HVR_BASE_NAMES         (required, but not from user parameter)
#        This  variable contains list of tables
#
#     HVR_TBL_KEYS           (required, but not from user parameter)
#        This  variable contains list of key columns
#
#     HVR_COL_NAMES          (required, but not from user parameter)
#        This  variable contains list of columns
#
#     HVR_SCHEMA             (optional, but not from user parameter)
#        This  variable sets schema name
#
#     HVR_FILE_NAMES         (required, but not from user parameter)
#        This  variable contains list of files transfered into HDFS.
#        If empty - integration process is  omitted
#
#     HVR_HIVE_ORC_TRACE     (advanced,optional, default:0)
#
#
################################################################################
import sys
import getopt
import os
import traceback
import subprocess
import pyhs2

class Options:
    trace = 0
    hive_db = ''
    hive_user = ''
    hive_passwd = ''
    orc_suffix = '_ORC'
    file_loc = ''

file_counter = 0
options = Options()

##### Support function #########################################################

def trace(level, msg):
    if options.trace >= level:
        print(msg)

def env_load():

    options.trace = int(os.getenv('HVR_HIVE_ORC_TRACE', options.trace))
    options.hive_db = os.getenv('HVR_HIVE_ORC_DB', options.hive_db)
    options.hive_user = os.getenv('HVR_HIVE_ORC_USER', options.hive_user)
    options.hive_passwd = os.getenv('HVR_HIVE_ORC_PASSWD', options.hive_passwd)
    options.file_loc = os.getenv('HVR_FILE_LOC', options.file_loc)

    if (options.hive_db == '') :
        raise Exception("HVR_HIVE_ORC_DB enviroment variable must be defined")

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

def run_cmd(args_list):
    trace(3,'Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
    (output, errors) = proc.communicate()
    if proc.returncode:
        raise RuntimeError(
            'Error running command: %s. Return code: %d, Error: %s' % (
                ' '.join(args_list), proc.returncode, errors))
    return (output, errors)

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
    for item in zip(hvr_base_names, hvr_tbl_names):
        tbl_map[item[1]] = []
        if files :
            pop_list = []
            for idx, f in enumerate(files):
                name = f[:f.find("/")]
                trace(3,"name={0} item[0]={1} item[1]={2}".format(name, item[0], item[1]))
                if name == item[1]:
                    tbl_map[item[1]].append(f)
                    pop_list.append(idx)
            # Pop files from list from high index to low to maintain index sanity
            for idx in reversed(pop_list):
                files.pop(idx)

    if files :
        raise Exception ("$HVR_FILE_NAMES contains unexpected list of files {0}".format(files))
    trace(3,tbl_map)
    return tbl_map


def delete_csv_files(file_list, file_loc):
    for file in file_list:
        file_name = file_loc + "/" + file
        trace(2,"Deleting file {0}".format(file_name))
        (out, errors)= run_cmd(['hadoop', 'fs', '-rm', '-r', file_name])


def process_tables():
    tbl_map = table_file_name_map()
    with pyhs2.connect(host='localhost',
                    port=10000,
                    authMechanism="PLAIN",
                    user=options.hive_user,
                    password=options.hive_passwd,
                    database=options.hive_db) as conn:
        with conn.cursor() as cur:
            try:
                for table in tbl_map:
                    trace(1,"Copying table {0} ...".format(table))
                    cur.execute("INSERT INTO {0}{1} SELECT * FROM {0}".format(table,options.orc_suffix))
                    delete_csv_files(tbl_map[table], options.file_loc)
            finally:
                conn.close()


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
    if ((argv == "refr_write_end" or argv == "integ_end") and
         os.getenv('HVR_FILE_NAMES') != ''):

        process_tables()

        if (file_counter > 0) :
            print ("Successfully transmitted {0:d} file(s)".format(file_counter))


if __name__ == "__main__":
    try:
        process(sys.argv[1], sys.argv[4:])
        sys.stdout.flush()
        sys.exit(0)
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush()
        sys.stderr.write("F_JX0D01: {0}\n".format(err))
        trace(1,"{0}\n".format(tb))
        sys.stderr.flush()
        sys.exit(1)
