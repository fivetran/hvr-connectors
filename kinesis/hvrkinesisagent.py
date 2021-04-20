#!python
# -*- coding: utf-8 -*-

################################################################################
#
# hvrkinesisagent.py
# Copyright (c) 2000-2020 HVR Software bv
#
################################################################################
#
# NAME
#     hvrkinesisagent.py - HVR AWS Kinesis integrate agent
#
# SYNOPSIS
#     as agent
#     python hvrkinesisagent.py mode loc chn
#   
# DESCRIPTION
#
#     This script can be used to send tables via Kinesis message broker
#     The script reads the supplied list of CSV files and converts the content 
#     to JSON format before sending to Kinesis. Each line in the CSV file 
#     represents one Kinesis message.
#
# OPTIONS
#
# AGENT OPERATION
#
#     MODES
#
#     refr_write_end
#        Send data files via Kinesis message broker
#        Drop staging tables 
#
#     integ_end
#        Send data files via Kinesis message broker
#        Drop staging tables 
#
#     LOCATIONS/CHANNELS
#         Command line options loc and chn are ignored
#          
#
# ENVIRONMENT VARIABLES
#     The following environment variables are required by the the agent, or optionally influence the behavior of
#     the agent and should be supplied by channel Environment actions:
#
#     HVR_AWS_REGION              (required)
#        This variable sets the region name where the Kinesis stream exists
#
#     HVR_KINESIS_STREAM          (required)
#        This variable sets a Kinesis stream for a single stream, or accepts the substitution {hvr_tbl_name} to have
#        a stream per integrated table
#      
#     HVR_KINESIS_PARTITION_KEY   (optional)
#        This variable tells the plugin which column's value(s) to use as the Kinesis stream partition key.
#        it can be column names, separated by commas (case sensitive), in which case the same column name is assumed
#        to exists in all the tables being replicated, or if this environment variable is omitted, it will use the
#        primary key column value(s), concatenated together if it's a composite key, separated by '|'.
#
#     HVR_KINESIS_TRACE           (optional)
#        Non-zero enables tracing
#
#     The following environment variables are required by the the agent but are implicitly passed but HVR's agent plugin
#     framework:
#
#     HVR_FILE_LOC                (required)
#        This variable sets a location of files to be sent
#  
#     HVR_TBL_NAMES               (required)       
#        This variable contains list of tables as defined in HVR's metadata
#
#     HVR_BASE_NAMES              (required)
#        This variable contains list of tables as defined as target names.
#
#     HVR_FILE_NAMES              (required)
#        This  variable contains list of files transferred into file system.
#        If empty - integration process is omitted
#
#
# ADDITIONAL METADATA
#     Additional metadata can optionally be added to the JSON message by 
#     including these columns through ColumnProperties :
#       - op_type (usually set to hvr_op_)
#       - op_timestamp (usually set to hvr_cap_timestamp)
#
# AWS KINESIS CREDENTIALS
#     In order to allow this agent to access Kinesis, boto3 must be configured
#     as per the documentation :
#     http://docs.pythonboto.org/en/latest/boto_config_tut.html
#
#
# DIAGNOSTICS
#     TODO
#
################################################################################


import sys
import traceback
import getopt
import os
import time
import csv
import json
import boto3

# env variables
aws_region = ''
kinesis_stream = ''
kinesis_part_key = ''
kinesis_trace = 0

file_counter = 0


# Support functions and classes
class MyCSVDialect(csv.Dialect):
    strict = True
    skipinitialspace = True
    quoting = csv.QUOTE_ALL
    delimiter = ','
    quotechar = '"'
    lineterminator = '\n'


def trace(msg):
    if kinesis_trace >= 1:
        print("TRACE[{0}] : {1}".format(kinesis_trace, msg))
        sys.stdout.flush()


def env_load():
    global kinesis_trace
    global aws_region
    global kinesis_stream
    global kinesis_part_key

    kinesis_trace = int(os.getenv('HVR_KINESIS_TRACE', kinesis_trace))
    aws_region = os.getenv('HVR_AWS_REGION', aws_region)
    kinesis_stream = os.getenv('HVR_KINESIS_STREAM', kinesis_stream)
    kinesis_part_key = os.getenv('HVR_KINESIS_PARTITION_KEY', kinesis_part_key)

    if aws_region == '' or kinesis_stream == '':
        raise Exception(
            "AWS_REGION and HVR_KINESIS_STREAM environment variable must be defined.")


def env_var_print():
    trace("============================================")
    for key, value in os.environ.items():
        if key.find('HVR') != -1:
            trace(key + " = " + value)
    trace("============================================")


def table_name_normalize(name):
    index = name.find(".")
    schema = ''
    if index != -1:
        schema = name[:index]

    return schema, name[index + 1:]


def table_file_name_map():
    # build search map
    hvr_tbl_names = os.getenv('HVR_TBL_NAMES').split(":")
    hvr_base_names = os.getenv('HVR_BASE_NAMES').split(":")
    files = os.getenv('HVR_FILE_NAMES').split(":")
    tbl_map = {}
    for item in zip(hvr_base_names, hvr_tbl_names):
        tbl_map[item] = []
        pop_list = []
        for idx, f in enumerate(files):
            name = f[f.find("-") + 1:]
            if name[-4:].lower() == ".csv":
                name = name[:-4]
            if name.lower() == item[1]:
                tbl_map[item].append(f)
                pop_list.append(idx)
        # Pop files from list from high index to low to maintain index sanity
        for idx in reversed(pop_list):
            files.pop(idx)

    if files:
        raise Exception("$HVR_FILE_NAMES contains unexpected list of files {0}".format(files))

    return tbl_map


def key_col_map():
    base_names = os.getenv('HVR_BASE_NAMES').split(":")
    base_key_names = os.getenv('HVR_TBL_KEYS_BASE').split(":")
    part_key_env_var = os.getenv('HVR_KINESIS_PARTITION_KEY')

    key_cols = {}
    for i, table in enumerate(base_names):
        if part_key_env_var is None:
            #
            # Use the keys passed in in the list of primary keys for this table
            #
            trace("Using pks from env var list. Table={} key name={}".format(table,base_key_names[i]))
            key_cols[table] = base_key_names[i]
        else:
            # Use the key column supplied by the user in HVR_KINESIS_PARTITION_KEY
            trace("Using pks from user supplied env var list. Table={} key name={}".format(table,part_key_env_var))
            key_cols[table] = part_key_env_var

    for table in key_cols:
        trace("key_col_map : table={} key_cols[table]={}".format(table, key_cols[table]))

    return key_cols


def send_msg(connection, stream, content, hvr_base_table, key_cols):
    # Send a row as an individual JSON messages
    for row in content:
        msg_dict = {'columns': row}
        if 'op_type' in row:
            msg_dict['op_type'] = msg_dict['columns'].pop('op_type')
        if 'op_timestamp' in row:
            msg_dict['op_timestamp'] = msg_dict['columns'].pop('op_timestamp')
        if stream != hvr_base_table:
            # Include an extra 'tablename' column only if the stream name is not the table name
            msg_dict['tablename'] = hvr_base_table

        #
        # Concatenate the key_col values together separated by '|' to form the partition key value
        #
        col_list = key_cols[hvr_base_table].split(",")
        part_key = ''
        for i, key_name in enumerate(col_list):
            if key_name in row:
                trace("Found key_name={} with row value={}".format(key_name,row[key_name]))
                key_value = row[key_name]
            else:
                trace("Didn't found key_name={0} in row".format(key_name))
                key_value = 'Undefined'
            if i == 0:
                part_key += key_value
            else:
                part_key += '|' + key_value

        trace("Partition key={0}".format(part_key))

        connection.put_record(
            StreamName=stream,
            Data=json.dumps(msg_dict, sort_keys=False),
            PartitionKey=part_key)


def read_file(connection, file_loc, key_cols):
    global file_counter

    tbl_map = table_file_name_map()

    for t in tbl_map:
        for name in tbl_map[t]:
            base_name = table_name_normalize(t[0])
            hvr_schema = base_name[0]
            hvr_base_table = base_name[1]

            #
            # Plug in the table name if {hvr_tbl_name} has been supplied, otherwise it get left as supplied
            #
            stream = kinesis_stream.replace('{hvr_tbl_name}', hvr_base_table)

            full_name = file_loc + '/' + name
            trace("Reading and parsing file '" + full_name + "' ... ")

            try:
                with open(full_name) as csv_file:
                    content = csv.DictReader(csv_file, None, None, MyCSVDialect())
                    send_msg(connection, stream, content, hvr_base_table, key_cols)
                    trace("CSV file processing finished for:" + full_name)
                # remove successfully transmitted file
                os.remove(full_name)
                file_counter = file_counter + 1

            except IOError as err:
                raise Exception("Couldn't open file " + full_name)


def file_loc_cleanup():
    file_loc = os.getenv('HVR_FILE_LOC')
    files = os.listdir(file_loc)
    for name in files:
        if name == "." or name == ".." or os.path.isdir(name):
            continue

        full_name = file_loc + "/" + name
        os.remove(full_name)


def file_loc_processing(key_cols):
    file_loc = os.getenv('HVR_FILE_LOC')
    connection = boto3.client('kinesis', region_name=aws_region)
    read_file(connection, file_loc, key_cols)


def main(argv):
    env_load()
    if kinesis_trace > 0:
        trace("Invocation mode ={0}".format(argv))
    if argv == "refr_write_end" or argv == "integ_end":
        if kinesis_trace > 0:
            env_var_print()
        if os.getenv('HVR_FILE_NAMES') != '':
            key_cols = key_col_map()
            file_loc_processing(key_cols)
            if file_counter > 0:
                print("Successfully transmitted {0:d} file(s)".format(file_counter))

    if argv == "refr_write_begin":
        file_loc_cleanup()


#
# Main entry point
#
if __name__ == "__main__":
    try:
        main(sys.argv[1])
        sys.stdout.flush()
        sys.exit(0)
    except Exception as err:
        tb = traceback.format_exc()
        sys.stdout.flush()
        sys.stderr.write("F_JX0D01: {0}\n".format(err))
        sys.stderr.write("F_JX0D01: {0}\n".format(tb))
        sys.stderr.flush()
        sys.exit(1)
