#!/usr/bin/env python
# -*- coding: utf-8 -*-

################################################################################
#
# hvrkinesisagent.py
# Copyright (c) 2000-2020 HVR Software bv
#
################################################################################
#
# NAME
#               hvrkinesisagent.py - HVR AWS Kinesis integrate agent with S3 support
#
# SYNOPSIS
#               as agent
#               python hvrkinesisagent.py mode loc chn
#

# DESCRIPTION
#               This script can be used to send tables via Kinesis message broker
#               and/or Amazon S3 storage.
#               The script reads the supplied list of JSON files and sends the content
#               to Kinesis and/or S3. Each entry in the JSON file represents one Kinesis message
#               or one entry in the S3 JSON file.
#
# OPTIONS
#
# AGENT OPERATION
#
#               MODES
#
#               refr_write_end
#                     Send data files via Kinesis message broker and/or S3
#                     Drop staging tables
#
#               integ_end
#                     Send data files via Kinesis message broker and/or S3
#                     Drop staging tables
#
#               LOCATIONS/CHANNELS
#                         Command line options loc and chn are ignored
#
#
# ENVIRONMENT VARIABLES
#               The following environment variables are required by the the agent, or optionally influence the behavior of
#               the agent and should be supplied by channel Environment actions:
#
#               HVR_AWS_REGION                                   (required)
#                     This variable sets the region name where the Kinesis stream and/or S3 bucket exists
#
#               HVR_KINESIS_STREAM                         (required for Kinesis)
#                     This variable sets a Kinesis stream for a single stream, or accepts the substitution {hvr_tbl_name} to have
#                     a stream per integrated table
#
#               HVR_KINESIS_PARTITION_KEY         (optional)
#                     This variable tells the plugin which column's value(s) to use as the Kinesis stream partition key.
#                     it can be column names, separated by commas (case sensitive), in which case the same column name is assumed
#                     to exists in all the tables being replicated, or if this environment variable is omitted, it will use the
#                     primary key column value(s), concatenated together if it's a composite key, separated by '|'.
#
#               HVR_KINESIS_TRACE                             (optional)
#                     Non-zero enables tracing
#
#               HVR_S3_ENABLED                                   (optional)
#                     Set to 1 to enable S3 output. Default is 0 (disabled).
#
#               HVR_S3_BUCKET                                       (required for S3)
#                     This variable sets the S3 bucket where JSON files will be stored
#
#               HVR_S3_PREFIX                                       (optional)
#                     This variable sets the prefix (folder path) within the S3 bucket
#                     Default is 'hvr-data/'
#
#               HVR_S3_FILE_FORMAT                         (optional)
#                     This variable sets the format for S3 file names. Can include:
#                     {table} - table name
#                     {timestamp} - current timestamp
#                     Default is '{table}/{timestamp}.json'
#
#               The following environment variables are required by the the agent but are implicitly passed but HVR's agent plugin
#               framework:
#
#               HVR_FILE_LOC                                        (required)
#                     This variable sets a location of files to be sent
#
#               HVR_TBL_NAMES                                       (required)
#                     This variable contains list of tables as defined in HVR's metadata
#
#               HVR_BASE_NAMES                                   (required)
#                     This variable contains list of tables as defined as target names.
#
#               HVR_FILE_NAMES                                   (required)
#                     This     variable contains list of files transferred into file system.
#                     If empty - integration process is omitted
#
#
# ADDITIONAL METADATA
#               Additional metadata can optionally be added to the JSON message by
#               including these columns through ColumnProperties :
#                    - op_type (usually set to hvr_op_)
#                    - op_timestamp (usually set to hvr_cap_timestamp)
#
# AWS CREDENTIALS
#               In order to allow this agent to access Kinesis and S3, boto3 must be configured
#               as per the documentation :
#               http://docs.pythonboto.org/en/latest/boto_config_tut.html
#
#
# DIAGNOSTICS
#               TODO
#
################################################################################

from functools import total_ordering
import sys
import traceback
import os
import time
import json
import math
import random
import boto3  # AWS SDK for Python
from botocore.exceptions import ClientError
import shutil  # For high-level file operations (e.g., deleting directories)
import re  # Regular expression operations
from datetime import datetime
from pathlib import Path
import ast
from sys import getsizeof

# Global variables and constants
json_header = {
    # Defines a header structure for JSON messages, containing metadata about the data flow.
    # This includes information about the source database, timestamps, event references,
    # and operation types.
    "flowFuncName": "RAW_CBR",
    "flowTechName": {
        "db": {
            "alias": "DEV-SQL",
            "server": "SQLSERV01",
            "instance": "DEFAULT",
            "base": "DEV",
            "engine": "SQLServer",
            "source": "SOURCE",
            "zone": "",
            "techName": "Company_X"
        },
        "flatfile": {
            "serverType": "",
            "connexionString": "",
            "sourceFolder": "",
            "souceApplication": "",
            "techName": ""
        },
        "realtime": {
            "siteName": "",
            "engine": "",
            "captor": "",
            "sourceApplication": "",
            "techName": ""
        }
    },
    "instanceType": "DEV",
    "modType": {
        "mod": "",
        "batchNumber": ""
    },
    "eventTimestamp": "20250423142047000000",
    "flowTimestamp": "20250423144249354000",
    "timezone": "+00:00",
    "transformTimestamp": "",
    "eventUniqueReferenceID": "DEV-0012303A00133545000F-0000000001",
    "eventType": "UPDATE",
    "user": "DEV_usr",
    "program": "DEV_prg",
    "job": "DEV_job",
    "sourceReference": {
        "table": "DISPO",
        "schema": "dbo",
        "filename": "",
        "realtime": ""
    },
    "schemaVersion": "",
    "CDC_fields": {}  # Added empty CDC_fields dictionary for change data capture specific fields
}

# Mapping of HVR operation codes to descriptive event types.
op_dic = {"0": "DELETE", "1": "INSERT", "2": "UPDATE",
          "3": "BEFORE UPDATE", "4": "BEFORE UPDATE"}
file_counter = 0  # Counter for successfully processed files.


def utf8len(s) -> int:
    """Get the sting size in bytes"""
    return len(s.encode('utf-8'))


def remove_path(path) -> None:
    """
    Removes a file or a directory specified by the given path.
    Prints a message indicating the outcome of the operation.

    Args:
        path (str): The path to the file or directory to be removed.
    """
    if os.path.isfile(path):
        os.remove(path)
        print(f"File {path} removed")
        print('*' * 80)
    elif os.path.isdir(path):
        shutil.rmtree(path)  # Recursively remove directory
        print(f"Directory {path} removed")
        print('*' * 80)
    else:
        print(f"Path {path} does not exist")
        print('*' * 80)


def trace(msg) -> None:
    """
    Prints a trace message to stdout if kinesis_trace level is enabled.
    This is used for debugging and logging internal script operations.

    Args:
        msg (str): The message to be traced.
    """
    if kinesis_trace >= 1:
        print("TRACE[{0}] : {1}".format(kinesis_trace, msg))
        sys.stdout.flush()  # Ensure the message is immediately written to stdout


def concatenate(iterable, sep="-") -> list:
    """
    Concatenates elements of an iterable into a single string, separated by a specified delimiter.
    This function has an unusual return type of `list` but its implementation
    returns a string. It might be a historical anomaly or a specific requirement.
    Assuming the intent is to return a string.

    Args:
        iterable (list): A list of strings to be concatenated.
        sep (str, optional): The separator to use between elements. Defaults to "-".

    Returns:
        str: A single string formed by concatenating the iterable elements.
    """
    seq = iterable[0]
    for word in iterable[1:]:
        seq += (sep + word)
    # This should return a string, not a list. Original code returns a string.
    return seq


def get_secret_value(secret_name, key_name) -> str:
    """
    Retrieves a specific key's value from a secret stored in AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret to retrieve.
        key_name (str): The specific key within the secret's JSON string whose value is needed.

    Returns:
        str: The value associated with the key, or an error message if retrieval fails
             or the key is not found.
    """
    # Create a Secrets Manager client using the 'eu-west-3' region.
    # Note: The region is hardcoded here, consider making it dynamic if needed.
    client = boto3.client('secretsmanager', region_name="eu-west-3")

    try:
        # Attempt to retrieve the secret by its ID.
        response = client.get_secret_value(SecretId=secret_name)

        # Check if 'SecretString' is present in the response.
        if 'SecretString' in response:
            try:
                # Parse the SecretString, which is expected to be a JSON string.
                secret = json.loads(response['SecretString'], strict=False)

                # Check if the desired key exists in the parsed secret.
                if key_name in secret:
                    return secret[key_name]
                else:
                    return f"Key '{key_name}' not found."
            except json.JSONDecodeError as json_err:
                # Handle errors during JSON parsing of the secret string.
                return f"Error parsing secret JSON: {str(json_err)}"
        else:
            # Handle cases where the secret string is not found in the response.
            return "Secret cannot be processed."

    except Exception as e:
        # Catch any other exceptions during secret retrieval.
        return f"Error retrieving secret: {str(e)}"


def arg_load() -> None:
    """
    Loads configuration parameters from command-line arguments (sys.argv)
    and sets up global variables for Kinesis and S3 operations.
    It also fetches AWS credentials from Secrets Manager.

    Raises:
        Exception: If required environment variables (AWS_REGION, KINESIS_STREAM,
                   S3_BUCKET when S3 is enabled) are not defined.
    """
    global kinesis_trace
    global aws_region
    global kinesis_stream
    global kinesis_endpoint_url
    global kinesis_access_key_id
    global kinesis_secret_access_key
    global kinesis_part_key
    global s3_enabled
    global s3_bucket
    global s3_prefix
    global s3_file_format
    global cdc_tool_version
    global cdc_tool
    global secret_name
    global environment
    global server
    global base
    global db
    global successfully_sent_records
    global max_retries
    global base_delay   # sec
    global batch_size
    global alias
    # Retrieve the record count (in the event of a retry)
    counter_path = f"{os.getenv('HVR_FILE_LOC')}/counter_{os.getenv('HVR_AGENT_BEGIN_TSTAMP')}.txt"
    if Path(counter_path).exists():
        with open(counter_path, 'r') as file:
            successfully_sent_records = int(file.read())
    else:
        successfully_sent_records = 0

    # Load environment and user arguments
    # Convert the argument string to a dictionary
    args = ast.literal_eval(sys.argv[4])
    environment = args.get('environment')
    aws_region = args.get('aws_region')
    kinesis_stream = args.get('kinesis_stream')
    kinesis_endpoint_url = args.get('kinesis_endpoint_url')
    plaque = args.get('plaque')
    secret_name = f"/my-data-capture/{environment}/service/my-data-capture-{environment}/vault-secret"

    alias = args.get('alias')
    server = args.get('server')
    instance = args.get('instance')
    base = args.get('base')

    # Construct key names for fetching Kinesis credentials from Secrets Manager
    kinesis_id = f"KINESIS_ACCESS_KEY_ID_{plaque}"
    kinesis_secret = f"KINESIS_SECRET_ACCESS_KEY_{plaque}"

    # Retrieve AWS access key ID and secret access key from Secrets Manager
    kinesis_access_key_id = get_secret_value(secret_name, kinesis_id)
    kinesis_secret_access_key = get_secret_value(secret_name, kinesis_secret)

    max_retries = args.get('max_retries')
    base_delay = args.get('base_delay')  # seconds
    batch_size = args.get('batch_size')  # seconds

    s3_enabled = args.get('s3_enabled')
    kinesis_trace = args.get('kinesis_trace')
    # Initialize Kinesis partition key (can be overridden by env var later)
    kinesis_part_key = ''
    cdc_tool_version = os.getenv('HVR_VERSION')
    cdc_tool = 'Fivetran'

    # Populate the 'db' dictionary within the global json_header with parsed values
    db = {
        "alias": alias,
        "server": server,
        "instance": instance,
        "base": base,
        "engine": "SQLServer",
        "source": "SOURCE",
        "zone": "",
        "techName": "Company_X"
    }

    # S3 specific variables setup (mostly hardcoded based on environment)
    # S3 bucket name dynamically formed
    s3_bucket = f'hsi-my-data-capture-{environment}'
    s3_prefix = 'hvr-data/'  # Default S3 prefix (folder path)
    # S3 file naming format
    s3_file_format = 'fivetran-logs/{table}/{timestamp}.json'

    # Validate required parameters
    if aws_region == '':
        raise Exception("HVR_AWS_REGION environment variable must be defined.")

    if kinesis_stream == '':
        raise Exception("HVR_KINESIS_STREAM must be defined.")

    if s3_enabled and (s3_bucket == ''):
        raise Exception(
            "HVR_S3_BUCKET environment variable must be defined when HVR_S3_ENABLED is set.")


def env_var_print() -> None:
    """
    Prints the values of environment variables starting with 'HVR_' for tracing purposes.
    """
    trace("============================================")
    for key, value in os.environ.items():
        if key.find('HVR') != -1:  # Checks if 'HVR' is in the environment variable name
            trace(key + " = " + value)
    trace("============================================")


def table_name_normalize(name) -> tuple:
    """
    Normalizes a table name by separating the schema from the table name.
    If a schema is present (indicated by a dot), it extracts both.

    Args:
        name (str): The full table name, possibly including a schema (e.g., 'schema.table').

    Returns:
        tuple: A tuple containing (schema, table_name). Schema will be an empty string
               if not present in the input name.
    """
    index = name.find(".")
    schema = ''
    if index != -1:
        schema = name[:index]

    return schema, name[index + 1:]


def table_file_name_map() -> dict:
    """
    Builds a mapping between HVR table names (both base and original)
    and the corresponding JSON files located in HVR_FILE_LOC.
    It filters files based on a specific naming pattern and timestamp.

    Returns:
        dict: A dictionary where keys are tuples (hvr_base_name, hvr_tbl_name, hvr_cap_tstamp)
              and values are lists of relevant file names.
    """
    global total_updated_rows

    # Get lists of table names, base names, and capture timestamps from environment variables
    hvr_tbl_names = os.getenv('HVR_TBL_NAMES').split(":")
    hvr_base_names = os.getenv('HVR_BASE_NAMES').split(":")
    hvr_tbl_narows = os.getenv('HVR_TBL_NROWS').split(":")
    hvr_tbl_cap_tstamp = []
    if os.getenv('HVR_TBL_CAP_TSTAMP'):
        hvr_tbl_cap_tstamp = os.getenv('HVR_TBL_CAP_TSTAMP').split(":")
    else:
        hvr_tbl_cap_tstamp = ["000"] * \
            len(os.getenv('HVR_BASE_NAMES').split(":"))
    total_updated_rows = sum([int(idx) for idx in hvr_tbl_narows])

    # List all files in HVR_FILE_LOC that match the Regex pattern - Expected file names: 'channel-cap_tstamp-intgtstamp-sequence-tablename.json'
    file_pattern = re.compile(r'^[a-zA-Z0-9_]+-\d+-\d+-\d+-[a-zA-Z_]+\.json$')
    files = [f for f in sorted(os.listdir(
        os.getenv('HVR_FILE_LOC'))) if file_pattern.match(f)]
    tbl_map = {}  # Dictionary to store the mapping

    # Iterate through the combined HVR metadata (base name, table name, capture timestamp)
    for item in zip(hvr_base_names, hvr_tbl_names, hvr_tbl_cap_tstamp):
        tbl_map[item] = []
        pop_list = []

        # Iterate through available files to find those belonging to the current table
        for idx, f in enumerate(files):
            # Extract integration timestamp from file name
            intg_tstamp = f.split("-")[1]
            name = f.split("-")[4]  # Extract table name from file name
            if name[-5:].lower() == ".json":
                name = name[:-5]  # Remove '.json' extension
            # Check if the file's table name matches and cap_tstamp is greater than the baseline
            if name.lower() == item[1] and intg_tstamp >= item[2]:
                tbl_map[item].append(f)  # Add file to the map
                # Mark index for removal from the `files` list
                pop_list.append(idx)

        # Remove processed files from the `files` list to avoid reprocessing and to identify unexpected files. Iterating in reverse prevents index issues.
        for idx in reversed(pop_list):
            files.pop(idx)

        trace(f"tbl_map: {tbl_map}")

    # If any files remain in the `files` list, it means they were not mapped to any HVR table.
    if files:
        trace(f"{os.getenv('HVR_FILE_LOC')} contains unexpected list of files {files}")

    return tbl_map


def key_col_map() -> dict:
    """
    Determines the partition key columns for each HVR table.
    It prioritizes 'HVR_KINESIS_PARTITION_KEY' if set, otherwise falls back
    to the primary key columns defined in HVR's metadata.

    Returns:
        dict: A dictionary where keys are HVR table names and values are strings
              of comma-separated column names to be used as partition keys.
    """
    tbl_names = os.getenv('HVR_TBL_NAMES').split(":")
    base_key_names = os.getenv('HVR_TBL_KEYS_BASE').split(":")
    part_key_env_var = os.getenv('HVR_KINESIS_PARTITION_KEY')

    key_cols = {}
    for i, table in enumerate(tbl_names):
        if part_key_env_var is None:
            # If HVR_KINESIS_PARTITION_KEY is not set, use HVR's primary key metadata
            trace("Using pks from env var list. Table={} key name={}".format(
                table, base_key_names[i]))
            key_cols[table] = base_key_names[i]
        else:
            # If HVR_KINESIS_PARTITION_KEY is set, use its value for all tables
            trace("Using pks from user supplied env var list. Table={} key name={}".format(
                table, part_key_env_var))
            key_cols[table] = part_key_env_var

    # Trace the final mapping for verification
    for table in key_cols:
        trace("key_col_map : table={} key_cols[table]={}".format(
            table, key_cols[table]))

    return key_cols


def prepare_json_messages(content, hvr_table, stream=None):
    """
    Prepares a list of JSON messages from the raw content for Kinesis/S3.
    It enriches each data record with HVR metadata, CDC fields, and other
    header information.

    Args:
        content (dict): The parsed JSON content from a file, typically
                        a dictionary with table names as keys and a list of data records as values.
        hvr_table (str): The HVR table name currently being processed.
        stream (str, optional): The Kinesis stream name. Defaults to None.

    Returns:
        list: A list of dictionaries, where each dictionary is a fully
              formed JSON message ready to be sent.
    """
    messages = []
    trace("Preparing JSON records...")
    # Iterate over each data item (row) for the given HVR table
    for i, actual_data_item in enumerate(content[hvr_table]):
        # Create a shallow copy of the global json_header to avoid modifying the original
        msg_dict = json_header.copy()

        # Dictionary to store HVR-specific fields (starting with 'hvr_' or 'repl_')
        hvr_fields = {}
        data_fields = {}  # Dictionary to store actual data fields

        # Separate hvr/repl fields from actual data fields
        for key, value in actual_data_item.items():
            if key.startswith(("hvr_", "repl_")):
                hvr_fields[key] = value
            else:
                data_fields[key] = value

        # Set the main data payload in the message
        msg_dict["data"] = data_fields

        # Add HVR-specific metadata to the message
        msg_dict["hvr_fields"] = hvr_fields

        # Set CDC (Change Data Capture) fields
        msg_dict["CDC_fields"] = {
            'CDCTool': cdc_tool,
            'ToolVersion': cdc_tool_version
        }

        # Set instance type based on the environment
        msg_dict['instanceType'] = environment.upper()

        # Extract and format transaction ID components for unique reference
        tx_seq = hvr_fields.get('hvr_tx_seq_nbr', 'UNKNOWN')
        tx_countdown = f"{int(hvr_fields.get('hvr_tx_countdown_nbr', '0000000000')):010d}"
        msg_dict["eventUniqueReferenceID"] = concatenate(
            [alias, str(tx_seq), str(tx_countdown)])

        # Set flow and event timestamps if available in hvr_fields
        if 'hvr_change_time' in hvr_fields:
            msg_dict["flowTimestamp"] = hvr_fields['hvr_change_time']
        if 'repl_cap_tstamp' in hvr_fields:
            msg_dict["eventTimestamp"] = hvr_fields['repl_cap_tstamp']

        # Set database connection details in the flowTechName
        msg_dict['flowTechName']['db'] = db

        # Map HVR operation code to human-readable event type
        op_code = hvr_fields.get('hvr_change_op', '')
        msg_dict["eventType"] = op_dic.get(str(op_code), 'UNKNOWN')

        # Include 'op_type' and 'op_timestamp' if they exist in the original data item
        if 'op_type' in actual_data_item:
            msg_dict['op_type'] = actual_data_item['op_type']
        if 'op_timestamp' in actual_data_item:
            msg_dict['op_timestamp'] = actual_data_item['op_timestamp']

        # If the stream name is different from the HVR table name, update source reference table
        # This condition seems a bit redundant if stream is always derived from hvr_table.
        if stream != hvr_table:
            msg_dict["sourceReference"]["table"] = hvr_table.upper()

        messages.append(msg_dict)  # Add the prepared message to the list

    trace("Preparation")
    return messages


def send_to_kinesis_s3(kinesis_connection, s3_client, stream, content, hvr_table, hvr_base_table, key_cols) -> None:
    """
    Sends processed JSON messages to AWS Kinesis and/or S3.

    Args:
        kinesis_connection (boto3.client): The Kinesis client object.
        s3_client (boto3.client): The S3 client object.
        stream (str): The Kinesis stream name.
        content (dict): The parsed JSON content for the current table.
        hvr_table (str): The HVR table name.
        hvr_base_table (str): The HVR base table name.
        key_cols (dict): A dictionary mapping table names to their partition key columns.
    """
    # Prepare JSON messages with necessary metadata and formatting
    global successfully_sent_records
    trace("Preparing payload...")
    messages = prepare_json_messages(content, hvr_table, stream)
    trace("Payload ready")
    messages_dict = []  # List to hold messages formatted for Kinesis put_records
    s3_messages_dict = []  # List to hold messages formatted for S3

    for i, msg_dict in enumerate(messages):
        row = content[hvr_table][i]  # Get the original row data

        # Construct the partition key for Kinesis.
        # It concatenates values of specified key columns from the row.
        col_list = key_cols[hvr_table].split(",")
        part_key = ''
        for j, key_name in enumerate(col_list):
            if key_name in row:
                trace("Found key_name={} with row value={}".format(
                    key_name, row[key_name]))
                # Convert to string to ensure proper concatenation
                key_value = str(row[key_name])
            else:
                trace("Didn't find key_name={0} in row".format(key_name))
                key_value = 'Undefined'  # Use 'Undefined' if key column not found

            if j == 0:
                part_key += key_value
            else:
                part_key += '|' + key_value  # Separate composite keys with '|'

        trace("Partition key={0}".format(part_key))

        # Format message for Kinesis `put_records` API call
        messages_dict.append({
            # Data must be bytes
            'Data': json.dumps(msg_dict, sort_keys=False).encode('utf-8'),
            'PartitionKey': part_key
        })

        # Add the full message dictionary to the list for S3 (S3 payload is simpler)
        s3_messages_dict.append(msg_dict)

    # print(f"The message being sent to Kinesis is {getsizeof(messages_dict)} bytes and contains {len(messages_dict)} records.")
    # --- Send to Kinesis with retry ---

    records_to_send = messages_dict.copy()

    # Put_records in batch
    batch_successfully_sent_records = 0
    for i in range(math.ceil(len(records_to_send) / batch_size)):
        # failed_count = 2
        for attempt in range(max_retries):
            try:
                trace(
                    f"The message being sent to Kinesis is {getsizeof(records_to_send[i*batch_size:(i+1)*batch_size])} bytes long and contains {len(records_to_send[i*batch_size:(i+1)*batch_size])} records.")

                kinesis_response = kinesis_connection.put_records(
                    StreamName=stream,
                    Records=records_to_send[i*batch_size:(i+1)*batch_size]
                )
                # failed_count += -1
                # if kinesis_trace >= 1:
                #     for idx, record_response in enumerate(kinesis_response['Records']):
                #         if 'SequenceNumber' in record_response:
                #             print(f"Record {idx} SequenceNumber: {record_response.get('SequenceNumber')}")
                failed_count = kinesis_response.get('FailedRecordCount', 0)
                if failed_count == 0:
                    batch_successfully_sent_records += len(
                        records_to_send[i*batch_size:(i+1)*batch_size])
                    trace(f"Batch {i+1} sent successfully.")
                    break  # we exit the loop if failed_count==0, which means that everything was sent to kinesis

                # unsent_records = []
                # for idx, record_response in enumerate(kinesis_response['Records']):
                #     if 'ErrorCode' in record_response:
                #         unsent_records.append(records_to_send[idx])
                #         trace(f"Error message for record {idx}/{len(records_to_send)}: {record_response.get('ErrorMessage')}")
                #
                trace(f"Retrying current file.")

                # Exponential backoff with jitter
                wait_time = base_delay * (2 ** attempt)
                wait_time += random.uniform(0, 0.1 * wait_time)

                trace(f"Waiting {wait_time:.2f} seconds before next retry.")
                time.sleep(wait_time)
                #
                # successfully_sent_records += (len(records_to_send) - len(unsent_records))
                # # Only retry the failed records in the next loop
                # records_to_send = unsent_records

            except ClientError as e:
                error_code = e.response['Error']['Code']
                print(f"ClientError encountered: {error_code}")
                raise  # Unexpected AWS client error — re-raise

            except Exception as e:
                print(f"Unexpected Exception: {e}")
                raise  # re-raise the unexpected errors

        else:
            print("Failed to send records to Kinesis after retries.")

    successfully_sent_records += batch_successfully_sent_records

    # --- Send to S3 if enabled ---
    if s3_enabled:
        # Current timestamp for S3 file naming
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        # Construct S3 key (file path) using the defined format, table name, and timestamp
        s3_key = s3_file_format.replace('{table}', hvr_table).replace(
            '{timestamp}', timestamp)
        trace(f"Sending data to S3 bucket: {s3_bucket}, key: {s3_key}")

        try:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                # JSON content as string, pretty-printed
                Body=json.dumps(s3_messages_dict, indent=4),
                ContentType='application/json'  # Specify content type
            )
            trace(f"Data successfully sent to S3.")
        except Exception as e:
            print(f"ERROR sending to S3: {str(e)}")


def read_file(kinesis_connection, s3_client, file_loc, key_cols) -> None:
    """
    Reads JSON files from the specified location, parses their content,
    and sends the data to Kinesis and/or S3. Successfully processed files are then removed.

    Args:
        kinesis_connection (boto3.client): The Kinesis client object.
        s3_client (boto3.client): The S3 client object.
        file_loc (str): The directory where JSON files are located.
        key_cols (dict): A dictionary mapping table names to their partition key columns.

    Raises:
        Exception: If a file contains invalid JSON or cannot be opened.
    """
    global file_counter
    global successfully_sent_records

    tbl_map = table_file_name_map()  # Get the mapping of HVR tables to their data files

    # Iterate through each table metadata tuple (base_name, tbl_name, tstamp)
    for t in tbl_map:
        # Iterate through each file associated with the current table
        for name in tbl_map[t]:
            base_name = table_name_normalize(t[0])  # Normalize base table name
            table_name = table_name_normalize(t[1])  # Normalize HVR table name
            # Extract schema (not directly used in send_to_kinesis_s3 but available)
            hvr_schema = table_name[0]
            hvr_table = table_name[1]  # Extract HVR table name
            hvr_base_table = base_name[1]  # Extract HVR base table name

            # Construct the full path to the file
            full_name = os.path.join(file_loc, name)
            print("Reading and parsing file '" + full_name + "' ... ")
            try:
                with open(full_name, 'r') as json_file:
                    try:
                        # Load JSON content from the file
                        content = json.load(json_file)

                        # Replace placeholder in Kinesis stream name with the actual HVR table name
                        stream = kinesis_stream.replace(
                            '{hvr_tbl_name}', hvr_table)

                        # Send the processed content to Kinesis and/or S3
                        send_to_kinesis_s3(
                            kinesis_connection, s3_client, stream, content, hvr_table, hvr_base_table, key_cols)

                        trace(
                            f"{successfully_sent_records} records successfully sent out of {total_updated_rows} records")
                        print("Kinesis processing finished for: " + full_name)

                    except json.JSONDecodeError as je:
                        # Handle cases where the file content is not valid JSON
                        raise Exception(
                            f"Invalid JSON in file {full_name}: {str(je)}")

                # Remove the file after successful transmission
                remove_path(full_name)

                # Maintain record counter if the trace is activated
                file_path = f"{os.getenv('HVR_FILE_LOC')}/counter_{os.getenv('HVR_AGENT_BEGIN_TSTAMP')}.txt"
                if (successfully_sent_records == total_updated_rows):
                    if os.path.exists(file_path):
                        os.remove(file_path)
                else:
                    f = open(file_path, 'w')
                    f.write(str(successfully_sent_records))
                    f.close()
                file_counter = file_counter + 1  # Increment successful file counter

            except IOError as err:
                # Handle errors if the file cannot be opened
                raise Exception("Couldn't open file " +
                                full_name + ": " + str(err))
    # if (successfully_sent_records != total_updated_rows):
    #     raise Exception(
    #         f"{total_updated_rows-successfully_sent_records} missing records.")


def file_loc_cleanup() -> None:
    """
    Cleans up the HVR file location directory by removing all files within it.
    This is typically called at the beginning of a refresh write operation.
    """
    file_loc = os.getenv(
        'HVR_FILE_LOC')  # Get the HVR file location from environment variables
    if not file_loc or not os.path.exists(file_loc):
        print(
            f"WARNING: HVR_FILE_LOC '{file_loc}' does not exist or is not set. Skipping cleanup.")
        return

    files = os.listdir(file_loc)  # List all items in the directory
    for name in files:
        # Skip current directory ('.') and parent directory ('..') or subdirectories
        if name == "." or name == ".." or os.path.isdir(os.path.join(file_loc, name)):
            continue

        full_name = os.path.join(file_loc, name)  # Construct full path
        remove_path(full_name)  # Remove the file


def file_loc_processing(key_cols) -> None:
    """
    Orchestrates the file processing by establishing connections to AWS services
    (Kinesis and S3) and then calling `read_file` to process the data.

    Args:
        key_cols (dict): A dictionary mapping table names to their partition key columns.
    """
    file_loc = os.getenv('HVR_FILE_LOC')  # Get the HVR file location

    # Initialize Kinesis and S3 clients (will be created only if needed)
    kinesis_connection = None
    s3_client = None

    # Create Kinesis client using configured credentials and region
    kinesis_connection = boto3.client(
        'kinesis',
        region_name=aws_region,
        endpoint_url=kinesis_endpoint_url,
        aws_access_key_id=kinesis_access_key_id,
        aws_secret_access_key=kinesis_secret_access_key
    )
    trace(
        f"Kinesis client initialized for region: {aws_region}, endpoint: {kinesis_endpoint_url}")

    # Create S3 client only if S3 output is enabled
    if s3_enabled:
        s3_client = boto3.client('s3')
        trace(f"S3 client initialized.")

    # Call read_file to start processing data files
    read_file(kinesis_connection, s3_client, file_loc, key_cols)


def main(argv) -> None:
    """
    Main function of the script. It parses the invocation mode and
    orchestrates the data integration process.

    Args:
        argv (str): The invocation mode (e.g., 'refr_write_end', 'integ_end', 'refr_write_begin').
    """
    arg_load()  # Load all configuration arguments and environment variables
    if kinesis_trace > 0:
        trace("Invocation mode ={0}".format(argv))

    # Handle 'refr_write_end' or 'integ_end' modes (data transmission and staging table drops)
    if argv == "refr_write_end" or argv == "integ_end":
        if kinesis_trace > 0:
            env_var_print()  # Print HVR environment variables if tracing is enabled

        # Proceed only if there are files to process (HVR_FILE_NAMES is not empty)
        if os.getenv('HVR_FILE_NAMES') != '':
            key_cols = key_col_map()  # Determine partition key columns for tables
            # Process files and send to Kinesis/S3
            file_loc_processing(key_cols)

            # Report on successfully transmitted files
            if file_counter > 0:
                print("Successfully transmitted {0:d} file(s)".format(
                    file_counter))

    # Handle 'refr_write_begin' mode (cleanup before a refresh write)
    if argv == "refr_write_begin":
        file_loc_cleanup()  # Clean up the file location directory


# Main entry point of the script
if __name__ == "__main__":
    try:
        # Call the main function with the first command-line argument (invocation mode)
        main(sys.argv[1])
        sys.stdout.flush()  # Ensure all buffered output is written
        sys.exit(0)  # Exit with success code
    except Exception as err:
        # Catch any exceptions that occur during execution
        tb = traceback.format_exc()  # Get the full traceback
        sys.stdout.flush()  # Ensure stdout is flushed before writing to stderr
        # Write error message to stderr
        sys.stderr.write("F_JX0D01: {0}\n".format(err))
        # Write traceback to stderr
        sys.stderr.write("F_JX0D01: {0}\n".format(tb))
        sys.stderr.flush()  # Ensure stderr is flushed
        sys.exit(1)  # Exit with error code
