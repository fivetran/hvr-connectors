# hvrspanneragent.go - HVR Spanner connector
## Description
This Go script can be used to perform updates and deletes against a Spanner table, and/or refresh a Spanner table.  The script uses the Spanner client to write maps for inserts or updates based on csv files that are written to a directory by an HVR file connector.

Integrate/Refresh writes table changes to the location specified in the HVR file connector location. This can be either on the hub server or on a separate server. These changes need to be written as csv files with headers, as defined by the FileFormat action.  This connector connects to Spanner using the Spanner client and loads and/or inserts/updates the data from the integrate location into the target table.

The following options are available with this connector:
- Soft Delete
- Timekey

The following options are not available with this connector:
- Hard Delete (standard replication)

## Requirements
- Integrate running on a Windows or Linux machine
- Golang installed on the Integrate machine
- Google Cloud Credentials file for Service Account with Cloud Spanner Database User role
- The Integrate action defined with options RenameExpression={hvr_integ_tstamp}-{hvr_tbl_name}.csv, MaxFileSize=3000000
- The FileFormat action defined with options Csv, HeaderLine (optionally QuoteCharacter="""")
- One of the following sets of actions:
    - For Soft Delete Replication
        - The AgentPlugin action defined with options Command=$HVR_CONFIG/plugin/agent/spanner/hvrspanneragent.exe UserArgument=-s
        - The ColumnProperties action defined with options Name=hvr_is_deleted, Extra, SoftDelete, Datatype=integer
    - For Timekey Replication
        - The AgentPlugin action defined with options Command=$HVR_CONFIG/plugin/agent/spanner/hvrspanneragent.exe 
        - The ColumnProperties action defined with options Name=hvr_integ_key, Extra, IntegrateExpression={hvr_integ_seq}, TimeKey, Datatype=varchar, Length=36
        - The ColumnProperties Name=hvr_integ_op, Extra, IntegrateExpression={hvr_op}, Datatype=integer 

## Environment variables
The following environment variables are required by the the agent or optionally influence the behavior of
the connector. They should be supplied by channel Environment actions.

| Environment variable        | Mandatory | Description |
| --------------------        | --------- | ----------- |
| GOOGLE_APPLICATION_CREDENTIALS | Yes    | The directory path for the key file for the Google service account. The account needs to have Cloud Spanner Database User role. |
| HVR_SPANNER_PROJECTID       |    Yes    | The Google cloud project id   |
| HVR_SPANNER_INSTANCEID      |    Yes    | The Google cloud instance id |
| HVR_SPANNER_DATABASE        |    Yes    | The Google Spanner database name |
| HVR_SPANNER_REFRESH_THREADS |     No    | The number of Go threads to be used for processing files during refresh, defaults to 10 if not specified |
| HVR_SPANNER_TRACE           |     No    | Enables tracing of the hvrspanneragent. Tracing Levels: 0 - No tracing, 1 - Minimal logging, 2 - File-level logging, 3 - Row-level logging |


## UserArgument options
The following options may be set in the /UserArguments of the AgentPlugin action for hvrdatabricksagent.py

| Option | Description |
| ------ | ----------- |
|   -s   | Use this option for soft deletion. All transaction are integrated as an InsertOrUpdate mutation, which means if a row doesn't exist in the target table then an insert is done, if it does then the existing row is updated. Without this options all transactions are integrated as an insert. |

## Authentication
HVR needs authentication to write rows to the target Spanner table.  Set an Environment action with Name=GOOGLE_APPLICATION_CREDENTIALS, Value="path to credentials file"

## Table Creation
The script does not create tables in Spanner. The tables need to be created in Spanner prior to running any Refresh job. The tables need to include any "Extra" columns in the HVR channel, such as metadata columns like timestamps or delete indicators.

## File Format
By default the script works with CSV files that have a header line.  Since there is no schema component to a CSV file, and 
since all values are written as strings, the datatypes are defined by the data type of the table being loaded and as long 
as the values in the CSV file correspond, there are no conversion issues.

It is not that uncommon to find the default field separator, the comma, inside data fields themselves. This causes parsing issues, and it may be necessary to set the QuoteCharacter property of the FileFormat action to an appropriate value. Similarly, LineSeparator, EscapeCharacter, and FieldSeparator may need to be set to suit your particular use case. 

## Files written by HVR and processed by the connector
The connector associates the files produced by integrate or refresh with the different tables by parsing the file name. The RenameExpression property for the Integrate action needs to be set to {hvr_integ_tstamp}-{hvr_tbl_name}.csv.

The connector processes one table at a time. Especially during refresh operations, many files may be generated for a single table. Within a refresh job or an integrate cycle, the script will iterate through all tables in the channel and then through all files for that table. As soon as the data for a file has been integrated and the Spanner client has been closed, the connector will delete the file that has been processed.

## Refresh Create/Recreate target table
The script does not support creating or recreating tables on the Spanner target. All rows are truncated during refresh jobs, including any that are marked as deleted or that include timekey history.

## DDL changes
The script does not support capturing DDL changes from the source.

## Sliced Refresh
The script does not support the use of slicing for refresh.

## Change Log
| Version | Date       | Description |
| ------- | ---------- | ----------- |
| 1.0     | 2024-03-07 | First version - includes all changes up to this time as defined in the change log in hvrspanneragent.go |
| 1.1     | 2024-07-15 | Added code to ensure that numeric strings aren't converted to integers if the target column is a string |


