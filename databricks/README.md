# hvrdatabricksagent.py - HVR Databricks connector
## Description
This python script can be used to merge CDC changes and/or refresh a Databricks Delta table.  The script uses
ODBC to connect to Databricks and apply the changes.  Databricks hosted on Azure and AWS are supported.   

To support this connector the target location is Azure Blob storage, Azure ADLS Gen2, or an S3 bucket.  A FileFormat
action is configured for Csv with /HeaderLine (default), or Avro, Parquet.  The Databricks cluster that is the 
target for this connector must be configured with access to the Azure storage or S3 bucket.

Integrate/Refresh writes table changes to the integrate location (Blob/ADLS store or S3 bucket) as files in the
format defined by the FileFormat action.  This connector connects to the cluster via ODBC and using SQL
statements loads and/or merges the data from the integrate location into the target table.

The following options are available with this connector:
- CDC replication
- Soft Delete
- Timekey
- Create/recreate target table during refresh
  - Create the target as a managed (default) or unmanaged table

## Requirements
- Integrate running on a Windows or Linux machine
- The Simba Spark ODBC driver from Databricks downloaded and installed on the Integrate machine
- If Linux:
  - unixODBC downloaded and installed
- Python pyodbc package downloaded and installed
- A DSN configured to connect to Databricks (see Databricks documentation)
- For Databricks hosted by Azure, using Blob storage:
  - Python azure-storage-blob package downloaded and installed
  - An Azure Blob Storage account & container - Integrate will land files here
  - The Databricks cluster configured to be able to access the Azure Blob Storage
- For Databricks hosted by Azure, using ADLS Gen2:
  - Python azure-storage-file-datalake package downloaded and installed
  - An Azure ADLS Gen2 account & container - Integrate will land files here
  - The Databricks cluster configured to be able to access the ADLS Gen2 storage 
  - An Access Key (regardless of authentication method) so the connector can manage the files
- For Databricks hosted by AWS:
  - Python boto3 package downloaded and installed
  - An S3 bucket - Integrate will land files here
  - The Databricks cluster configured to be able to access the S3 bucket
- The Integrate action defined with /ReorderRows=SORT_COALESCE
- A FileFormat action with one of the following (the default is CSV)
  - /Csv /HeaderLine
  - /Avro
  - /Parquet

## Environment variables
The following environment variables are required by the the agent or optionally influence the behavior of
the connector. They should be supplied by channel Environment actions.

| Environment variable       | Mandatory | Description |
| --------------------       | --------- | ----------- |
| HVR_DBRK_DSN               |    Yes    | The DSN configured on the Integrate machine to connect to Databricks   |
| HVR_DBRK_FILESTORE_ID      |   Maybe   | The AWS Access Key for connecting to the target location |
| HVR_DBRK_FILESTORE_KEY     |   Maybe   | The Azure Access Key or the AWS Secret Key for connecting to the target location |
| HVR_DBRK_FILESTORE_REGION  |     No    | The region where the S3 bucket resides - for connecting to the target location |
| HVR_DBRK_ADAPT_DDL_ADD_COL |     No    | If set, script detects new columns and adds them during integrate if <br>HVR_DBRK_HVRCONNECT is configured |
| HVR_DBRK_BURST_EXTERNAL_LOC|     No    | The location for the unmanaged burst table |
| HVR_DBRK_CONNECT_STRING    |     No    | Replaces HVR_DBRK_DSN if desired |
| HVR_DBRK_CONNECT_TIMEOUT   |     No    | If set, the connection to the cluster will have the specified timeout (seconds) |
| HVR_DBRK_DATABASE          |     No    | Specifies the target database, if not the default |
| HVR_DBRK_DELIMITER         |     No    | The FieldSeparator in the FileFormat /Csv action if set |
| HVR_DBRK_EXTERNAL_LOC      |     No    | The location for the unmanaged target table if create-on-refresh is configured |
| HVR_DBRK_FILEFORMAT        |     No    | The file format configured in the FileFormat action if not CSV |
| HVR_DBRK_FILESTORE_OPS     |     No    | Control which file operations are executed |
| HVR_DBRK_FILE_EXPR         |     No    | The value of Integrate /RenameExpression if set |
| HVR_DBRK_HVRCONNECT        |     No    | Provides the credentials for the script to connect to the repository |
| HVR_DBRK_LINE_SEPARATOR    |     No    | The LineSeparator in the FileFormat /Csv action if set |
| HVR_DBRK_LOAD_BURST_DELAY  |     No    | Delay, in seconds, after creating the burst table and before loading |
| HVR_DBRK_MULTIDELETE       |     No    | Handle the multi-delete change that is a result of SAPXform |
| HVR_DBRK_PARALLEL          |     No    | If set, and if running on a POSIX system, process tables in parallel |
| HVR_DBRK_PARTITION_table   |     No    | If set, target table is created with partitions columns |
| HVR_DBRK_REFRESH_RESTRICT  |     No    | If set, and refresh, delete rows matching this condition instead of truncating |
| HVR_DBRK_SLICE_REFRESH_ID  |     No    | Should be set by hvrslicedrefresh.py.  If set connector runs sliced refresh logic |
| HVR_DBRK_TBLPROPERTIES     |     No    | If set, the connector will set these table properties during refresh |
| HVR_DBRK_TIMEKEY           |     No    | Set to 'ON' if the target table is Timekey  |
| HVR_DBRK_UNMANAGED_BURST   |     No    | If 'ON', create burst table as external |

## UserArgument options
The following options may be set in the /UserArguments of the AgentPlugin action for hvrdatabricksagent.py

| Option | Description |
| ------ | ----------- |
|   -c   | The context used to refresh.  Set this option if '-r' is set and a Context is used in the refresh |
|   -d   | Name of the SoftDelete column.  Default is 'is_deleted'.  Set this option if the SoftDelete column is configured <br>with a name other than 'is_deleted'. |
|   -D   | Name of the SoftDelete column.  Set this option if the SoftDelete column is in the target table. |
|   -o   | Name of the {hvr_op} column.  Default is ‘op_type’.  Set this option if the name of the Extra column populated by <br>{hvr_op} is different than ‘op_type’. |
|   -m   | If set, map NUMBER with prec <= 10 and scale = 0 to INTEGER |
|   -n   | If set, the connector will apply inserts using INSERT SQL instead of MERGE |
|   -O   | Name of the {hvr_op} column.   Set this option if the target table includes the Extra column. |
|   -p   | Set this option if it is desired that the target is not truncated before the refresh. |
|   -r   | Set this option to instruct the script to create/recreate the target table during Refresh |
|   -t   | Set this option if the target is TimeKey.   Same as using the Environment action HVR_DBRK_TIMEKEY=ON |
|   -w   | Specify files in ADLS G2 to databricks using 'wasbs://' instead of 'abfss://' |
|   -y   | If set the script will NOT delete the files on S3 or Azure store after they have been applied to the target |

## Authentication
The connector needs authentication to connect to the file store and delete the files processed during the cycle.  Set Environment actions to the following values to provide this authorization to the conector.

#### AWS using access key
    HVR_DBRK_FILESTORE_ID - Set to the AWS access key ID
    HVR_DBRK_FILESTORE_KEY - Set to the AWS secret access key
    HVR_DBRK_FILESTORE_REGION - Set tot he region where the S3 bucket is located

#### AWS using an IAM role
If HVR_DBRK_FILESTORE_ID is not set, the connector will call the boto3 API without passing in credentials.  The boto3 library will then fall back on a set of providers as outlined in [Boto3 Credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).  If the boto3 library does not find credentials using any other providers, and if this is an EC2 instance, boto3 will try to load credentials from the instance metadata service, allowing it to use an IAM role. See the "IAM roles" section for details.

#### Azure Blob store
    HVR_DBRK_FILESTORE_KEY - Set to the Azure access key

#### Azure ADLS gen2
The ADLSg2 file store can be authorized either through an access key, or OAuth credentials.  If HVR_DBRK_FILESTORE_KEY is set, the connector will use it to connect to the file store.

    HVR_DBRK_FILESTORE_KEY - Set to the Azure access key

If HVR_DBRK_FILESTORE_KEY is not set, the connector will authenticate with DefaultAzureCredential as described in [How to authorize Python apps on Azure](https://docs.microsoft.com/en-us/azure/developer/python/azure-sdk-authenticate). The DefaultAzureCredential automatically uses the app's managed identity (MSI) in the cloud, and automatically loads a local service principal from environment variables when running locally. The following environment variables should be set in the HVR user's environment when not running in the cloud:

    AZURE_TENANT_ID - Set to the directory (tenant) ID
    AZURE_CLIENT_ID - Set to the application (client) ID
    AZURE_CLIENT_SECRET - Set to the client secret

## File Format
By default the script works with CSV files that have a header line.  Since there is no schema component to a CSV file, and 
since all values are written as strings, the datatypes are defined by the data type of the table being loaded and as long 
as the values in the CSV file correspond, there are no conversion issues.

However, it is not that uncommon to find the default field separator, the comma, in the data and this causes parsing issues.
If, because of the characters in the data, the FileFormat action is modified to set the LineSeparator and/or FieldSeparator 
character, there are Environment actions to communicate these settings to the connector:

      HVR_DBRK_DELIMITER
      HVR_DBRK_LINE_SEPARATOR

The connector also supports PARQUET and AVRO files.   If the FileFormat action is set to one of these, use 
HVR_DBRK_FILEFORMAT to communicate that setting to the connector.   

If the FileFormat is AVRO or PARQUET, note that PARQUET and AVRO files have schema definitions in them.   These can a data 
type inconsistency error to be thrown.   For instance, HVR sets the precision of an Oracle NUMBER column to 1000 if the DDL 
for the column does not specify a precision.   When DataBricks loads data from one of these files (with precision of at least 
one column’s datatype set to 1000), Databricks will throw an error.   To prevent the error a ColumnProperties action should 
be added to define a precision lower than or equal to 38.

If using HVR 6, and the FileFormat is PARQUET, set BlockCompress=GZIP on the FileFormat action

## Files written by HVR and processed by the connector
The connector associates the files produced by integrate or refresh with the different tables by parsing the file
name.  If the Integrate RenameExpression is set, the value of RenameExpression needs to be passed to the connector in
HVR_DBRK_FILE_EXPR.

When issuing the SQL that moves the data from those files to a Delta table, the pathname is generated differently depending on the target.  For AWS the path starts with 's3://'.   For Azure Blob store the path starts with 'wasbs://'.   For ADLS Gen2 the path starts with 'abfss://'.

The connector processes one table at a time.   As soon as the data for a table has been merged, the connector will delete the files for that table.  If the connector is interrupted (this could happen when Integrate is suspended), then when HVR or Integrate start back up, the connector will be called again with the same environment variable settings. For this reason the connector always checks that the files exist before processing a table.   If they do not exist the connector will skip that table as it has already been processed.

The connector also checks to see if the files written by Integrate in the last cycle are the only files in the folder.  If they are, then the connector can create the burst table as an unmanaged table passing the folder as the location.  See 'Burst table' below.

The various file operations performed by the connector can be disabled using HVR_DBRK_FILESTORE_OPS.  Its valid values are:

    'check'
    'delete'
    '+cleanup'
    'none'

If set to 'none' the connector will not check that the files exist, nor will it delete the files after the table has been processed.

If set to '+cleanup' the connector will delete any files it finds in the folder during the 'check' phase that are not part of this integrate cycle.  Note that to enable '+cleanup', HVR_DBRK_FILE_EXPR must be set to the value of Integrate /RenameExpression and the pathname specified by HVR_DBRK_FILE_EXPR must include {hvr_tbl_name} as an element in a folder, not just the file name.

## Burst table
If HVR_DBRK_BURST_EXTERNAL_LOC is set, the script creates the burst table as an unmanaged table.  Otherwise it creates the burst table as a managed table.  The burst table is loaded using the COPY INTO sql.  Burst tables are not dropped when the script is done.  Before using an existing burst table, the script checks to see if it needs to be re-created and, if so, drops the existing burst table.  The sql to create the burst table, and to truncate the burst table, is the same sql:  CREATE OR REPLACE TABLE ...

## Target is a replication copy
To maintain a replication copy, the connector requires that the following ColumnProperties actions are defined. Note 
that the connector will not apply these columns to the Databricks target table.

    ColumnProperties /Name=op_type /Extra /IntegrateExpression={hvr_op} /Datatype=integer /Context=!refresh
    ColumnProperties /Name=is_deleted /Extra /SoftDelete /Datatype=integer /Context=!refresh

A simple example of the configuration for a replicate copy follows:

    AgentPlugin /Command=hvrdatabricksagent.py
    ColumnProperties /Name=op_type /Extra /IntegrateExpression={hvr_op} /Datatype=integer /Context=!refresh
    ColumnProperties /Name=is_deleted /Extra /SoftDelete /Datatype=integer /Context=!refresh
    Environment /Name=HVR_DBRK_FILESTORE_KEY /Value=“ldkfjljfdgljdfgljdflgj”
    Environment /Name=HVR_DBRK_DSN /Value=azuredbrk
    FileFormat /Csv /HeaderLine
    Integrate /ReorderRows=SORT_COALESCE

## Target is TimeKey
If the connector is configured for a TimeKey target (HVR_DBRK_TIMEKEY=ON or AgentPlugin /UserArguments="-t"),
then the connector will load all the changes in the files directly to the target table.  Any /Extra column defined 
for the target exist in the target table.

## Target is SoftDelete
If the target is SoftDelete, as opposed to a replicate copy, the connector must be configured to preserve the
SoftDelete column.  The SoftDelete column can have any name.  To indicate to the connector the name of the 
SoftDelete column, and that it should be preserved, use the "-D" option in the AgentPlugin /UserArguments.
A sample configuration for SoftDelete follows:

    AgentPlugin /Command=hvrdatabricksagent.py /UserArgument="-D is_deleted"
    ColumnProperties /Name=op_type /Extra /IntegrateExpression={hvr_op} /Datatype=integer /Context=!refresh
    ColumnProperties /Name=is_deleted /Extra /SoftDelete /Datatype=integer
    Environment /Name=HVR_DBRK_FILESTORE_KEY /Value=“jkhkgkjhkjh="
    Environment /Name=HVR_DBRK_DSN /Value=azuredbrk
    FileFormat /Csv /HeaderLine
    Integrate /ReorderRows=SORT_COALESCE

## Refresh Create/Recreate target table
The connector can be configured to create/recreate the target table when refresh is run.  The requirements are:
- Integrate runs on the hub
- The connector is running under Python 3
- The '-r' option is set in the AgentPlugin /UserArgument
- The repository connection string is provided to the Agent Plugin via the HVR_DBRK_HVRCONNECT Environment action.

If the connector is running in an HVR 6 install, the connection information consists of the URL, hubname, username and password, all separated by spaces.   For instance:

    https://192.168.22.444:4340 hvrhub user password

If the connector is running in an HVR 5 install, the easy way to get the connection information is by using the GUI.  If you run HVR Initialize, the GUI will display the command as it should be entered if run at the command line.  The format of the hvrinit command is "hvrinit <options> connect_string channel_name".  For instance, to run hvrinit against my Oracle repository to initilaize channel 'msq2orexp':

    hvrinit -oj -h oracle 'hvrhub/!{Qf6QmNqX}!' msq2orexp

If the channel that I am configuring with the hvrdatabricksagent.py plugin is named "msq2databrk", the connect string is:

    -h oracle 'hvrhub/!{Qf6QmNqX}!' msq2databrk

Once you have the respository connection string, convert it to a Base64 value using a Base64 encoder/decode on the internet.

The connector retrieves the table description and all the ColumnProperites actions so that it can generate the same column description as HVR would.  If there are Contexts on the ColumnProperties actions, and if the Context is used for a refresh, the '-c <context>' option should be added to the UserArguments so that the connector knows which ColumnProperties actions to apply.

The script can create the target table as a managed, or an unmanaged table.   By default the table is created managed.
To create an unmanaged table, specify the location of the table using the HVR_DBRK_EXTERNAL_LOC environment action.  
Note that the pathname specified by HVR_DBRK_EXTERNAL_LOC may contain {hvr_tbl_name} and, if it does, the script will 
perform the substitution.  For example:

       /Name=HVR_DBRK_EXTERNAL_LOC /Value="/mnt/delta/{hvr_tbl_name}"

If there are ColumnProperties actions tied to a Context, and that Context is used with the refresh, the "-c" Context should be passed to the connector using the "-c" option.

The table can be configured so that partitioning is defined upon create with HVR_DBRK_PARTITION_table.   Set "table" to the HVR table name and set the Value of the Environment action to a comma separated list of columns.  For example:

       /Name=HVR_DBRK_PARTITION_kc4col /Value=c2,c1

## DDL changes
The script can be configured to automatically add columns.  If an AdaptDDL action is created for the source group, HVR will automatically add the column to internal caches and will start adding that column's values to the CDC data transmitted to the target.  If the Environment action HVR_DBRK_ADAPT_DDL_ADD_COL=on is set on the target group, the script will check to see if there are extra columns in the incoming data that do not exist in the target table and, if there are, add them.  Note that to do this the script must query the repository to get the data types and so HVR_DBRK_HVRCONNECT must also be set.

## Sliced Refresh
If the environment variable HVR_DBRK_SLICE_REFRESH_ID is set on a refresh, the connector will use locking and control functionality to ensure that the target table is truncated or created only at the beginning, and that only one slice job at a time accesses the target table.  This logic is in conjunction with the hvrslicedrefresh.py script.

## Table Properties
By default the connector will set the following properties on the table when it is refreshed:

    delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true

If the Environment variable HVR_DBRK_TBLPROPERTIES is set, the connector will replace the default table properties
with the value in the Environment variable.  If HVR_DBRK_TBLPROPERTIES is set to '' or "", the connector will
not set table properties during refresh.

## Change Log
| Version | Date     | Description |
| ------- | -------- | ----------- |
| 1.0     | 06/18/21 | First version - includes all changes up to this time as defined in the change log in hvrdatabricksagent.py |
| 1.1     | 06/30/21 | Fix table_file_name_map for non-default /RenameExpression |
| 1.2     | 07/01/21 | Escape quote all column names to support column name like class# |
| 1.3     | 07/02/21 | Issue plutiple COPY INTO commands if # files > 1000 |
| 1.4     | 07/09/21 | Fixed a bug in create table processing ColumnProperties DatatypeMatch where it would only apply to <br>first column that matched |
| 1.5     | 07/09/21 | Fixed create table column ordering - respect source column order |
| 1.6     | 07/09/21 | Provide an Environment variable for customizing table properties |
| 1.7     | 07/14/21 | Added support for /DatatypeMatch="number[prec=0 && scale=0]" only |
| 1.8     | 07/20/21 | Added support for sliced refresh when generated by hvrslicedrefresh.py -s' |
| 1.9     | 07/21/21 | Add UserArguments option to pass in the refresh Context - used to filter the ColumnProperties actions |
| 1.10    | 07/22/21 | Use 'wasbs://' instead of 'abfss://' when passing file path into databricks |
| 1.11    | 07/23/21 | Fixed throwing "F_JX0D03: list assignment index out of range" checking Python version |
| 1.12    | 07/23/21 | Use OAuth authentication to list and access files in ADLS gen 2 - use access key if set |
| 1.13    | 07/27/21 | Fixed throwing "F_JX0D03: list assignment index out of range" processing target columns |
| 1.14    | 07/27/21 | Fixed throwing 'F_JX0D03: delete_file() takes 2 positional arguments but # were given' |
| 1.15    | 07/28/21 | Process ColumnProperties and TableProperties where chn_name='*' as well as chn_name=<channel> |
| 1.16    | 07/30/21 | Fixed resilience of merge command - only insert ot update if hvr_op != 0 |
| 1.17    | 07/30/21 | Added -E & -i options for refreshing two targets with the same job |
| 1.18    | 08/04/21 | Fixed (re)create of target table appending rows |
| 1.19    | 08/06/21 | Fixed regression from v1.18 where create table failed on a managed target table |
|         |          | Added finer controls over what file operations are executed |
|         |          | Reduce the number of files returned by azstore_service.get_file_system_client.get_paths |
| 1.20    | 08/24/21 | Added a '+cleanup' option for HBVR_DBRK_FILESTORE_OPS to cleanup files |
| 1.21    | 08/25/21 | Only cleanup during integrate, not refresh |
| 1.22    | 08/27/21 | Create burst table explicilty instead of as select from target |
| 1.23    | 09/01/21 | Added an option (-n) to apply inserts using INSERT sql instead of MERGE |
| 1.24    | 09/02/21 | Added support for partitioning |
| 1.25    | 09/02/21 | Added support for parallel processing |
| 1.26    | 09/03/21 | Refactored the MERGE SQL, INSERT SQL |
| 1.27    | 09/09/21 | Added support for wildcards in partitioning spec |
| 1.28    | 09/10/21 | Use target column ordering for select clause of INSERT SQL |
| 1.29    | 09/21/21 | Re-introduced logic that removes non-burst columns if refresh |
| 1.30    | 09/22/21 | Fixed a couple of bugs building table map |
| 1.31    | 09/30/21 | Fixed order of columns in target table when created |
| 1.32    | 09/30/21 | Added way to set a delay between loading the burst and merge |
| 1.33    | 10/12/21 | Fixed table mathcing with wildcards |
| 1.34    | 10/18/21 | Added ability to define the restrict condition for a refresh |
| 1.35    | 10/18/21 | Changed connector to REPLACE the target table if refresh without CREATE instead of TRUNCATE |
| 1.36    | 10/20/21 | Add an option to downshift basename when used in HVR_DBRK_EXTERNAL_LOC |
| 1.37    | 11/12/21 | Remove HVR_DBRK_PARALLEL |
| 1.38    | 11/16/21 | Drop target table if necessary before creating it |
| 1.39    | 11/17/21 | Restore support for HVR_DBRK_PARALLEL - Linux only |
| 1.40    | 12/17/21 | Added support for refresh/create of an empty table |
| 1.41    | 01/06/22 | Only create burst table if it does not match target table |
| 1.42    | 01/19/22 | Fixed table wildcard matching with '!' operator |
| 1.43    | 01/21/22 | Fixed 'table not found' check; default unmanaged_burst to OFF |
| 1.44    | 01/21/22 | Strip leading & trailing spaces from HVRCONNECT after decode |
| 1.45    | 01/21/22 | Fixed processing of DESCRIBE with column named 'name' |
| 1.46    | 01/24/22 | Fixed error: 'Options' object has no attribute 'use_unmanaged_burst_table' |
| 1.47    | 01/25/22 | Fixed MERGE failure if a key column is updated |
| 1.48    | 02/02/22 | Fixed merge of key update, delete step with >1 key columns |
| 1.49    | 02/02/22 | Support Softdelete |
| 1.50    | 02/03/22 | Add environment variable for setting target table name |
| 1.51    | 02/04/22 | Use REST Apis for HVR6 repository gets |
| 1.52    | 02/11/22 | Fix SoftDelete - use 2 merge statements |
| 1.53    | 02/15/22 | Fixed logic that validates MANAGED state of table |
| 1.54    | 02/16/22 | If '-r' not set on refresh, use TRUNCATE not CREATE OR REPLACE |
| 1.55    | 02/25/22 | Fixed a bug: sliced refresh, "-r" not set; target table truncated each slice |
| 1.56    | 03/16/22 | Do not use derived partition columns |
| 1.57    | 03/16/22 | Cast the burst columns in the COPY INTO SQL |
| 1.58    | 03/16/22 | Do not fail if a table is removed from the channel |
| 1.59    | 03/22/22 | Disable unmanaged burst |
| 1.60    | 03/22/22 | Fixed parsing of HVR_FILE_LOC when auth uses InstanceProfile |
| 1.61    | 04/05/22 | Add partial support for DDL (ADD column only), make SSL verification optional |
| 1.62    | 04/08/22 | Implemented partial DDL support for HVR 5 |
| 1.63    | 04/13/22 | Log a message after: 1) the target table is created, 2) columns are added |
| 1.64    | 04/20/22 | Re-implemented unmanaged burst with an external loc & burst is loaded |
| 1.65    | 04/21/22 | Fixed implementation of ADD DDL when new column isnt in input file |
| 1.66    | 04/26/22 | Fixed implementation of ADD DDL to work with timekey & truncate refresh |
| 1.67    | 05/02/22 | Fixed multi-delete SQL |
| 1.68    | 05/05/22 | Use derived columns in burst table,merge |
| 1.69    | 05/20/22 | Burst table: removed drop if needs to be recreated<br>Removed describe called by target table creation<br>Throw error if incoming data has column not in target & adapt not configured |
| 1.70    | 06/17/22 | Fixed precision and scale handling in create table, HVR6 |
| 1.71    | 06/21/22 | Optionally map NUMBER with prec <= 10 and scale=0 to INTEGER |
| 1.72    | 06/23/22 | Re-implemented the unmanaged burst option |
