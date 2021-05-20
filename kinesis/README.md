# hvrkinesisagent.py - HVR AWS Kinesis Connector
## Description
This script can be used to send Integrated change data messages or rows from a Refresh via the AWS Kinesis message broker.
The script reads the supplied list of CSV files and converts the content to JSON format
before sending to Kinesis. Each line in the CSV file represents one Kinesis message.
## Connector modes (automatically supplied by HVR connector framework)
| mode           | Description                                |
| -------------- | ------------------------------------------ |
| refr_write_end | Send data files via Kinesis message broker |
| integ_end      | Send data files via Kinesis message broker |

All other modes are ignored.

## Environment variables
The following environment variables are required by the the agent, or optionally influence the behavior of
the connector and should be supplied by channel Environment actions. Variables designated as mandatory must
be supplied by the user, those designated as Automatic will be supplied by the HVR connector framework, and
those designated as not being mandatory are optional and generally have a default value.

| Environment variable       | Mandatory | Description |
| --------------------       | --------- | ----------- |
| HVR_AWS_REGION             |    Yes    | This variable sets the AWS region name where the Kinesis stream exists   |
| HVR_KINESIS_STREAM         |    Yes    | This variable sets a Kinesis stream for a single stream, or accepts the <br>substitution {hvr_tbl_name} to have a stream per integrated table        |
| HVR_KINESIS_PARTITION_KEY  |     No    | This variable tells the plugin which column's value(s) to use as the <br>Kinesis stream partition key. It can be column names, separated by commas<br>(case sensitive), in which case the same column name is assumed to exists <br> in all the tables being replicated, or if this environment variable is <br> omitted, it will use the primary key column value(s), concatenated <br>together if it's a composite key, separated by '&#124;'.|
| HVR_KINESIS_TRACE          |     No    | Non-zero enables tracing |
| HVR_FILE_LOC               | Automatic | This variable sets a location of files to be sent |
| HVR_TBL_NAMES              | Automatic | This variable contains list of tables as defined in HVR's metadata |
| HVR_BASE_NAMES             | Automatic | This variable contains list of tables as defined as target names |
| HVR_FILE_NAMES             | Automatic | This  variable contains list of files written into file system |


## Additional Metadata
Additional metadata can optionally be added to the JSON message by including these as extra columns through ColumnProperties :
- op_type (usually set to {hvr_op})
- op_timestamp (usually set to {hvr_cap_timestamp})

## AWS Credentials
In order to allow this agent to access Kinesis, boto3 must be configured as per the documentation :
[the AWS documentation](http://docs.pythonboto.org/en/latest/boto_config_tut.html)
