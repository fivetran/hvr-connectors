# Writing a connector

If the AgentPlugin action is configured for a target group or location, HVR calls the program defined in the action four times:

- Before a refresh starts
- After a refresh completes
- Before an integrate cycle starts
- After an integrate cycle ends

The target platform should be providing either an API or a SQL interface.   In all the AgentPlugins we’ve written so far, where the target is not a messaging service, there is some sort of data load API or SQL, and then some sort of MERGE-type SQL.   The data load interface will be used to implement Refresh and the MERGE to implement CDC.   If there is not a MERGE SQL, the connector may have to explicitly issue INSERT/UPDATE and DELETE statements.

Once the API has been identified, determine whether there is a Python package for that API.  If the API is SQL then the Python package is pyodbc.  

Depending on the target platform, and customer requirements, the connector may be expected to support a replication copy of the source table in the target, a replication copy with SoftDelete, and/or a Timekey target.

Support for a replication copy on the target will typically require an Extra column with IntegrateExpression set to {hvr_op} as well as an Extra column with /SoftDelete set.  These columns can be discarded by the connector when pushing the data into the target.

    /ReorderRows=SORT_COALESCE

The target location will typically be a directory on the integrate machine or a cloud file store.   A FileFormat action must be defined.   The template scripts provided expect CSV files.

The file format depends upon what file formats are support by the target platorm API, and the customer’s requirements.

The connector typically removes the files written by Refresh or Integrate after it has merged the data into the target.  If the target location is a cloud file store then this step requires a Python package for that cloud store.

The first step might be to create a channel with the ‘hvrskeletonagent.py’ and the following actions and initialize the channel, run Refresh and move data through Integrate.

skeleton	SRC	*	*	Capture 	
skeleton	TRG	*	*	AgentPlugin /Command=hvrskeletonagent.py	
skeleton	TRG	*	*	FileFormat /Csv	
skeleton	TRG	*	*	Integrate 	

