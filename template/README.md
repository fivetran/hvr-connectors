# Writing a connector

A connector is a program or script that provides an interface between HVR and a target that is not currently supported natively by HVR. A connector is configured into HVR by 1) making the target location a disk or cloud store location, 2) defining a FileFormat target action to specify the format of the files written in that location, and 3) defining an AgentPlugin action that tells HVR to run the connector.

When the AgentPlugin action is configured for a target group or location, HVR calls the program defined in the action four times:

- Before a refresh starts
- After a refresh completes
- Before an integrate cycle starts
- After an integrate cycle ends

When HVR calls the conenctor at the refresh end or integrate end it passes the following information using environment variables:

- A list of the tables affected during this refresh or integrate cycle
- For each table listed, a list of the columns
- For each table listed, a list of the key columns
- A list of the files written by this refresh or integrate cycle
- The file location

The job of the connector is to either refresh the target using the data in the files listed, or to update the target using the data in the files listed.

To write a connector, the developer needs to determine:

- What language will the connector be written in?
- What API(s) are available and is there an interface to that API for the chosen language:
  - If the target supports database-type objects, what is the API to load/instantiate the table?  What file formats does this API support?
  - If the target supports database-type objects, is there a SQL interface for merging the data into the target?
  - If the target is a messageing system, what is the API?

If there is a MERGE SQL interface for merging the CDC data into the target table, then The Integrate action must have the /ReorderRows=SORT_COALESCE option set so that the merge logic will not fail.

As a first step, create a channel with the 'hvrskeletonagent.py' connector.  This will give you a feel for how the connector fits into the flow of data in HVR, and expose you to the information passed to the connector by HVR (make sure to include more than one table in your tests).  Here is a sample list of the actions for this channel that should be defined for the target:

     TRG	*	*	AgentPlugin /Command=hvrskeletonagent.py	
     TRG	*	*	FileFormat /Csv	
     TRG	*	*	Integrate 	

Examples of connectors where the target supports database-type objects are:

     actianavalanche
     bigquery
     databricks
     netezza

Examples of existing connectors where the target is a streaming platform are:

     eventhub
     kenesis

