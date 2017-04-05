execute sp_configure 'show advanced options', 1
reconfigure with override

execute sp_configure 'blocked process threshold (s)', 15
reconfigure with override

execute sp_configure 'blocked process threshold (s)'
go

CREATE EVENT SESSION [MSSQL*Maintenance] ON SERVER 
ADD EVENT sqlos.wait_info
	(
		ACTION (sqlserver.client_app_name, sqlserver.client_hostname, sqlserver.[database_name], sqlserver.sql_text, sqlserver.username)
		WHERE
			([duration]>15000) AND
			([sqlserver].[session_id]>50) AND
			([wait_type]<>'BROKER_EVENTHANDLER') AND
			([wait_type]<>'BROKER_RECEIVE_WAITFOR') AND
			([wait_type]<>'DBMIRRORING_CMD') AND
			([wait_type]<>'BROKER_TRANSMITTER') AND
			([wait_type]<>'CXPACKET') AND
			([wait_type]<>'DBMIRROR_EVENTS_QUEUE')
	),
ADD EVENT sqlserver.blocked_process_report,
ADD EVENT sqlserver.xml_deadlock_report 
ADD TARGET package0.event_file(SET filename=N'mssql_maintenance.xel',max_file_size=(5))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=ON)
GO

ALTER EVENT SESSION [MSSQL*Maintenance] ON SERVER
STATE=START
GO
