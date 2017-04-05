USE msdb
GO

DECLARE @collection_set_id integer, @collection_set_uid uniqueidentifier

EXEC sp_syscollector_create_collection_set 
    @name=N'MSSQL*Maintenance Activity Trace', 
    @collection_mode=0, 
    @description=N'Logging of server activities.', 
    @logging_level=1, 
    @days_until_expiration=14, -- !! ggf. anpassen, derzeit 14 Tage
    @schedule_name=N'CollectorSchedule_Every_15min', -- !! ggf. anpassen, derzeit UPLOAD alle 15 Minuten
    @collection_set_id=@collection_set_id OUTPUT, 
    @collection_set_uid=@collection_set_uid OUTPUT

DECLARE @collector_type_uid uniqueidentifier =
	(
		SELECT collector_type_uid FROM [msdb].[dbo].[syscollector_collector_types] 
		WHERE [name] = N'Generic T-SQL Query Collector Type'
	)

DECLARE @collection_item_id int

EXEC sp_syscollector_create_collection_item 
    @name=N'Active User Requests', 
    @parameters=N'
<ns:TSQLQueryCollector xmlns:ns="DataCollectorType">
<Query>
<Value>
select
	getdate() as snap_time_stamp,
	R.session_id,
	R.request_id,
	S.login_name,
	coalesce(
		''Job "'' + SJ.name + ''" : Step '' + convert(varchar, SJS.step_id) + '' "'' + SJS.step_name + ''"'',
		S.program_name) as program_name,
	db_name(R.database_id) as dbname,
	R.start_time,
	datediff(second, R.start_time, getdate()) as duration_sec,
	R.command,
	R.status,
	R.last_wait_type,
	R.wait_time,
	convert(varchar(1000), ST.text) as sql_text
from
	sys.dm_exec_requests R
		inner join sys.dm_exec_sessions S on R.session_id=S.session_id
		cross apply sys.dm_exec_sql_text(R.sql_handle) as ST
		-- SQL Agent Jobs Information
		left join (
				select distinct
					spid,
					convert(varbinary, substring(program_name, 30, 34), 1) as job_id_vb,
					convert(integer, substring(program_name, 72, len(program_name) - 72)) as step_id
				from master.dbo.sysprocesses
				where program_name like ''SQLAgent - TSQL JobStep%''
			) JobSP on S.session_id=JobSP.spid
		left join msdb.dbo.sysjobs SJ on
			JobSP.job_id_vb=convert(varbinary, SJ.job_id)
		left join msdb.dbo.sysjobsteps SJS on
			SJ.job_id=SJS.job_id and
			JobSP.step_id=SJS.step_id
where
	R.command is not null and
	isnull(R.wait_type, '''')&lt;&gt;''BROKER_RECEIVE_WAITFOR'' and
	S.is_user_process=convert(bit, -1) and
	S.session_id&lt;&gt;@@spid
</Value>
<OutputTable>active_user_requests</OutputTable>
</Query>
</ns:TSQLQueryCollector>', 
    @collection_item_id=@collection_item_id OUTPUT, 
    @frequency=300, -- !! ggf. anpassen, derzeit COLLECT alle 300 Sekunden, also alle 5 Minuten
    @collection_set_id=@collection_set_id, 
    @collector_type_uid=@collector_type_uid
GO

declare @MyCollectionSetId integer =
	(
		select collection_set_id from msdb.dbo.syscollector_collection_sets
		where [name]='MSSQL*Maintenance Activity Trace'
	)

execute sp_syscollector_start_collection_set @collection_set_id = @MyCollectionSetId
go
