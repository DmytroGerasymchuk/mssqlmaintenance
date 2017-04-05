USE msdb
go

if not exists (select 1 from msdb.dbo.sysschedules where name='CollectorSchedule_Every_Day')
exec msdb.dbo.sp_add_schedule
		@schedule_name=N'CollectorSchedule_Every_Day', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20160519, 
		@active_end_date=99991231, 
		@active_start_time=220000, -- !! ggf. anpassen, derzeit um 22:00
		@active_end_time=235959
go

DECLARE @collection_set_id integer, @collection_set_uid uniqueidentifier

EXEC sp_syscollector_create_collection_set 
    @name=N'MSSQL*Maintenance Config Trace', 
    @collection_mode=1, 
    @description=N'Logging of server configuration.', 
    @logging_level=1,
    @days_until_expiration=365, -- !! ggf. anpassen, derzeit 1 Jahr
    @schedule_name=N'CollectorSchedule_Every_Day', -- !! ggf. anpassen, Schedule s. oben!
    @collection_set_id=@collection_set_id OUTPUT, 
    @collection_set_uid=@collection_set_uid OUTPUT

DECLARE @collector_type_uid uniqueidentifier =
	(
		SELECT collector_type_uid FROM [msdb].[dbo].[syscollector_collector_types] 
		WHERE [name] = N'Generic T-SQL Query Collector Type'
	)

DECLARE @collection_item_id int

EXEC sp_syscollector_create_collection_item 
    @name=N'Log msdb.dbo.sysjobs', 
    @parameters=N'
<ns:TSQLQueryCollector xmlns:ns="DataCollectorType">
<Query>
<Value>
select
	SJ.job_id,
	SJ.name,
	msdb.dbo.SQLAGENT_SUSER_SNAME(SJ.owner_sid) as [owner],
	SC.name as category,
	case SJ.[description] when ''No description available.'' then null else SJ.[description] end as [description],
	SJ.[enabled],
	SJ.start_step_id,
	SJ.delete_level, SJ.date_created, SJ.date_modified, SJ.version_number
from
	msdb.dbo.sysjobs SJ
		inner join msdb.dbo.syscategories SC on SJ.category_id=SC.category_id
		left join sys.server_principals SP on SJ.owner_sid=SP.sid
</Value>
<OutputTable>msdb_dbo_sysjobs</OutputTable>
</Query>
</ns:TSQLQueryCollector>', 
    @collection_item_id=@collection_item_id OUTPUT, 
    @collection_set_id=@collection_set_id, 
    @collector_type_uid=@collector_type_uid

EXEC sp_syscollector_create_collection_item 
    @name=N'Log msdb.dbo.sysjobsteps', 
    @parameters=N'
<ns:TSQLQueryCollector xmlns:ns="DataCollectorType">
<Query>
<Value>
select
	job_id, step_id, step_name, subsystem, convert(nvarchar(1000), command) as command,
	cmdexec_success_code, on_success_action, on_success_step_id, on_fail_action, on_fail_step_id,
	[server], database_name, database_user_name, retry_attempts, retry_interval,
	output_file_name, proxy_id
from
	msdb.dbo.sysjobsteps
</Value>
<OutputTable>msdb_dbo_sysjobsteps</OutputTable>
</Query>
</ns:TSQLQueryCollector>', 
    @collection_item_id=@collection_item_id OUTPUT, 
    @collection_set_id=@collection_set_id, 
    @collector_type_uid=@collector_type_uid
GO

declare @MyCollectionSetId integer =
	(
		select collection_set_id from msdb.dbo.syscollector_collection_sets
		where [name]='MSSQL*Maintenance Config Trace'
	)

execute sp_syscollector_start_collection_set @collection_set_id = @MyCollectionSetId
go