use [$(MaintDBName)]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if exists (select * from sys.schemas where name='tools')
	print 'tools schema already exists.'
else
	begin
		print 'tools schema does not exist - creating...'
		execute ('create schema tools')
	end
go

execute base.usp_prepare_object_creation 'tools', 'usp_db_info'
go

create procedure tools.usp_db_info
	@DBName varchar(255) = null as
begin

	select
		D.name as [DatabaseName],
		(select max(backup_finish_date) from msdb..backupset where type = 'D' and database_name = D.name) as [LastFullBackupDate],
		(select max(backup_finish_date) from msdb..backupset where type = 'I' and database_name = D.name) as [LastDiffBackupDate],
		(select max(backup_finish_date) from msdb..backupset where type = 'L' and database_name = D.name) as [LastLogBackupDate],
		D.recovery_model_desc as [RecoveryModel],
		D.is_auto_shrink_on as [AutoShrink],
		D.is_auto_close_on as [AutoClose],
		D.snapshot_isolation_state_desc as [SnapshotIsolation],
		D.is_broker_enabled as [BrokerEnabled],
		DM.mirroring_state_desc as [MirroringState]
	from
		sys.databases D
			inner join sys.database_mirroring DM on D.database_id=DM.database_id
	where
		D.name=coalesce(@DBName, D.name)
	order by 1

	if @DBName is not null
		begin
			declare @Cmd nvarchar(max)

			set @Cmd = '
use [' + @DBName + ']

select
	SS.name as [schema],
	ST.name as [table],
	ST.create_date,
	ST.modify_date,
	ST.lock_escalation_desc as [lock_escalation],
	P.partition_number,
	C.name as part_column_name,
	case
		when PF.name is null then null
		else case PF.boundary_value_on_right when 1 then ''<'' else ''<='' end
	end as part_comparison,
    RV.value as part_value,
	coalesce(FG1.name, FG2.name) as [filegroup],
	P.rows
from
	sys.tables ST
		inner join sys.schemas SS on ST.schema_id=SS.schema_id
		inner join sys.indexes SI on ST.object_id=SI.object_id and SI.index_id<2 -- only heaps or clustered indexes
		inner join sys.data_spaces DS on SI.data_space_id=DS.data_space_id

		inner join sys.partitions P on ST.object_id=P.object_id and P.index_id<2 -- only heaps or clustered indexes

		left join sys.index_columns IC on
			SI.object_id=IC.object_id and
			SI.index_id=IC.index_id and
			IC.partition_ordinal=1
		left join sys.columns C on
			IC.object_id=C.object_id and
			IC.column_id=C.column_id

		left join sys.partition_schemes PS on DS.data_space_id=PS.data_space_id
		left join sys.partition_functions PF on PS.function_id=PF.function_id
		left join sys.partition_range_values RV on
			PF.function_id=RV.function_id and P.partition_number=RV.boundary_id

		left join sys.filegroups FG1 on DS.data_space_id=FG1.data_space_id

		left join sys.destination_data_spaces DDS on
			DDS.partition_scheme_id=PS.data_space_id and DDS.destination_id=P.partition_number
		left join sys.filegroups FG2 on DDS.data_space_id=FG2.data_space_id
order by 1, 2, P.partition_number'

			execute (@Cmd)
		end

end
go

execute base.usp_prepare_object_creation 'tools', 'usp_who3'
go

create procedure tools.usp_who3
	@ActiveOnly bit = 0,
	@ProgramNamePattern varchar(255) = '%',
	@DBNamePattern varchar(255) = '%',
	@LoginNamePattern varchar(255) = '%',
	@HostNamePattern varchar(255) = '%' as
begin

	;with UserSessionInfo as (
		select top 100 percent
			S.session_id,
			S.host_name,
			S.program_name,
			S.login_name,
			isnull(R.status, S.status) as session_status,
			isnull(R.blocking_session_id, 0) as blocking_session_id,
			S.cpu_time,
			S.memory_usage,
			S.reads,
			S.writes,
			S.logical_reads,
			S.login_time,
			S.last_request_start_time,
			S.last_request_end_time,
			S.row_count,
			R.command,
			db_name(R.database_id) as dbname,
			R.wait_type,
			R.wait_resource,
			R.open_transaction_count as opentran,
			R.percent_complete as prct_complete,
			R.estimated_completion_time,
			R.cpu_time as request_cpu_time,
			R.reads as request_reads,
			R.writes as request_writes,
			R.logical_reads as request_logical_reads,
			coalesce(ST2.text, ST.text) as last_stmt_text,
			substring(
				ST2.text, 
				R.statement_start_offset/2 + 1,
				(
					case
						when R.statement_end_offset = -1 then len(convert(nvarchar(max), ST2.text)) * 2
						else R.statement_end_offset end
					- R.statement_start_offset
				) / 2
			) as current_stmt_text,
			SSU.UserObjectsAllocPageCount,
			SSU.UserObjectsDeallocPageCount,
			SSU.InternalObjectsAllocPageCount,
			SSU.InternalObjectsDeallocPageCount,
			RGRP.[name] as res_pool,
			RGWG.[name] as res_group,
			R.scheduler_id as scheduler,
			OsS.parent_node_id as numa_node
		from
			sys.dm_exec_sessions S
				inner join sys.dm_resource_governor_workload_groups RGWG on S.group_id=RGWG.group_id
				inner join sys.dm_resource_governor_resource_pools RGRP on RGWG.pool_id=RGRP.pool_id

				inner join sys.dm_exec_connections C on S.session_id=C.session_id
				cross apply sys.dm_exec_sql_text(C.most_recent_sql_handle) as ST
	
				inner join (
					SELECT
						SessionSpaceUsage.session_id,
				
						UserObjectsAllocPageCount		= SessionSpaceUsage.user_objects_alloc_page_count + isnull( SUM (TaskSpaceUsage.user_objects_alloc_page_count), 0) ,
						UserObjectsDeallocPageCount		= SessionSpaceUsage.user_objects_dealloc_page_count + isnull( SUM (TaskSpaceUsage.user_objects_dealloc_page_count), 0) ,
						InternalObjectsAllocPageCount	= SessionSpaceUsage.internal_objects_alloc_page_count + isnull( SUM (TaskSpaceUsage.internal_objects_alloc_page_count), 0) ,
						InternalObjectsDeallocPageCount	= SessionSpaceUsage.internal_objects_dealloc_page_count + isnull( SUM (TaskSpaceUsage.internal_objects_dealloc_page_count), 0)
					FROM
						sys.dm_db_session_space_usage AS SessionSpaceUsage
							left join sys.dm_db_task_space_usage AS TaskSpaceUsage
								ON SessionSpaceUsage.session_id = TaskSpaceUsage.session_id
					WHERE
						SessionSpaceUsage.database_id = 2
					GROUP BY
						SessionSpaceUsage.session_id,
						SessionSpaceUsage.user_objects_alloc_page_count,
						SessionSpaceUsage.user_objects_dealloc_page_count,
						SessionSpaceUsage.internal_objects_alloc_page_count,
						SessionSpaceUsage.internal_objects_dealloc_page_count
				) SSU on S.session_id=SSU.session_id		

				left join sys.dm_exec_requests R on S.session_id=R.session_id
				outer apply sys.dm_exec_sql_text(R.sql_handle) as ST2

				left join sys.dm_os_schedulers OsS on R.scheduler_id=OsS.scheduler_id
		where
			S.is_user_process=1 and
			S.session_id<>@@spid -- exclude itself
	)
	select
		USI.session_status,

		SP.dbname as active_db,

		USI.login_name,
		USI.host_name,
		coalesce(
			'Job "' + SJ.name + '" : Step ' + convert(varchar, SJS.step_id) + ' "' + SJS.step_name + '"',
			USI.program_name) as program_name,

		USI.session_id,
		USI.blocking_session_id as blk_by,
		isnull(TrInfo.TranCount, 0) as opentran,
		USI.wait_type,
		USI.wait_resource,

		PTC.pthreads,

		USI.command,
		USI.prct_complete,

		case when USI.estimated_completion_time < 36000000 then '0' else '' end +
			convert(varchar, USI.estimated_completion_time/1000/3600)+ ':' +
			right('0' + convert(varchar, (estimated_completion_time/1000)%3600/60), 2) + ':' +
			right('0' + convert(varchar, (estimated_completion_time/1000)%60), 2) as remg_time,

		USI.cpu_time,
		USI.memory_usage,
		USI.reads,
		USI.writes,
		USI.logical_reads,
		USI.login_time,
		USI.last_request_start_time,
		USI.last_request_end_time,
		USI.row_count,
		USI.dbname,
		USI.request_cpu_time,
		USI.request_reads,
		USI.request_writes,
		USI.request_logical_reads,
		USI.current_stmt_text,
		USI.last_stmt_text,
		USI.UserObjectsAllocPageCount,
		USI.UserObjectsDeallocPageCount,
		USI.InternalObjectsAllocPageCount,
		USI.InternalObjectsDeallocPageCount,

		USI.res_pool,
		USI.res_group,
		USI.scheduler,
		USI.numa_node
	from
		UserSessionInfo USI
			inner join (select distinct spid, db_name(dbid) as dbname from master.dbo.sysprocesses) SP on USI.session_id=SP.spid
			left join (select spid, count(*) as pthreads from master.dbo.sysprocesses group by spid) PTC on USI.session_id=PTC.spid
			-- Transaction Information
			left join (
					select session_id, count(*) as TranCount
					from sys.dm_tran_session_transactions 
					group by session_id
				) TrInfo on USI.session_id=TrInfo.session_id
			-- SQL Agent Jobs Information
			left join (
					select distinct
						spid,
						convert(varbinary, substring(program_name, 30, 34), 1) as job_id_vb,
						convert(integer, substring(program_name, 72, len(program_name) - 72)) as step_id
					from master.dbo.sysprocesses
					where program_name like 'SQLAgent - TSQL JobStep%'
				) JobSP on SP.spid=JobSP.spid
			left join msdb.dbo.sysjobs SJ on
				JobSP.job_id_vb=convert(varbinary, SJ.job_id)
			left join msdb.dbo.sysjobsteps SJS on
				SJ.job_id=SJS.job_id and
				JobSP.step_id=SJS.step_id
	where
		(@ActiveOnly=convert(bit, 0) or (@ActiveOnly=convert(bit, -1) and USI.command is not null)) and
		program_name like @ProgramNamePattern and
		SP.dbname like @DBNamePattern and
		USI.login_name like @LoginNamePattern and
		USI.host_name like @HostNamePattern

end
go

execute base.usp_prepare_object_creation 'tools', 'udf_get_operator_mail'
go

create function tools.udf_get_operator_mail(@BaseConfigValueName varchar(255))
returns varchar(255) as
begin

	return
		(
			select email_address from msdb.dbo.sysoperators where name=
				(select strValue from base.tblConfigValue where strConfigValueName=@BaseConfigValueName)
		)

end
go

execute base.usp_prepare_object_creation 'tools', 'usp_get_vlf_number'
go

create procedure tools.usp_get_vlf_number
	@DBName varchar(255) as
begin

	declare @LogInfo table (
		FileId sql_variant,
		FileSize sql_variant,
		StartOffset sql_variant,
		FSeqNo sql_variant,
		Status sql_variant,
		Parity sql_variant,
		CreateLSN sql_variant)

	declare @LogInfo2012 table (
		RecoveryUnitId sql_variant,
		FileId sql_variant,
		FileSize sql_variant,
		StartOffset sql_variant,
		FSeqNo sql_variant,
		Status sql_variant,
		Parity sql_variant,
		CreateLSN sql_variant)

	-- determine SQL Server version; if 2012 or higher, use @LogInfo2012 table!
	declare
		@SrvVerS varchar(255),
		@SrvVerI integer

	set @SrvVerS = convert(varchar, serverproperty('ProductVersion'))
	set @SrvVerI = left(@SrvVerS, charindex('.', @SrvVerS) - 1)

	set nocount on
	if @SrvVerI < 11
		begin
			insert into @LogInfo
			execute ('dbcc loginfo(''' + @DBName + ''')')
			return @@rowcount
		end
	else
		begin
			insert into @LogInfo2012
			execute ('dbcc loginfo(''' + @DBName + ''')')
			return @@rowcount
		end
	set nocount off

end
go

execute base.usp_update_module_info 'tools', 1, 0
go
