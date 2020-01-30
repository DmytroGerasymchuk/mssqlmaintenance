use [$(MaintDBName)]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

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
				outer apply sys.dm_exec_sql_text(C.most_recent_sql_handle) as ST
	
				inner join (
					SELECT
						SessionSpaceUsage.session_id,
				
						UserObjectsAllocPageCount		= SessionSpaceUsage.user_objects_alloc_page_count + isnull(sum(TaskSpaceUsage.user_objects_alloc_page_count), 0) ,
						UserObjectsDeallocPageCount		= SessionSpaceUsage.user_objects_dealloc_page_count + isnull(sum(TaskSpaceUsage.user_objects_dealloc_page_count), 0) ,
						InternalObjectsAllocPageCount	= SessionSpaceUsage.internal_objects_alloc_page_count + isnull(sum(TaskSpaceUsage.internal_objects_alloc_page_count), 0) ,
						InternalObjectsDeallocPageCount	= SessionSpaceUsage.internal_objects_dealloc_page_count + isnull(sum(TaskSpaceUsage.internal_objects_dealloc_page_count), 0)
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

	-- DBCC output format has changed since SQL Server 2012
	-- (+ new field RecoveryUnitId). Therefore, server version
	-- should be determined and corresponding output table used.

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

execute base.usp_prepare_object_creation 'tools', 'usp_html_from_query'
go

create procedure tools.usp_html_from_query
	@Query varchar(max),
	@HTML varchar(max) output,
	@QueryIsXmlResult bit = 0 as
begin

	-- "CSS"-Definitionen für Ausgabe der HTML-Tabelle
	declare
		@HeaderCellAdd varchar(max) = ' style="border: 1px solid black; text-align: left"',
		@NormalCellAdd varchar(max) = ' style="border: 1px solid black; font-size: x-small"'

	-- Parsing-Definitionen fürs Auslesen der XML-Inhalte aus String
	declare
		@RowStartContent varchar(max) = 'row xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"'
	declare
		@RowStartSignature varchar(max) = '<' + @RowStartContent + '>',
		@XSINilPattern varchar(max) = '% xsi:nil="true"/'

	-- ggf. Abfrage ausführen (falls das Ergebnis noch nicht als XML in String-Form bereits vorliegt)
	-- und HTML-konform encodieren und formatieren

	declare
		@QResult varchar(max)

	if @QueryIsXmlResult=convert(bit, 0)
		begin
			declare
				@QResultTab table (Value xml)

			declare
				@Cmd varchar(max)

			set @Cmd = @Query + ' for xml raw, type, elements xsinil'

			set nocount on
			insert into @QResultTab execute (@Cmd)
			set nocount off

			select @QResult = convert(varchar(max), Value) from @QResultTab
		end
	else
		set @QResult = @Query

	-- Ergebnis parsen!

	declare
		@StartPos integer,
		@Loc1 integer,
		@Loc2 integer,
		@TagText varchar(max)

	-- um Header-Zeile der Tabelle zu bauen, zuerst die Namen
	-- der auftretenden Felder extrahieren, dafür die erste Zeile
	-- analysieren

	declare
		@FirstRow varchar(max),
		@FirstSection varchar(max) = ''

	set @Loc1 = charindex(@RowStartSignature, @QResult)
	if @Loc1 <> 0
		begin
			set @Loc2 = charindex('</row>', @QResult, @Loc1 + len(@RowStartSignature))
			 -- zuerst komplette erste Zeile bis auf "</row>"-Tag am Ende auslesen
			set @FirstRow = substring(@QResult, @Loc1, @Loc2 - @Loc1)
			-- dann "<row>"-Tag am Anfang abschneiden
			set @FirstRow = substring(@FirstRow, len(@RowStartSignature) + 1, len(@FirstRow) - len(@RowStartSignature))

			set @FirstSection = '<tr>'

			set @StartPos = 1
			while @StartPos < len(@FirstRow)
				begin
					set @Loc1 = @StartPos -- "<"
					set @Loc2 = charindex('>', @FirstRow, @Loc1 + 1)
					set @TagText = substring(@FirstRow, @Loc1 + 1, @Loc2 - @Loc1 - 1)

					declare @HasNil bit

					if @TagText like @XSINilPattern
						begin
							set @TagText = left(@TagText, len(@TagText) - 16)
							set @HasNil = convert(bit, -1)
						end
					else
						set @HasNil = convert(bit, 0)

					while patindex('%[_]x00__[_]%', @TagText) > 0
						begin
							declare	@TagHexChar char(2) = substring(@TagText, patindex('%[_]x00__[_]%', @TagText) + 4, 2)
							declare @TagChar char(1) = char(convert(integer, convert(varbinary, @TagHexChar, 2)))
							set @TagText = replace(@TagText, '_x00' + @TagHexChar + '_', @TagChar)
						end

					set @FirstSection = @FirstSection + '<th' + @HeaderCellAdd + '>' + @TagText + '</th>'

					if @HasNil = convert(bit, -1)
						-- wenn leeres Element, dann sofort zum nächsten Element "einfach so" übergehen
						-- durch einfaches Inkrement der Position im String
						set @StartPos = @Loc2 + 1
					else
						-- ansonsten muss der Anfang des nächsten Elements gesucht werden
						set @StartPos = charindex('>', @FirstRow, @Loc2 + 1) + 1
				end

			set @FirstSection = @FirstSection + '</tr>'
		end

	-- die Namen der Elemente in der String-Darstellung von XML durch Tags "tr" bzw. "td" ersetzen

	set @StartPos = 1
	set @Loc1 = charindex('<', @QResult, @StartPos)

	while @Loc1 <> 0
		begin
			declare
				@EndTagShift integer,
				@NormalizedTagText varchar(max),
				@NewTagText varchar(max)

			set @Loc2 = charindex('>', @QResult, @Loc1 + 1)
			set @TagText = substring(@QResult, @Loc1 + 1, @Loc2 - @Loc1 - 1)
			set @EndTagShift = case when left(@TagText, 1) = '/' then 1 else 0 end

			set @NormalizedTagText = substring(@TagText, 1 + @EndTagShift, len(@TagText) - @EndTagShift)

			set @NewTagText =
				case @EndTagShift when 1 then '/' else '' end +
				case @NormalizedTagText
					when @RowStartContent then 'tr' -- es ist: Anfang der Zeile!
					when 'row' then 'tr'  -- es ist: Ende der Zeile!
					else 'td' + case @EndTagShift when 1 then '' else @NormalCellAdd end end +
				case when @TagText like @XSINilPattern then ' /' else '' end

			set @QResult = stuff(@QResult, @Loc1 + 1, len(@TagText), @NewTagText)

			set @StartPos = @Loc1 + len(@NewTagText) + 1 + 1
			set @Loc1 = charindex('<', @QResult, @StartPos)
		end

	-- Fertig, Ergebnis zusammen setzen und zurück liefern

	set @HTML =
		'<table style="width: 100%; border-collapse: collapse">' +
		@FirstSection +
		@QResult +
		'</table>'

end
go

execute base.usp_prepare_object_creation 'tools', 'usp_disk_space_check'
go

create procedure tools.usp_disk_space_check
	@SendMail bit = 1 as
begin

	declare
		@WarningPct numeric(10, 2) = 80,
		@ErrorPct   numeric(10, 2) = 90

	declare	@OutTab table (strOutput varchar(max))

	declare @RepTab table (
		strVolumeName varchar(max),
		strVolumeLabel varchar(max),
		numCapacityGBytes numeric(10, 4),
		numFreeSpaceGBytes numeric(10, 4),
		numFilledPct numeric(10, 2),
		numWarningPct numeric(10, 2),
		numErrorPct numeric(10, 2),
		strStateCode varchar(20)
	)

	declare
		@RetRes integer,
		@ErrNo integer,
		@XResult xml

	declare @BulkCmd nvarchar(max)

	set @BulkCmd =
		'select @XResult = T.C ' +
		'from openrowset(bulk ''' + (select strValue from base.tblConfigValue where strConfigValueName='tools.wmi_xml_dir') + '\volumes.xml'', single_nclob) T(C)'
	execute @RetRes = sp_executesql @BulkCmd, N'@XResult xml output', @XResult output
	set @ErrNo = @@error
			
	if @RetRes<>0 or @ErrNo<>0
		begin
			raiserror('Error encountered during call to sp_executesql with openrowset bulk single_nclob.', 16, 1)
			return
		end

	set nocount on
	insert into @RepTab
	select
		Aux2.*,
		case
			when numFilledPct>=numErrorPct then 'ERROR'
			when numFilledPct>=numWarningPct and numFilledPct<numErrorPct then 'WARNING'
			else 'OK'
		end as strStateCode
	from (
		select
			Aux1.*,
			convert(numeric(10, 2), (1 - (numFreeSpaceGBytes / numCapacityGBytes)) * 100) as numFilledPct,
			coalesce(BT.numWarningPct, @WarningPct) as numWarningPct,
			coalesce(BT.numErrorPct, @ErrorPct) as numErrorPct
		from (
			select
				T.C.value('./PROPERTY[@NAME="Name"][1]', 'varchar(max)') as strVolumeName,
				coalesce(nullif(T.C.value('./PROPERTY[@NAME="Label"][1]', 'varchar(max)'), ''), '???') as strVolumeLabel,
				convert(numeric(10, 4), T.C.value('./PROPERTY[@NAME="Capacity"][1]',  'decimal') / 1024 / 1024 / 1024) as numCapacityGBytes,
				convert(numeric(10, 4), T.C.value('./PROPERTY[@NAME="FreeSpace"][1]', 'decimal') / 1024 / 1024 / 1024) as numFreeSpaceGBytes
			from
				@XResult.nodes('/COMMAND/RESULTS/CIM/INSTANCE') as T(C)
		) Aux1
			left join base.tblVolumeBound BT on Aux1.strVolumeName=BT.strVolumeName
	) Aux2
	set nocount off

	select
		convert(varchar(20), left(strVolumeName, 20)) as [Disk Volume],
		convert(varchar(20), left(strVolumeLabel, 20)) as [Label],
		convert(varchar(10), numCapacityGBytes) as [Capacity GB],
		convert(varchar(10), numFreeSpaceGBytes) as [Free GB],
		convert(varchar(10), numFilledPct) as [Filled %],
		convert(varchar(10), numWarningPct) as [Warning %],
		convert(varchar(10), numErrorPct) as [Error %],
		convert(varchar(10), left(strStateCode, 10)) as [State Code]
	from @RepTab RT
	order by strVolumeName

	if exists (select 1 from @RepTab where strStateCode<>'OK')
		begin
			print 'There are problems.'
			if @SendMail=convert(bit, 0)
				print 'Mail will not be sent, because @SendMail was set to 0.'
			else
				begin
					declare
						@MailProfile varchar(255) = base.udf_get_mail_profile(),
						@OperatorMail varchar(255) = base.udf_get_operator_mail('tools.notify_operator'),
						@PreparedQueryResult varchar(max) = convert(varchar(max), (
							select
								strVolumeName as [Disk Volume], strVolumeLabel as [Label],
								numCapacityGBytes as [Capacity GB], numFreeSpaceGBytes as [Free GB], numFilledPct as [Filled %],
								numWarningPct as [Warning %], numErrorPct as [Error %],
								strStateCode as [State Code]
							from @RepTab
							where strStateCode<>'OK'
							order by strVolumeName
							for xml raw, type, elements xsinil)),
						@Html varchar(max)

					execute tools.usp_html_from_query @PreparedQueryResult, @Html output, 1

					execute msdb.dbo.sp_send_dbmail
						@profile_name=@MailProfile,
						@recipients=@OperatorMail,
						@subject='Disk Space Check Alert',
						@body_format='html',
						@body=@Html
				end
		end
	else
		print 'All OK.'

end
go

execute base.usp_prepare_object_creation 'tools', 'udf_real_win_service_name'
go

create function tools.udf_real_win_service_name(@FullNameIfDefaultInstance nvarchar(100), @PrefixIfNamedInstance nvarchar(100))
returns nvarchar(100) as
begin

	declare @real_service_name nvarchar(100)

    if serverproperty('InstanceName') is not null
		set @real_service_name = @PrefixIfNamedInstance + N'$' + convert(sysname, serverproperty('InstanceName'))
    else
		set @real_service_name = @FullNameIfDefaultInstance

	return @real_service_name

end
go

execute base.usp_prepare_object_creation 'tools', 'usp_service_info'
go

create procedure tools.usp_service_info
	@FullNameIfDefaultInstance nvarchar(100),
	@PrefixIfNamedInstance nvarchar(100),
	-- AUSGABE
	@real_service_name nvarchar(100) output,
	@auto_start integer output,
	@startup_account nvarchar(100) output as
begin

	declare
		@key nvarchar(200),
		@RetRes integer

	set @real_service_name = Tools.udf_real_win_service_name(@FullNameIfDefaultInstance, @PrefixIfNamedInstance)

	set @key = N'SYSTEM\CurrentControlSet\Services\' + @real_service_name

    execute @RetRes = master.dbo.xp_regread
		N'HKEY_LOCAL_MACHINE',
        @key,
        N'Start',
        @auto_start OUTPUT, N'no_output'
	if @RetRes<>0
		set @auto_start = 0

    execute @RetRes = master.dbo.xp_regread
		N'HKEY_LOCAL_MACHINE',
        @key,
        N'ObjectName',
        @startup_account OUTPUT, N'no_output'
	if @RetRes<>0
		set @startup_account = '???'

end
go

execute base.usp_prepare_object_creation 'tools', 'usp_mirroring'
go

create procedure tools.usp_mirroring
	@Operation varchar(255) = 'show' -- show, suspend, resume, failover, force_service_allow_data_loss
as
begin

	begin try

		set @Operation = lower(@Operation)

		if @Operation not in ('show', 'suspend', 'resume', 'failover', 'force_service_allow_data_loss')
			raiserror('Unknown operation.', 16, 1)

		set nocount on

		print 'Current mirroring state:'
		print ''
		
		select
			convert(varchar(30), db_name(database_id)) as [db_name],
			mirroring_state,
			convert(varchar(20), mirroring_state_desc) as [mirroring_state_desc],
			mirroring_role,
			convert(varchar(20), mirroring_role_desc) as [mirroring_role_desc]
		from sys.database_mirroring
		where mirroring_guid is not null
		order by 1

		if @Operation = 'show' -- bei "show" ist hier Schluss
			return

		declare @FromState table (mirroring_state tinyint primary key)
		declare @MirroringRole table (mirroring_role tinyint primary key)

		if @Operation = 'suspend'
			begin
				insert into @FromState values (1), (4), (5), (6) -- Disconnected from other partner; Synchronized; The partners are not synchronized; The partners are synchronized
				insert into @MirroringRole values (1), (2) -- Principal, Mirror
			end

		if @Operation = 'resume'
			begin
				insert into @FromState values (0) -- Suspended
				insert into @MirroringRole values (1) -- Principal
			end

		if @Operation = 'failover'
			begin
				insert into @FromState values (4), (6) -- Synchronized; The partners are synchronized
				insert into @MirroringRole values (1) -- Principal
			end

		if @Operation = 'force_service_allow_data_loss'
			begin
				insert into @FromState values (0), (1), (4), (5), (6) -- Suspended; Disconnected from other partner; Synchronized; The partners are not synchronized; The partners are synchronized
				insert into @MirroringRole values (2) -- Mirror
			end

		print ''
		print 'Requested operation is: ' + @Operation
		print ''
		print 'Databases will be affected with following settings:'
		print ''

		select mirroring_state from @FromState
		select mirroring_role from @MirroringRole

		declare AffectedDBs cursor local fast_forward for
			select
				db_name(database_id) as [db_name]
			from
				sys.database_mirroring dm
					inner join @FromState fs on dm.mirroring_state=fs.mirroring_state
					inner join @MirroringRole mr on dm.mirroring_role=mr.mirroring_role
			where dm.mirroring_guid is not null
			order by 1

		declare @CurDB varchar(255)

		open AffectedDBs
		fetch next from AffectedDBs into @CurDB

		while @@fetch_status=0
			begin
				declare @Cmd varchar(max) = 'use [master] alter database [' + @CurDB + '] set partner ' + @Operation
				print @Cmd
				execute (@Cmd)
				fetch next from AffectedDBs into @CurDB
			end

		deallocate AffectedDBs

		print ''
		print 'New mirroring state:'
		print ''
		
		select
			convert(varchar(30), db_name(database_id)) as [db_name],
			mirroring_state,
			convert(varchar(20), mirroring_state_desc) as [mirroring_state_desc],
			mirroring_role,
			convert(varchar(20), mirroring_role_desc) as [mirroring_role_desc]
		from sys.database_mirroring
		where mirroring_guid is not null
		order by 1

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_prepare_object_creation 'tools', 'usp_perm_info'
go

create procedure tools.usp_perm_info
	@DBName varchar(255),
	@PrincipalName varchar(255),
	@SchemaNamePattern varchar(255) = '%',
	@ObjectNamePattern varchar(255) = '%',
	@ObjectTypePattern varchar(255) = '%',
	@PermissionNamePattern varchar(255) = '%',
	@PermissionStatePattern varchar(255) = '%',
	@SuppressConnectPermission bit = 0,
	@SuppressToClauseInPermissionSql bit = 0
as
begin

	if @DBName like '%--%' or @DBName like '%/*%'
		begin
			raiserror('Invalid characters in @DBName parameter.', 16, 1)
			return
		end

	declare @Cmd nvarchar(max) = '
	use [' + @DBName + ']

	select
		coalesce(schema_name(so.schema_id), dbp.class_desc) as [schema],
		coalesce(so.[name], case dbp.class when 0 then db_name() else schema_name(dbp.major_id) end) as [name],
		coalesce(so.type_desc, dbp.class_desc) as [type],
		dbp.permission_name,
		dbp.state_desc as permission_state,
		case when dbp.state_desc like ''GRANT%'' then ''GRANT'' else dbp.state_desc end + '' '' +
			dbp.permission_name + case dbp.class when 0 then '''' when 1 then '' on ['' + schema_name(so.schema_id) + ''].['' + so.[name] + '']'' else '' on '' + dbp.class_desc + ''::['' + schema_name(dbp.major_id) + '']'' end +
			' + case when @SuppressToClauseInPermissionSql=convert(bit, 0) then ''' to ['' + dp.name + '']'' +' else '' end + '
			case dbp.state when ''W'' then '' WITH GRANT OPTION'' else '''' end
			collate SQL_Latin1_General_CP1_CI_AS as permission_sql
	from
		sys.database_principals dp
			inner join sys.database_permissions dbp on dp.principal_id=dbp.grantee_principal_id
			left join sys.objects so on dbp.major_id=so.object_id and dbp.class=1
	where
		dp.[name]=@Principal
	'

	declare @Result table
		(
			[schema] sysname,
			[name] sysname,
			[type] nvarchar(60),
			[permission_name] nvarchar(128),
			[permission_state] nvarchar(60),
			[permission_sql] varchar(255)
		)

	insert into @Result
		execute sp_executesql @Cmd, N'@Principal varchar(255)', @Principal=@PrincipalName

	select *
	from @Result r
	where
		r.[schema] like @SchemaNamePattern and
		r.[name] like @ObjectNamePattern and
		r.[type] like @ObjectTypePattern and
		r.[permission_name] like @PermissionNamePattern and
		r.[permission_state] like @PermissionStatePattern and
		(@SuppressConnectPermission=convert(bit, 0) or (@SuppressConnectPermission=convert(bit, -1) and not (r.[type]='DATABASE' and r.[permission_name]='CONNECT')))

end
go

execute base.usp_update_module_info 'tools', 1, 7
go
