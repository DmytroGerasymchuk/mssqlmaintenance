use [$(MaintDBName)]
go

if exists (select 1 from sys.schemas where name='pol')
	begin
		print '"pol" schema detected. This means, "pol" module is already installed.'
		print 'Not everything from "pol" module may be installed multiple times.'
		print 'Setting NOEXEC=ON...'
		set noexec on
	end
go

create schema pol
go

create table pol.tblRegistry (
	lngPolicyNumber integer not null,
	strPolicyName varchar(255) not null,
	lngProcessingOrder integer not null,
	ysnServerLevel bit not null,
	ysnOnlineDBsOnly bit not null,
	ysnActive bit not null,
	strModuleName varchar(255) not null,
	
	constraint PK_pol_Registry primary key (lngPolicyNumber),
	constraint UX_pol_Registry_PolicyName unique (strPolicyName),
	constraint UX_pol_Registry_ModuleName unique (strModuleName)
)
go

create table pol.tblProcessingExclusion (
	lngPolicyNumber integer not null,
	strValue varchar(255) not null,
	
	constraint PK_pol_ProcessingExclusion unique (lngPolicyNumber, strValue),
	
	constraint FK_pol_ProcessingExclusion_Registry
		foreign key (lngPolicyNumber)
		references pol.tblRegistry (lngPolicyNumber)
		on update no action
		on delete cascade
)
go

create type pol.TT_DBInfo as table (
	[DatabaseName] varchar(255),
	[LastFullBackupDate] datetime,
	[LastDiffBackupDate] datetime,
	[LastLogBackupDate] datetime,
	[RecoveryModel] varchar(100),
	[AutoShrink] bit,
	[AutoClose] bit,
	[SnapshotIsolation] varchar(50),
	[BrokerEnabled] bit,
	[MirroringState] nvarchar(100)
)
go

set nocount on
insert into base.tblConfigValue (strConfigValueName, strValue)
	values
		('pol.required_maxdop', '0'),
		('pol.allowed_max_vlf', '50'),
		('pol.allow_sharepoint', '0'),

		('pol.mirr_safety_level', '1'), -- 1=asynchronous, 2=synchronous
		('pol.mirr_conn_timeout', '240'), -- timeout in seconds
		('pol.mirr_partner_name', 'TCP://$$$NOTSET$$$:PORTNUMBER')
set nocount off
go

set noexec off
print 'NOEXEC is now set to OFF...'
go

execute base.usp_prepare_object_creation 'pol', 'usp_register_policy'
go

create procedure pol.usp_register_policy
	@PolicyNumber integer,
	@PolicyName varchar(255),
	@IsServerLevel bit,
	@OnlineDBsOnly bit,
	@ModuleName varchar(255),
	@ProcessingExclusions varchar(max) = null,
	@Active bit = 1 as
begin

	begin try

		begin transaction

			if not exists (select * from pol.tblRegistry where lngPolicyNumber=@PolicyNumber)
				begin
					print formatmessage('Policy %s is not registered yet. Adding to registry. (+)', format(@PolicyNumber, '000'))
					
					set nocount on
					insert into pol.tblRegistry (lngPolicyNumber, strPolicyName, lngProcessingOrder, ysnServerLevel, ysnOnlineDBsOnly, ysnActive, strModuleName)
						values (@PolicyNumber, @PolicyName, @PolicyNumber, @IsServerLevel, @OnlineDBsOnly, @Active, @ModuleName)
					set nocount off

					if @ProcessingExclusions is not null
						begin
							set nocount on
							insert into pol.tblProcessingExclusion (lngPolicyNumber, strValue)
								select @PolicyNumber, strValue from base.udf_csv2table(@ProcessingExclusions)
							set nocount off
						end
				end
			else
				print formatmessage('Policy %s is already registered. Nothing will be done. (.)', format(@PolicyNumber, '000'))
		
		commit transaction
	
	end try
	
	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_bufprint'
go

create procedure pol.usp_bufprint
	@Message varchar(255) as
begin
	set nocount on
	insert into #Message (strPolicyName, strMessage)
		select base.udf_get_context(), @Message
	set nocount off
end
go

execute base.usp_prepare_object_creation 'pol', 'usp_for_each_db'
go

create procedure pol.usp_for_each_db
	@Pattern varchar(max),
	@PolicyNumber integer,
	@Stmt nvarchar(max) as
	
begin

	begin try
	
		declare DBNames cursor local fast_forward for
			select DBF.dbname
			from
				base.udf_expand_dbpattern_raw(@Pattern) DBF
					inner join pol.tblRegistry R on R.lngPolicyNumber=@PolicyNumber
					left join pol.tblProcessingExclusion PE on
						DBF.dbname like PE.strValue collate SQL_Latin1_General_CP1_CI_AS and
						PE.lngPolicyNumber=@PolicyNumber
			where
				PE.strValue is null and -- die Datenbank ist nicht ausgeschlossen
				((R.ysnOnlineDBsOnly=convert(bit, 0)) or (R.ysnOnlineDBsOnly=convert(bit, -1) and DBF.dbstate=0))
			order by 1
			
		declare @CurDBName varchar(255)
		
		open DBNames
		fetch next from DBNames into @CurDBName
		
		while @@fetch_status=0
			begin
				execute sp_executesql @Stmt, N'@DBName varchar(255)', @CurDBName
				fetch next from DBNames into @CurDBName
			end
			
		deallocate DBNames
	
	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_check'
go

create procedure pol.usp_check
	@SendMail bit = 1,
	@PolicyFilter varchar(1000) = null,
	@PolicyFilterInvertMatch bit = 0 as
begin

	begin try

		create table #Message (
			idMessage integer not null identity,
			strPolicyName varchar(255) not null,
			strAffectedDBName varchar(255) null,
			strMessage varchar(255) not null)
			
		declare @PolicyFilterTab table (lngPolicyNumber integer not null primary key)

		print convert(varchar, getdate(), 120) + ' Policy filter: (' + case @PolicyFilterInvertMatch when convert(bit, 0) then '+' else '-' end + ')[' + isnull(@PolicyFilter, 'ALL') + ']'

		set nocount on
		if @PolicyFilter is null
			begin
				if @PolicyFilterInvertMatch=convert(bit, 0)
					insert into @PolicyFilterTab
						select lngPolicyNumber from pol.tblRegistry
			end
		else
			begin
				if @PolicyFilterInvertMatch=convert(bit, 0)
					insert into @PolicyFilterTab
						select lngPolicyNumber from pol.tblRegistry
						intersect
						select convert(integer, strValue) from base.udf_csv2table(@PolicyFilter)
				else
					insert into @PolicyFilterTab
						select lngPolicyNumber from pol.tblRegistry
						except
						select convert(integer, strValue) from base.udf_csv2table(@PolicyFilter)
			end
		set nocount off

		declare @EffectiveFilter varchar(1000) =
			isnull(
				stuff(
					convert(
						varchar(1000),
						(select ',' + convert(varchar, lngPolicyNumber) from @PolicyFilterTab for xml path (''))
					),
				1, 1, ''),
				'NULL'
			)

		print convert(varchar, getdate(), 120) + ' Policies to be processed after filter application: [' + @EffectiveFilter + ']'

		print convert(varchar, getdate(), 120) + ' Starting policy processing...'

		declare
			@PolicyNumber integer,
			@PolicyName varchar(255),
			@IsServerLevel bit,
			@ModuleName varchar(255)

		declare ProcessingList cursor local fast_forward for
			select R.lngPolicyNumber, R.strPolicyName, R.ysnServerLevel, R.strModuleName
			from pol.tblRegistry R inner join @PolicyFilterTab PFT on R.lngPolicyNumber=PFT.lngPolicyNumber
			where R.ysnActive=convert(bit, -1)
			order by R.lngProcessingOrder
			
		open ProcessingList
		fetch next from ProcessingList into @PolicyNumber, @PolicyName, @IsServerLevel, @ModuleName

		while @@fetch_status=0
			begin
				print convert(varchar, getdate(), 120) + ' ' + format(@PolicyNumber, '000') + ' ' + @PolicyName
				execute base.usp_set_context @PolicyName
				
				declare @Stmt varchar(max)
				
				if @IsServerLevel=convert(bit, 0)
					begin
						set @Stmt = 'execute pol.' + @ModuleName + ' @DBName'
						execute pol.usp_for_each_db '*', @PolicyNumber, @Stmt
					end
				else
					begin
						set @Stmt = 'execute pol.' + @ModuleName
						execute (@Stmt)
					end

				fetch next from ProcessingList into @PolicyNumber, @PolicyName, @IsServerLevel, @ModuleName
			end

		deallocate ProcessingList

		execute base.usp_set_context ''
		
		print convert(varchar, getdate(), 120) + ' Processing finished.'
		
		if (select count(*) from #Message) > 0
			begin
				print convert(varchar, getdate()) + ' There are buffered messages.'
				print ''
				select
					idMessage as [Order],
					left(strPolicyName, 40) as [Policy Name],
					left(strAffectedDBName, 39) as [Affected DB],
					strMessage as [Message]
				from #Message
				order by 1

				if @SendMail=convert(bit, 0)
					print convert(varchar, getdate()) + ' Mail will not be sent, because it is suppressed by @SendMail option.'
				else
					begin
						declare
							@MailProfile varchar(255) = base.udf_get_mail_profile(),
							@OperatorMail varchar(255) = base.udf_get_operator_mail('pol.notify_operator'),
							@MachineName varchar(255) = convert(varchar, serverproperty('MachineName')),
							@InstanceName varchar(255) = convert(varchar, serverproperty('InstanceName')),
							@Subject varchar(255),
							@query varchar(max) = 'select idMessage as [Order], strPolicyName as [Policy Name], strAffectedDBName as [Affected DB], strMessage as [Message] from #Message order by 1',
							@html varchar(max)

						set @Subject = @MachineName + isnull('\' + @InstanceName, '') + ': Policy Check Messages'

						execute tools.usp_html_from_query @query, @html output

						execute msdb.dbo.sp_send_dbmail
							@profile_name=@MailProfile,
							@recipients=@OperatorMail,
							@subject=@Subject,
							@body_format='html',
							@body=@html
					end
			end
		else
			print convert(varchar, getdate()) + ' No messages.'
				
		drop table #Message

	end try
	
	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_autogrowth_prct'
go

create procedure pol.usp_pol_autogrowth_prct
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
		select base.udf_get_context(), @DBName, 'File: ' + [name]
		from sys.master_files MF
		where MF.database_id=db_id(@DBName) and MF.is_percent_growth=1
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_autogrowth_100mb'
go

create procedure pol.usp_pol_autogrowth_100mb
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
		select base.udf_get_context(), @DBName, 'File: ' + [name]
		from sys.master_files MF
		where MF.database_id=db_id(@DBName) and MF.growth<>0 and MF.growth<>12800 and MF.is_percent_growth=0
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_dbcollation'
go

create procedure pol.usp_pol_dbcollation
	@DBName varchar(255) as
begin

	declare @AllowSharePoint integer = base.udf_get_config_value('pol.allow_sharepoint')
	
	if @AllowSharePoint is null
		begin
			insert into #Message (strPolicyName, strAffectedDBName, strMessage)
				select base.udf_get_context(), @DBName, 'pol.allow_sharepoint is not set.'
			return
		end

	if @AllowSharePoint<>1
		set @AllowSharePoint = 0

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, 'DB: ' + isnull(D.collation_name, '?') + '; Server: ' + convert(nvarchar, serverproperty('Collation'))
	from sys.databases D
	where
		D.[name]=@DBName and
		(
			D.collation_name is null or
			(
				D.collation_name<>serverproperty('Collation') and
				(
					@AllowSharePoint = 0 or
					(@AllowSharePoint = 1 and D.collation_name<>'Latin1_General_CI_AS_KS_WS')
				)
			)
		)
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_compatlevel'
go

create procedure pol.usp_pol_compatlevel
	@DBName varchar(255) as
begin

	declare @ServerCompatLevel varchar(100)
	set @ServerCompatLevel = convert(varchar, serverproperty('ProductVersion'))
	set @ServerCompatLevel = left(@ServerCompatLevel, charindex('.', @ServerCompatLevel) - 1) + '0'

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, 'DB: ' + isnull(convert(varchar, D.[compatibility_level]), '?') + '; Server: ' + @ServerCompatLevel
	from sys.databases D
	where
		D.name=@DBName and
		(D.[compatibility_level] is null or convert(varchar, D.[compatibility_level])<>@ServerCompatLevel)
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_lastbackup'
go

create procedure pol.usp_pol_lastbackup
	@DBName varchar(255) as
begin

	set nocount on
	
	declare @TmpReport TT_DBInfo
	insert into @TmpReport execute tools.usp_db_info
	delete from @TmpReport where DatabaseName<>@DBName
	
	-- check full backups
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select
		base.udf_get_context(),
		T.DatabaseName,
		'Last Full Backup: ' + isnull(convert(varchar, T.LastFullBackupDate, 120), '?') + ', ' +
		'Last Diff. Backup: ' + isnull(convert(varchar, T.LastDiffBackupDate, 120), '?')
	from @TmpReport T
	where
		-- report policy violation if...
		-- ...LastFullBackup is not present or older than 2 days
		(T.LastFullBackupDate is null or datediff(day, T.LastFullBackupDate, getdate())>2) and
		-- ...AND there is no differential backup or differential backup is also too old
		(T.LastDiffBackupDate is null or datediff(day, T.LastDiffBackupDate, getdate())>2)

	-- check TX log backups
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select
		base.udf_get_context(),
		T.DatabaseName,
		'Last Log Backup: ' + isnull(convert(varchar, T.LastLogBackupDate, 120), '?')
	from @TmpReport T
	where (T.LastLogBackupDate is null or datediff(day, T.LastLogBackupDate, getdate())>2)
		and T.RecoveryModel<>'SIMPLE'
		and T.DatabaseName<>'model' -- log backup is not needed for "model" database

	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_autoclose'
go

create procedure pol.usp_pol_autoclose
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
		select base.udf_get_context(), @DBName, 'AutoClose=ON'
		from sys.databases D
		where D.[name]=@DBName and D.is_auto_close_on=convert(bit, -1)
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_autoshrink'
go

create procedure pol.usp_pol_autoshrink
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
		select base.udf_get_context(), @DBName, 'AutoShrink=ON'
		from sys.databases D
		where D.[name]=@DBName and D.is_auto_shrink_on=convert(bit, -1)
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_rmodel'
go

create procedure pol.usp_pol_rmodel
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
		select base.udf_get_context(), @DBName, D.recovery_model_desc
		from sys.databases D
		where D.name=@DBName and D.recovery_model=3
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_nooffline'
go

create procedure pol.usp_pol_nooffline
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
		select base.udf_get_context(), @DBName, D.state_desc
		from sys.databases D
		where D.[name]=@DBName and D.[state]<>0
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_startup'
go

create procedure pol.usp_pol_startup as
begin

	declare
		@awaited_start_code integer,
		@start_desc varchar(50)

	declare
		@real_service_name nvarchar(100),
		@auto_start integer,
		@startup_account nvarchar(100)
		
	declare @Msg varchar(255)

	if serverproperty('IsClustered')=1
		begin
			set @awaited_start_code = 3
			set @start_desc = 'manually'
		end
	else
		begin
			set @awaited_start_code = 2
			set @start_desc = 'automatically'
		end

	execute tools.usp_service_info
		'MSSQLSERVER', 'MSSQL',
		@real_service_name output, @auto_start output, @startup_account output
	
	if @auto_start<>@awaited_start_code
		begin
			set @Msg =
				'SQL Server (' + @real_service_name + ') ' +
				'must be configured to start ' + @start_desc + '.'
			execute pol.usp_bufprint @Msg
		end

	execute tools.usp_service_info
		'SQLServerAgent', 'SQLAgent',
		@real_service_name output, @auto_start output, @startup_account output
	
	if @auto_start<>@awaited_start_code
		begin
			set @Msg =
				'SQL Server Agent (' + @real_service_name + ') ' +
				'must be configured to start ' + @start_desc + '.'
			execute pol.usp_bufprint @Msg
		end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_accounts'
go

create procedure pol.usp_pol_accounts as
begin

	declare
		@real_service_name nvarchar(100),
		@auto_start integer,
		@startup_account nvarchar(100)
		
	declare @Msg varchar(255)

	execute tools.usp_service_info
		'MSSQLSERVER', 'MSSQL',
		@real_service_name output, @auto_start output, @startup_account output
	
	if charindex('\', @startup_account) = 0 and charindex('@', @startup_account) = 0
		begin
			set @Msg =
				'SQL Server (' + @real_service_name + ') ' +
				'must run under named account. Currently runs under ' + @startup_account + '.'
			execute pol.usp_bufprint @Msg
		end

	execute tools.usp_service_info
		'SQLServerAgent', 'SQLAgent',
		@real_service_name output, @auto_start output, @startup_account output
	
	if charindex('\', @startup_account) = 0 and charindex('@', @startup_account) = 0
		begin
			set @Msg =
				'SQL Server Agent (' + @real_service_name + ') ' +
				'must run under named account. Currently runs under ' + @startup_account + '.'
			execute pol.usp_bufprint @Msg
		end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_priorityboost'
go

create procedure pol.usp_pol_priorityboost as
begin

	if (select value_in_use from sys.configurations where configuration_id=1517)=1
		execute pol.usp_bufprint 'The option "priority boost" must be turned off.'

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_instancename'
go

create procedure pol.usp_pol_instancename as
begin

	if @@servername is null
		execute pol.usp_bufprint '@@servername may not be null.'
	else
		if	(@@servername not like convert(varchar, serverproperty('MachineName')) + '\%') and
			(@@servername<>convert(varchar, serverproperty('MachineName')))
			begin
				declare @Msg varchar(255)
				set @Msg =
					'Invalid instance name. Machine: ' +
					convert(varchar, serverproperty('MachineName')) +
					', but instance: ' + @@servername + '.'
				execute pol.usp_bufprint @Msg
			end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_dbccjob'
go

create procedure pol.usp_pol_dbccjob as
begin

	declare @MDDBCCJob varchar(255) = db_name() + ': Check Database (User DBs)'

	if not exists (select * from msdb.dbo.sysjobs where name=@MDDBCCJob)
		execute pol.usp_bufprint 'Job for check of user databases does not exist.'
	else
		if (select [enabled] from msdb.dbo.sysjobs where name=@MDDBCCJob) = 0
			execute pol.usp_bufprint 'Job for check of user databases is disabled.'

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_maxdop'
go

create procedure pol.usp_pol_maxdop as
begin

	declare
		@RequiredMaxDop integer = convert(integer, base.udf_get_config_value('pol.required_maxdop')),
		@RunningMaxDop integer = (select convert(integer, value_in_use) from sys.configurations where configuration_id=1539)
	
	if @RequiredMaxDop is null
		begin
			execute pol.usp_bufprint 'pol.required_maxdop is not set.'
			return
		end

	if @RequiredMaxDop<>@RunningMaxDop
		begin
			declare @Msg varchar(255) = formatmessage('Must be set to %d, but currently is %d.', @RequiredMaxDop, @RunningMaxDop)
			execute pol.usp_bufprint @Msg
		end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_sa_account'
go

create procedure pol.usp_pol_sa_account as
begin

	declare
		@IsDisabled bit,
		@IsExpirationChecked bit,
		@IsPolicyChecked bit

	select @IsDisabled=is_disabled, @IsExpirationChecked=is_expiration_checked, @IsPolicyChecked=is_policy_checked
		from sys.sql_logins where [name]='sa'

	if @IsDisabled=convert(bit, -1) execute pol.usp_bufprint 'SA account is disabled.'
	if @IsExpirationChecked=convert(bit, -1) execute pol.usp_bufprint 'Expiration for SA account is set.'
	if @IsPolicyChecked=convert(bit, -1) execute pol.usp_bufprint 'Policy check for SA account is set.'

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_vlfnumber'
go

create procedure pol.usp_pol_vlfnumber
	@DBName varchar(255) as
begin

	declare
		@MaxVLFs integer = convert(integer, base.udf_get_config_value('pol.allowed_max_vlf')),
		@CurVLFs integer

	if @MaxVLFs is null
		begin
			insert into #Message (strPolicyName, strAffectedDBName, strMessage)
				select base.udf_get_context(), @DBName, 'pol.allowed_max_vlf is not set.'
			return
		end


	execute @CurVLFs = tools.usp_get_vlf_number @DBName

	if @CurVLFs > @MaxVLFs
		begin
			set nocount on
			insert into #Message (strPolicyName, strAffectedDBName, strMessage)
				values(base.udf_get_context(), @DBName, formatmessage('Max. allowed: %d, but currently: %d.', @MaxVLFs, @CurVLFs))
			set nocount off
		end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_orphaned_exclusions'
go

create procedure pol.usp_pol_orphaned_exclusions as
begin

	set nocount on

	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select distinct
		base.udf_get_context(),
		PE.strValue,
		'The database cannot be found, but an entry exists in base.tblProcessingExclusion. Please check.'
	from
		pol.tblProcessingExclusion PE
			left join sys.databases D on D.[name] like PE.strValue collate SQL_Latin1_General_CP1_CI_AS
	where
		D.database_id is null and
		PE.strValue not in ('ReportServer', 'ReportServerTempDB') and
		(
			charindex('%', PE.strValue) = 0 and
			charindex('[', PE.strValue) = 0 and
			charindex(']', PE.strValue) = 0
		)

	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select distinct
		base.udf_get_context(),
		IBS.strDBName,
		'The database cannot be found, but an entry exists in base.tblIndividualBackupSetting. Please check.'
	from
		base.tblIndividualBackupSetting IBS
			left join sys.databases D on IBS.strDBName=D.name collate SQL_Latin1_General_CP1_CI_AS
	where
		D.database_id is null

	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_unneeded_exclusions'
go

create procedure pol.usp_pol_unneeded_exclusions as
begin

	declare @AllowSharePoint integer = base.udf_get_config_value('pol.allow_sharepoint')
	
	if @AllowSharePoint is null or @AllowSharePoint<>1
		set @AllowSharePoint = 0

	if @AllowSharePoint=1
		begin
			set nocount on
			insert into #PolCheckMessages (strPolicyName, strAffectedDBName, strMessage)
			select
				base.udf_get_context(),
				PE.strValue,
				'This exclusion for policy 3 is not needed, since the database has SharePoint collation and pol.allow_sharepoint=1.'
			from
				pol.tblProcessingExclusion PE
					left join sys.databases D on PE.strValue=D.name collate SQL_Latin1_General_CP1_CI_AS
			where
				PE.lngPolicyNumber=3 and -- "DB collation must be = server collation"
				D.collation_name='Latin1_General_CI_AS_KS_WS' and -- SharePoint collation
				PE.strValue not in ('ReportServer', 'ReportServerTempDB')
			set nocount off
		end
end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_fatal_errors'
go

create procedure pol.usp_pol_fatal_errors as
begin

	declare @BadLogRecords table (
		LogDate datetime not null,
		ProcessInfo varchar(max),
		Text varchar(max)
	)

	declare @LogOrder integer = 1

	while @LogOrder >= 0
		begin
			set nocount on

			delete from @BadLogRecords

			insert into @BadLogRecords
			execute xp_readerrorlog
				@LogOrder,
				1 -- log type (1 = sql server)

			delete from @BadLogRecords
			where
				not (
					(Text like '%DBCC database corruption%') or
					(Text like '%Stack Dump%SQLDump%')
				)
				or Text is null

			insert into #Message (strPolicyName, strAffectedDBName, strMessage)
			select
				base.udf_get_context(),
				null,
				left(convert(varchar, BLR.LogDate, 120) + ' ' + ltrim(replace(BLR.Text, '*', '')), 250)
			from
				@BadLogRecords BLR
			order by
				BLR.LogDate

			set nocount off

			set @LogOrder = @LogOrder - 1
		end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_job_step_log_names'
go

create procedure pol.usp_pol_job_step_log_names as
begin

	declare @NumChanges integer

	execute @NumChanges = jobs.usp_check_log_names @AutoFix=0, @Silent=1

	if @NumChanges>0
		begin
			declare @Msg varchar(255) = formatmessage('%d job step log name(s) do not conform to naming rules.', @NumChanges)
			execute pol.usp_bufprint @Msg
		end

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_job_category'
go

create procedure pol.usp_pol_job_category as
begin

	set nocount on
	insert into #Message (strPolicyName, strMessage)
	select base.udf_get_context(), formatmessage('Job "%s": Category "%s"', SJ.[name], SC.[name])
	from msdb.dbo.sysjobs SJ inner join msdb.dbo.syscategories SC on SJ.category_id=SC.category_id
	where SC.name like '[[]Uncategorized%'
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_job_notification'
go

create procedure pol.usp_pol_job_notification as
begin

	set nocount on
	insert into #Message (strPolicyName, strMessage)
	select base.udf_get_context(), formatmessage('Job "%s": mail notification must be "on failure" or "on completion".', SJ.[name])
	from msdb.dbo.sysjobs SJ
	where SJ.notify_level_email not in (2, 3)
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_perf_issues'
go

create procedure pol.usp_pol_perf_issues as
begin

	declare @BadLogRecords table (
		LogDate datetime not null,
		ProcessInfo varchar(max),
		Text varchar(max)
	)

	declare @LogOrder integer = 1

	while @LogOrder >= 0
		begin
			set nocount on

			delete from @BadLogRecords

			insert into @BadLogRecords
			execute xp_readerrorlog
				@LogOrder,
				1 -- log type (1 = sql server)

			delete from @BadLogRecords
			where
				not (
					(Text like 'FlushCache: % for db %:%') or
					(Text like '% I/O requests taking longer than 15 seconds %')
				)
				or Text is null

			declare
				@Pattern_FlushCache varchar(255) = '% for db %:%',
				@Pattern_LongIO varchar(255) = '% in database [[]%] (%). %'

			insert into #Message (strPolicyName, strAffectedDBName, strMessage)
			select
				base.udf_get_context(),
				coalesce(D.[name], '???') as strAffectedDBName,
				strMessage
			from
				(
					select
						left(convert(varchar, BLR.LogDate, 120) + ' ' + BLR.Text, 250) as strMessage,
						case
							when patindex(@Pattern_FlushCache, BLR.Text) > 0
								then right(BLR.Text, len(BLR.Text) - patindex(@Pattern_FlushCache, BLR.Text) - 7)
							when patindex(@Pattern_LongIO, BLR.Text) > 0
								then substring(
									BLR.Text,
									charindex('(', BLR.Text, patindex(@Pattern_LongIO, BLR.Text)) + 1,
									charindex(')', BLR.Text, patindex(@Pattern_LongIO, BLR.Text)) - charindex('(', BLR.Text, patindex(@Pattern_LongIO, BLR.Text)) - 1
								) + ':0'
							else
								'0:0'
						end as strDBTag,
						BLR.LogDate
					from
						@BadLogRecords BLR
				) Aux left join sys.databases D on
					D.database_id=convert(integer, left(Aux.strDBTag, charindex(':', Aux.strDBTag) - 1))
			order by
				Aux.LogDate

			set nocount off

			set @LogOrder = @LogOrder - 1
		end
end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_arithabort'
go

create procedure pol.usp_pol_arithabort
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, 'ARITHABORT=OFF'
	from sys.databases D
	where D.name=@DBName and D.is_arithabort_on=0
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_mirr_must_be'
go

create procedure pol.usp_pol_mirr_must_be
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, 'It seems that mirroring is not configured for this DB. Please check.'
	from sys.database_mirroring D
	where D.database_id=db_id(@DBName) and D.mirroring_guid is null
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_mirr_must_be_on'
go

create procedure pol.usp_pol_mirr_must_be_on
	@DBName varchar(255) as
begin

	set nocount on
	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, D.mirroring_state_desc
	from sys.database_mirroring D
	where D.database_id=db_id(@DBName) and D.mirroring_guid is not null and D.mirroring_state not in (2, 4) -- 2=synchronizing, 4=synchronized
	set nocount off

end
go

execute base.usp_prepare_object_creation 'pol', 'usp_pol_mirr_config'
go

create procedure pol.usp_pol_mirr_config
	@DBName varchar(255) as
begin

	declare
		@MirroringSafetyLevel integer = convert(integer, base.udf_get_config_value('pol.mirr_safety_level')),
		@MirroringConnectionTimeout integer = convert(integer, base.udf_get_config_value('pol.mirr_conn_timeout')),
		@MirroringPartnerName varchar(255) = base.udf_get_config_value('pol.mirr_partner_name')

	if @MirroringSafetyLevel is null
		insert into #Message (strPolicyName, strAffectedDBName, strMessage)
			select base.udf_get_context(), @DBName, 'pol.mirr_safety_level is not set.'

	if @MirroringConnectionTimeout is null
		insert into #Message (strPolicyName, strAffectedDBName, strMessage)
			select base.udf_get_context(), @DBName, 'pol.mirr_conn_timeout is not set.'

	if @MirroringPartnerName is null
		insert into #Message (strPolicyName, strAffectedDBName, strMessage)
			select base.udf_get_context(), @DBName, 'pol.mirr_partner_name is not set.'

	if @MirroringSafetyLevel is null or @MirroringConnectionTimeout is null or @MirroringPartnerName is null
		return

	set nocount on

	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, formatmessage('Safety level: is=%d, must be=%d.', D.mirroring_safety_level, @MirroringSafetyLevel)
	from sys.database_mirroring D
	where
		D.database_id=db_id(@DBName) and
		D.mirroring_guid is not null and
		D.mirroring_safety_level<>@MirroringSafetyLevel

	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, formatmessage('Connection timeout: is=%d, must be=%d.', D.mirroring_connection_timeout, @MirroringConnectionTimeout)
	from sys.database_mirroring D
	where
		D.database_id=db_id(@DBName) and
		D.mirroring_guid is not null and
		D.mirroring_connection_timeout<>@MirroringConnectionTimeout

	insert into #Message (strPolicyName, strAffectedDBName, strMessage)
	select base.udf_get_context(), @DBName, formatmessage('Partner name: is=%s, must be=%s.', D.mirroring_partner_name, @MirroringPartnerName)
	from sys.database_mirroring D
	where
		D.database_id=db_id(@DBName) and
		D.mirroring_guid is not null and
		D.mirroring_partner_name<>@MirroringPartnerName

	set nocount off

end
go

declare
	@EStr1 varchar(255) = 'master,msdb,' + db_name() + ',ReportServer,ReportServerTempDB',
	@Estr2 varchar(255) = 'master,tempdb,model,msdb,' + db_name(),
	@EStr3 varchar(255) = 'master,tempdb,model,msdb,' + db_name() + ',ReportServer,ReportServerTempDB'

execute pol.usp_register_policy  1, 'Autogrowth must be not in %',				0, 1, 'usp_pol_autogrowth_prct',		'master'
execute pol.usp_register_policy  2, 'Autogrowth must be 100 MB',				0, 1, 'usp_pol_autogrowth_100mb',		@EStr1
execute pol.usp_register_policy  3, 'DB collation must be = server collation',	0, 0, 'usp_pol_dbcollation',			'ReportServer,ReportServerTempDB'
execute pol.usp_register_policy  4, 'DB compat. level must be = server level',	0, 0, 'usp_pol_compatlevel'
execute pol.usp_register_policy  5, 'Last backup may not be too old',			0, 0, 'usp_pol_lastbackup',				'tempdb'
execute pol.usp_register_policy  6, 'AutoClose must be turned off',				0, 0, 'usp_pol_autoclose'
execute pol.usp_register_policy  7, 'AutoShrink must be turned off',			0, 0, 'usp_pol_autoshrink'
execute pol.usp_register_policy  8, 'Recovery model may not be SIMPLE',			0, 0, 'usp_pol_rmodel',					'master,msdb,tempdb,ReportServerTempDB'
execute pol.usp_register_policy  9, 'All databases should be ONLINE',			0, 0, 'usp_pol_nooffline'
execute pol.usp_register_policy 10, 'Services startup',							1, 0, 'usp_pol_startup'
execute pol.usp_register_policy 11, 'Services accounts',						1, 0, 'usp_pol_accounts'
execute pol.usp_register_policy 12, 'Priority boost',							1, 0, 'usp_pol_priorityboost'
execute pol.usp_register_policy 13, 'Instance name',							1, 0, 'usp_pol_instancename'
execute pol.usp_register_policy 14, 'User databases check job',					1, 0, 'usp_pol_dbccjob'
execute pol.usp_register_policy 16, 'max degree of parallelism',				1, 0, 'usp_pol_maxdop'
execute pol.usp_register_policy 17, 'SA account',								1, 0, 'usp_pol_sa_account'
execute pol.usp_register_policy 18, 'Number of Virtual Log Files',				0, 1, 'usp_pol_vlfnumber',				@EStr2
execute pol.usp_register_policy 19, 'Housekeeping: orphaned exclusions',		1, 0, 'usp_pol_orphaned_exclusions'
execute pol.usp_register_policy 20, 'Housekeeping: unneeded exclusions',		1, 0, 'usp_pol_unneeded_exclusions'
execute pol.usp_register_policy 21, 'Fatal errors',								1, 0, 'usp_pol_fatal_errors'
execute pol.usp_register_policy 22, 'Job step log names',						1, 0, 'usp_pol_job_step_log_names'
execute pol.usp_register_policy 23, 'Job category',								1, 0, 'usp_pol_job_category'
execute pol.usp_register_policy 24, 'Job notification',							1, 0, 'usp_pol_job_notification'
execute pol.usp_register_policy 25, 'Performance issues',						1, 0, 'usp_pol_perf_issues'
execute pol.usp_register_policy 26, 'Arithmetic Abort',							0, 0, 'usp_pol_arithabort',				@EStr3
-- the following polices will be regisred as inactive; please activate manually, if database mirroring is present!
execute pol.usp_register_policy 27, 'Mirroring: DB must be mirrored',			0, 0, 'usp_pol_mirr_must_be',			@Estr2, @Active=0
execute pol.usp_register_policy 28, 'Mirroring: Must be up and running',		0, 0, 'usp_pol_mirr_must_be_on',				@Active=0
execute pol.usp_register_policy 29, 'Mirroring: Configuration',					0, 0, 'usp_pol_mirr_config',					@Active=0
go


execute base.usp_update_module_info 'pol', 1, 1
go
