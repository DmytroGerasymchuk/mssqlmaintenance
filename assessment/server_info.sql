
create procedure #tmp_startup_account
	@ServiceDescription nvarchar(100),
	@FullNameIfDefaultInstance nvarchar(100),
	@PrefixIfNamedInstance nvarchar(100) as
begin

	declare
		@real_service_name nvarchar(100),
		@key nvarchar(200),
		@RetRes integer,
		@auto_start integer,
		@startup_account nvarchar(100)

    if serverproperty('InstanceName') is not null
		set @real_service_name = @PrefixIfNamedInstance + N'$' + convert(sysname, serverproperty('InstanceName'))
    else
		set @real_service_name = @FullNameIfDefaultInstance

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

	declare @output_str nvarchar(300)

	set @output_str =
		@ServiceDescription + N' (' + @real_service_name + N') ' +
		N'runs under ' + @startup_account + ' account. Autostart: ' +
		case @auto_start
			when 2 then N'Yes'	-- 2 means auto-start
            when 3 THEN N'No'	-- 3 means don't auto-start
			else N'No'			-- Safety net
		end

	print @output_str

end
go

declare @Cmd nvarchar(2000)

-- -----------------------------------------------
-- server description (name, version, edition, SP)
-- -----------------------------------------------

declare
 @TmpVer varchar(50),
 @SrvDescr varchar(100)

set @SrvDescr = 'SQL Server '
set @TmpVer = convert(varchar, serverproperty('ProductVersion'))

/*if @TmpVer like '8.%'
	set @SrvDescr = @SrvDescr + '2000'
else*/
	if @TmpVer like '9.%'
		set @SrvDescr = @SrvDescr + '2005'
	else
		if @TmpVer like '10.0.%'
			set @SrvDescr = @SrvDescr + '2008'
		else
			if @TmpVer like '10.50.%'
				set @SrvDescr = @SrvDescr + '2008 R2'
			else
				if @TmpVer like '11.0.%'
					set @SrvDescr = @SrvDescr + '2012'
				else
					if @TmpVer like '12.0.%'
						set @SrvDescr = @SrvDescr + '2014'
					else
						if @TmpVer like '13.0.%'
							set @SrvDescr = @SrvDescr + '2016'
						else
							set @SrvDescr = @SrvDescr + @TmpVer

set @SrvDescr = @SrvDescr + ', ' +  convert(varchar, serverproperty('Edition')) + ', ' + convert(varchar, serverproperty('ProductLevel')) + ' (' + @TmpVer + ')'

print 'Instance ' + isnull(@@servername, '???') + ' is ' + @SrvDescr

print ''

-- -----------------
-- server properties
-- -----------------

execute #tmp_startup_account 'SQL Server', 'MSSQLSERVER', 'MSSQL'
execute #tmp_startup_account 'SQL Server Agent', 'SQLServerAgent', 'SQLAgent'
print ''

set nocount on

create table #SrvProp (
	[Server Property Name] varchar(30),
	[Value] varchar(127)
)

create table #TmpMSVer (
	[Index] integer,
	[Name] varchar(200),
	Internal_Value varchar(200),
	Character_Value varchar(200))

insert into #TmpMSVer execute xp_msver 'Language'

declare @LangName varchar(100)

select @LangName = convert(varchar, Character_Value) from #TmpMSVer

drop table #TmpMSVer

insert into #SrvProp values ('Language', @LangName)
insert into #SrvProp values ('Server collation', convert(varchar, serverproperty('Collation')))

if serverproperty('IsIntegratedSecurityOnly') = 1
	insert into #SrvProp values ('Security mode', 'Windows Integrated')
else
	insert into #SrvProp values ('Security mode', 'Mixed')

if serverproperty('IsClustered') = 1
	insert into #SrvProp values ('Cluster', 'Part of the failover cluster.')
else
	insert into #SrvProp values ('Cluster', 'Not a part of the failover cluster.')

declare
	@RetRes integer,
	@RegValue nvarchar(2000)

execute @RetRes = master..xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\Setup',
	N'SQLPath',
	@RegValue output, 'no_output'

if @RetRes<>0
	insert into #SrvProp values ('Installation path', 'Cannot be determined.')
else
	insert into #SrvProp values ('Installation path', @RegValue)

set @RegValue = null

execute @RetRes = master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\MSSQLServer',
	N'DefaultData',
	@RegValue output, 'no_output' 

if @RegValue is not null
	insert into #SrvProp values ('Data root path', @RegValue)
else
	begin
		execute @RetRes = master.dbo.xp_instance_regread
			N'HKEY_LOCAL_MACHINE',
			N'Software\Microsoft\MSSQLServer\Setup',
			N'SQLDataRoot',
			@RegValue output, 'no_output'
		if @RetRes<>0
			insert into #SrvProp values ('Data root path', 'Cannot be determined.')
		else
			insert into #SrvProp values ('Data root path', @RegValue + '\Data')
	end

insert into #SrvProp values ('Min Server Memory', convert(varchar, (select value from master..syscurconfigs where config=1543)))
insert into #SrvProp values ('Max Server Memory', convert(varchar, (select value from master..syscurconfigs where config=1544)))
insert into #SrvProp values ('Max DOP', convert(varchar, (select value from master..syscurconfigs where config=1539)))

insert into #SrvProp values
	(
		'Startup SPs',
		replace(
			replace(
				replace(
					convert(
						varchar(max),
						(select name from master.sys.procedures where is_auto_executed = 1 order by 1 for xml raw, type)
					),
					'"/><row name="', ', '), -- zuerst zwischen den Einträgen Kommas einpflanzen
				'<row name="', ''), -- das brauchen wir nicht, steht ganz am Anfang der Zeile
			'"/>', '') -- das brauchen wir auch nicht, steht ganz am Ende der Zeile
	)

select * from #SrvProp

set nocount off

drop table #SrvProp

-- ---------------------
-- databases information
-- ---------------------

set nocount on

create table #dbinfo (
 [database] varchar(100) not null,
 [filename] varchar(40) not null,
 [size] numeric(10, 2) not null,
 [type] varchar(4) not null
)

declare AllDatabases cursor local for
 select name from sysdatabases /*where name not in ('master', 'model', 'msdb', 'tempdb')*/ order by 1
declare @CurDBName nvarchar(128)

open AllDatabases
fetch next from AllDatabases into @CurDBName

while @@fetch_status=0
	begin
		if databasepropertyex(@CurDBName, 'Status') = 'ONLINE'
			begin
				set @Cmd = 'use [' + @CurDBName + '] insert into #dbinfo
					select
						convert(varchar(20), db_name()) as [database],
						convert(varchar(40), filename) as [filename],
						convert(numeric(10, 2), convert(numeric, (size * 8)) / 1024) as [size],
						case when status & 0x40 = 0x40 then ''LOG'' else ''DATA'' end as [type]
					from sysfiles'
				execute (@Cmd)
			end
		else
			begin
					insert into #dbinfo ([database], [filename], [size], [type])
						values (@CurDBName + ' [' + convert(varchar, databasepropertyex(@CurDBName, 'Status')) + ']', '???', 0, 'DATA')
					insert into #dbinfo ([database], [filename], [size], [type])
						values (@CurDBName + ' [' + convert(varchar, databasepropertyex(@CurDBName, 'Status')) + ']', '???', 0, 'LOG')
			end
		if @@error<>0 break
		fetch next from AllDatabases into @CurDBName
	end

deallocate AllDatabases

print 'List of the databases:'
print ''

select
	left(DBNames.[database], 30) as [database],
	left(replicate(' ', 15 - len(DBDataSize.[vcsize])) + DBDataSize.[vcsize], 15) as [DATA size, MB],
	left(replicate(' ', 15 - len(DBLogSize.[vcsize])) + DBLogSize.[vcsize], 15) as [LOG size, MB],
	convert(varchar, databasepropertyex(DBNames.[database], 'Collation')) as [collation],
	case when databasepropertyex(DBNames.[database], 'Collation') = serverproperty('Collation') then
		'Yes' else 'No' end as [= server?],
	convert(varchar(11), databasepropertyex(DBNames.[database], 'Recovery')) as [recovery],
	case when databasepropertyex(DBNames.[database], 'IsAutoShrink') = 1 then
		'Yes' else 'No' end as [autoshrink],
	case
		when databasepropertyex(DBNames.[database], 'IsPublished') = 1 and databasepropertyex(DBNames.[database], 'IsMergePublished') = 1
			then 'transactional, merge'
		when databasepropertyex(DBNames.[database], 'IsPublished') = 1 and databasepropertyex(DBNames.[database], 'IsMergePublished') = 0
			then 'transactional'
		when databasepropertyex(DBNames.[database], 'IsPublished') = 0 and databasepropertyex(DBNames.[database], 'IsMergePublished') = 1
			then 'merge'
		else
			'No'
		end as [published],
	case when databasepropertyex(DBNames.[database], 'IsSubscribed') = 1 then 'Yes' else 'No' end as [subscribed]
from
 (select distinct [database] from #dbinfo) as DBNames
	inner join (select [database], convert(varchar, sum([size])) as [vcsize] from #dbinfo where [type]='DATA' group by [database]) as DBDataSize
		on DBNames.[database]=DBDataSize.[database]
	inner join (select [database], convert(varchar, sum([size])) as [vcsize] from #dbinfo where [type]='LOG' group by [database]) as DBLogSize
		on DBNames.[database]=DBLogSize.[database]
order by 1

drop table #dbinfo

-- --------------------------
-- SQL Agent jobs information
-- --------------------------

create table #TmpUserJobs (
	[job name] varchar(200),
	[category name] varchar(200),
	[enabled] varchar(3))

declare
	@TmpJobsCount integer,
	@TmpUserJobsCount integer,
	@MaxJobNameLen integer,
	@MaxCatNameLen integer

set nocount on

insert into #TmpUserJobs
select
	SJ.name as [job name],
	SC.name as [category name],
	case SJ.enabled when 0 then 'No' else 'Yes' end as [enabled]
from
	msdb..sysjobs SJ
		inner join msdb..syscategories SC on SJ.category_id=SC.category_id
where
	SC.name not like 'REPL-%'
order by 1

select @TmpJobsCount = count(*) from msdb..sysjobs

select @TmpUserJobsCount = count(*) from #TmpUserJobs
if @TmpUserJobsCount is null
	set @TmpUserJobsCount = 0

select @MaxJobNameLen = max(len([job name])) from #TmpUserJobs
select @MaxCatNameLen = max(len([category name])) from #TmpUserJobs

set nocount off

print 'Total number of SQL Agent jobs: ' + convert(varchar, @TmpJobsCount)
print 'Number of "user" jobs (all except replication): ' + convert(varchar, @TmpUserJobsCount)
print ''

if @TmpUserJobsCount > 0
	begin
		print 'List of the "user" jobs:'
		print ''
		set @Cmd = '
		select
			left([job name], ' + convert(varchar, @MaxJobNameLen) + ') as [job name],
			left([category name], ' + convert(varchar, @MaxCatNameLen) + ') as [category name],
			[enabled]
		from #TmpUserJobs'
		set nocount on
		execute (@Cmd)
		set nocount off
	end

drop table #TmpUserJobs
go

drop procedure #tmp_startup_account
go

-- ----------------------------
-- sys.master_files information
-- ----------------------------

print 'Contents of the sys.master_files:'
print ''

select
	convert(varchar(30), db_name(MF.database_id)) as database_name,
	MF.file_id,
	left(MF.type_desc, 10) as type_desc,
	convert(varchar(30), MF.name) as file_name,
	convert(varchar(15), convert(numeric(10, 2), convert(numeric, isnull(DF.size, MF.size))/128)) as Size_MB,
	convert(varchar(15), case MF.growth
		when 0 then convert(numeric(10, 2), convert(numeric, isnull(DF.size, MF.size))/128)
		else
			case MF.max_size
				when -1 then null
				when 268435456 then null
				else convert(numeric(10, 2), convert(numeric, MF.max_size)/128)
			end
	end) as Max_Size_MB,
	convert(varchar(15), case
		when MF.growth=0 then null
		when MF.is_percent_growth=0 then convert(numeric(10, 2), convert(numeric, MF.growth)/128)
		else null
	end) as Growth_MB,
	convert(varchar(15), case
		when MF.growth=0 then null
		when MF.is_percent_growth=1 then MF.growth
		else null
	end) as Growth_Prct,
	MF.physical_name
from
	sys.master_files MF
		left join tempdb.sys.database_files DF on
			MF.database_id=db_id('tempdb') and MF.file_id=DF.file_id
order by 1, 3 desc, 4

use master

-- --------------
-- best practices
-- --------------

-- diverse Einstellungen

declare
	@RCAllowed integer = (select convert(integer, value_in_use) from sys.configurations where name='remote access'),
	@RCTimeOut integer = (select convert(integer, value_in_use) from sys.configurations where name='remote query timeout (s)'),
	@SAPolicyChecking integer = (select is_policy_checked from sys.sql_logins where name='sa'),
	@NumberProtocols integer,
	@jobhistory_max_rows integer,
	@jobhistory_max_rows_per_job integer

execute xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\MSSQLServer',
	N'NumErrorLogs',
	@NumberProtocols output,
	N'no_output'

execute xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',
	N'JobHistoryMaxRows',
	@jobhistory_max_rows output,
	N'no_output'

execute xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',
	N'JobHistoryMaxRowsPerJob',
	@jobhistory_max_rows_per_job output,
	N'no_output'

-- Mail-Einstellungen

declare
	@MailProfileCount integer,
	@MailAccountName varchar(255),
	@MailAccountMail varchar(255),
	@MailProfileIsPublic bit,
	@MailProfileIsDefault bit

select
	@MailProfileCount = coalesce(count(*), 0)
from
	msdb.dbo.sysmail_profile

select
	@MailAccountName = SMA.name,
	@MailAccountMail = SMA.email_address,
	@MailProfileIsPublic = case when SMPP.principal_sid is null then convert(bit, 0) else convert(bit, -1) end,
	@MailProfileIsDefault = case when SMPP.is_default is null then convert(bit, 0) else SMPP.is_default end
from
	msdb.dbo.sysmail_profile SMP
		inner join msdb.dbo.sysmail_profileaccount SMPA on SMP.profile_id=SMPA.profile_id
		inner join msdb.dbo.sysmail_account SMA on SMPA.account_id=SMA.account_id
		left join msdb.dbo.sysmail_principalprofile SMPP on SMP.profile_id=SMPP.profile_id
where
	SMP.name='MaintDB-MailProfile'

if @@rowcount<>1
	begin
		set @MailAccountName='BAD CONFIG'
		set @MailAccountMail=null
		set @MailProfileIsPublic=null
		set @MailProfileIsDefault=null
	end

-- Ausgabe

select
	@RCAllowed as '[ibp01] rem. conn. allowed',
	@RCTimeOut as '[ibp02] rem. query timeout (s)',
	@SAPolicyChecking as '[ibp03] sa check_policy',
	coalesce(@NumberProtocols, 6) as '[ibp04] # of protocols',
	@jobhistory_max_rows as '[ibp05] max. job history log size',
	@jobhistory_max_rows_per_job as '[ibp06] max. job history rows per job'

select
	@MailProfileCount as '[ibp07] profile count',
	left(@MailAccountName, 40) as '[ibp08] mail account',
	left(@MailAccountMail, 40) as '[ibp09] mail address',
	@MailProfileIsPublic as '[ibp10] public profile',
	@MailProfileIsDefault as '[ibp11] default profile'
