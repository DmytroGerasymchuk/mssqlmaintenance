use [$(MaintDBName)]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

if exists (select 1 from sys.schemas where name='base')
	begin
		print '"base" schema detected. This means, "base" module is already installed.'
		print 'Not everything from "base" module may be installed multiple times.'
		print 'Setting NOEXEC=ON...'
		set noexec on
	end
go

create schema base
go

create table base.tblVersion (
	strModuleName varchar(255) not null,
	lngVersionMajor integer not null,
	lngVersionMinor integer not null,
	constraint PK_base_tblVersion primary key (strModuleName)
)
go

create table base.tblConfigValue (
	strConfigValueName varchar(255) not null,
	strValue varchar(255) not null,
	constraint PK_base_tblConfigValues primary key (strConfigValueName)
)
go

set nocount on
insert into base.tblConfigValue (strConfigValueName, strValue)
	values
		('tools.wmi_xml_dir', '$(WmiXmlDir)'),
		
		('maint.max_log_size', '1000'),
		('maint.history_retention', '4'),
		('maint.backup_path', '$$$NOTSET$$$'),
		('maint.backup_retention_days', '4'),

		('base.mail_profile', db_name() + '-MailProfile'),

		('maint.notify_operator', db_name() + '-DBA'),
		('tools.notify_operator', db_name() + '-DBA'),
		('pol.notify_operator', db_name() + '-DBA')
set nocount off
go

create table base.tblVolumeBound (
	strVolumeName varchar(100) not null,
	numWarningPct numeric(10, 2) not null,
	numErrorPct numeric(10, 2) not null,
	
	constraint PK_base_tblVolumeBound primary key (strVolumeName),
	constraint CK_base_tblVolumeBound check
		(
			numWarningPct between 1 and 100 and
			numErrorPct between	1 and 100 and
			numWarningPct < numErrorPct
		)
)
go

create table base.tblIndividualBackupSetting (
	strDBName varchar(255) not null,
	lngBackupRetentionDays integer not null,

	constraint PK_base_tblIndividualBackupSetting primary key (strDBName)
)
go

set noexec off
print 'NOEXEC is now set to OFF...'
go

if exists (select 1 from sys.procedures where schema_name(schema_id)='base' and name='usp_prepare_object_creation')
	execute ('drop procedure base.usp_prepare_object_creation')
go

create procedure base.usp_prepare_object_creation
	@SchemaName varchar(255),
	@ObjectName varchar(255),
	@Verbose bit = 0 as

begin

	declare @Cmd varchar(max)

	if exists (select * from sys.schemas where name=@SchemaName)
		begin
			if @Verbose=convert(bit, -1)
				print @SchemaName + ' schema already exists.'
		end
	else
		begin
			if @Verbose=convert(bit, -1)
				print @SchemaName + ' schema does not exist - creating...'
			set @Cmd = 'create schema [' + @SchemaName + ']'
			execute (@Cmd)
		end

	if @ObjectName is not null
		begin

			declare @ObjType varchar(2) =
				(
					select rtrim(O.type)
					from sys.objects O
					where schema_name(o.schema_id)=@SchemaName and O.name=@ObjectName
				)

			declare @FullName varchar(max) = '[' + @SchemaName + '].[' + @ObjectName + ']'

			if @ObjType is not null
				begin
					if @Verbose=convert(bit, -1)
						print 'Object ' + @FullName + ' already exists. It will be dropped first.'
					set @Cmd = 
						'drop ' +
						case @ObjType
							when 'U' then 'table'
							when 'P' then 'procedure'
							when 'FN' then 'function' -- scalar function
							when 'IF' then 'function' -- in-line table-function
							when 'TF' then 'function' -- table function
						end +
						' ' + @FullName
					execute (@Cmd)
				end

		end

end
go

execute base.usp_prepare_object_creation 'base', 'udf_errmsg'
go

create function base.udf_errmsg()
returns varchar(max) as
begin
	return 'Server Error ' + convert(varchar, error_number()) + ' at ' + isnull(error_procedure(), 'UNKNOWN') + ', Line ' + isnull(convert(varchar, error_line()), 'UNKNOWN') + ': ' + error_message()
end
go

execute base.usp_prepare_object_creation 'base', 'usp_update_module_info'
go

create procedure base.usp_update_module_info
	@ModuleName varchar(255),
	@VersionMajor integer,
	@VersionMinor integer as
	
begin

	print
		'Updating module information: ' + @ModuleName +
		' V' + convert(varchar, @VersionMajor) + '.' + convert(varchar, @VersionMinor)
		
	set nocount on

	;with NewVersion as (
		select
			@ModuleName as strModuleName,
			@VersionMajor as lngVersionMajor,
			@VersionMinor as lngVersionMinor
	)
	merge
		into base.tblVersion T
		using NewVersion S
		on T.strModuleName=S.strModuleName
	when not matched by target
		then insert (strModuleName, lngVersionMajor, lngVersionMinor)
			values (S.strModuleName, S.lngVersionMajor, S.lngVersionMinor)
	when matched
		then update
			set
				T.lngVersionMajor=S.lngVersionMajor,
				T.lngVersionMinor=S.lngVersionMinor;

end
go

execute base.usp_prepare_object_creation 'base', 'udf_expand_dbpattern_raw'
go

create function base.udf_expand_dbpattern_raw (@Pattern varchar(max))
returns @DBFilter table (
	dbname sysname not null,
	dbstate tinyint not null
) as
begin

	if @Pattern='*system' -- all system databases
		insert into @DBFilter values
			('master', 0),
			('model', 0),
			('msdb', 0)
	else
		if @Pattern='*user' -- all user databases
			insert into @DBFilter
				select name, [state] from sys.databases
				where name not in ('master', 'tempdb', 'model', 'msdb')
		else
			if @Pattern='*' -- all databases
				insert into @DBFilter
					select name, [state] from sys.databases
			else
				if @Pattern like 'list(%)' -- comma-separated list of database names
					begin
						declare @ListBody varchar(max) = substring(@Pattern, 6, len(@Pattern) - 6)

						if @ListBody='$usedefinition' -- pre-defined list
							select @ListBody = strValue
							from base.tblConfigValue
							where strConfigValueName='base.database_list'

						declare @X xml =
							convert(xml,
								'<Value>' +
								replace(@ListBody, ',',	'</Value><Value>') +
								'</Value>'
							)
						insert into @DBFilter
							select name, [state] from sys.databases 
							where name in (
								select ltrim(rtrim(T.C.value('.', 'varchar(max)')))
								from @X.nodes('Value') as T(C)
							)
					end
				else -- only one specific database
					insert into @DBFilter
						select name, [state] from sys.databases
						where name=@Pattern

	return

end
go

execute base.usp_prepare_object_creation 'base', 'usp_for_each_db'
go

create procedure base.usp_for_each_db
	@Pattern varchar(max),
	@Stmt nvarchar(max),
	@SkipReadOnly bit = 0,
	@UseReplaceInsteadParametrization bit = 0 as
	
begin

	begin try

		declare @DBFilter table (
			dbname sysname not null,
			dbstate tinyint not null
		)

		set nocount on
		insert into @DBFilter
			select dbname, dbstate
			from base.udf_expand_dbpattern_raw(@Pattern)
		set nocount off

		set nocount on
		delete from DBF
		from
			@DBFilter DBF
		where
			DBF.dbstate<>0 -- only ONLINE databases
		if @@rowcount<>0
			begin
				print 'Some non-ONLINE databases were detected.'
				print 'They will be skipped.'
				print ''
			end
		set nocount off

		if @SkipReadOnly=convert(bit, -1)
			begin
				set nocount on
				delete from DBF
				from
					@DBFilter DBF
				where
					databasepropertyex(DBF.dbname, 'Updateability')='READ_ONLY'
				if @@rowcount<>0
					begin
						print 'Some READ_ONLY databases were detected.'
						print 'They will be skipped.'
						print ''
					end
				set nocount off
			end

		print 'List of the affected databases:'
		print ''
		set nocount on
		select left(dbname, 40) as Name, db_id(dbname) as ID from @DBFilter order by 1
		set nocount off

		declare DBNames cursor local fast_forward for
			select dbname from @DBFilter order by 1
			
		declare @CurDBName varchar(255), @FirstInList bit = convert(bit, -1)
		
		open DBNames
		fetch next from DBNames into @CurDBName
		
		while @@fetch_status=0
			begin
				declare @OutStr varchar(max) = convert(varchar, getdate(), 120) + ' Command: ' + @Stmt + '; @DBName=' + @CurDBName
				if @FirstInList=convert(bit, -1) set @FirstInList = convert(bit, 0)	else print ''
				print replicate('-', len(@OutStr))
				print @OutStr
				print replicate('-', len(@OutStr))
				if @UseReplaceInsteadParametrization = convert(bit, 0)
					execute sp_executesql @Stmt, N'@DBName varchar(255)', @CurDBName
				else
					begin
						declare @PrepStmt varchar(max) = replace(@Stmt, '@DBName', @CurDBName)
						execute (@PrepStmt)
					end
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

execute base.usp_prepare_object_creation 'base', 'udf_get_mail_profile'
go

create function base.udf_get_mail_profile()
returns varchar(255) as
begin

	return (select strValue from base.tblConfigValue where strConfigValueName='base.mail_profile')

end
go

execute base.usp_prepare_object_creation 'base', 'udf_get_operator_mail'
go

create function base.udf_get_operator_mail(@BaseConfigValueName varchar(255))
returns varchar(255) as
begin

	return
		(
			select email_address from msdb.dbo.sysoperators where name=
				(select strValue from base.tblConfigValue where strConfigValueName=@BaseConfigValueName)
		)

end
go

execute base.usp_update_module_info 'base', 1, 1
go
