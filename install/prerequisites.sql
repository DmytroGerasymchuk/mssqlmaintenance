declare @RetRes integer

execute @RetRes = master.dbo.xp_create_subdir '$(JobLogDir)'

if @RetRes<>0
	raiserror('master.dbo.xp_create_subdir failed.', 16, 1)
go

if not exists (select 1 from msdb.dbo.sysmail_profile where name='$(MaintDBName)-MailProfile')
	raiserror ('The required mail profile is not configured in your system.', 16, 1)
go

execute sp_configure 'show advanced options', 1
reconfigure with override
execute sp_configure 'clr enabled', 1
reconfigure with override
go

use [$(MaintDBName)]
go

if exists (select * from sys.schemas where name='file_ops')
	print 'file_ops schema already exists.'
else
	begin
		print 'file_ops schema does not exist - creating...'
		execute ('create schema file_ops')
	end
go

alter database [$(MaintDBName)] set trustworthy on
go

if exists (select 1 from sys.assemblies where name='maintdb_file_ops')
	begin
		print 'Assembly maintdb_file_ops already exists in the database. Cleaning up existing objects...'
		drop function file_ops.ls
		drop procedure file_ops.mv
		drop procedure file_ops.ren
		drop procedure file_ops.rm
		drop assembly maintdb_file_ops
	end
go

create assembly maintdb_file_ops from 'C:\TEMP\maintdb_file_ops.dll'
with permission_set=external_access
go

create function file_ops.ls(@Path nvarchar(max), @Pattern nvarchar(max))
returns table (
	Name nvarchar(max),	Extension nvarchar(max),
	SizeBytes bigint,
	CreationTime datetime, CreationTimeUtc datetime,
	LastWriteTime datetime, LastWriteTimeUtc datetime
)
as external name maintdb_file_ops.[DS.file_ops].ls
go

create procedure file_ops.mv(@SourcePathName nvarchar(max), @DestPathName nvarchar(max))
as external name maintdb_file_ops.[DS.file_ops].mv
go

create procedure file_ops.ren(@Path nvarchar(max), @OldName nvarchar(max), @NewName nvarchar(max))
as external name maintdb_file_ops.[DS.file_ops].ren
go

create procedure file_ops.rm(@PathName nvarchar(max))
as external name maintdb_file_ops.[DS.file_ops].rm
go
