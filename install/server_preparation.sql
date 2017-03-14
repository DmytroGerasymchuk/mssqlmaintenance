execute sp_configure 'show advanced options', 1
reconfigure with override
execute sp_configure 'remote query timeout', 0
reconfigure with override

alter login sa with check_policy=off

execute xp_instance_regwrite
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\MSSQLServer',
	N'NumErrorLogs',
	REG_DWORD,
	15

if serverproperty('EngineEdition')=4
	print 'This is an Express edition. SQL Server Agent is not present.'
else
	begin	
		print 'Setting SQL Server Agent properties according to IBP...'
		execute msdb.dbo.sp_set_sqlagent_properties
			@jobhistory_max_rows=2000,
			@jobhistory_max_rows_per_job=100
		print '...OK'
	end
go

create procedure #setfsize
	@DBName varchar(255),
	@NewSizeKB integer,
	@NewFileGrowthKB integer as
begin

	print ''
	print 'Setting pre-defined sizes / growths for database: ' + @DBName + '...'

	declare @finfo table (fname sysname, fsizekb integer)
	declare @Cmd nvarchar(max), @SizePart nvarchar(max)

	set @Cmd = 'select name, size*8 from [' + @DBName + '].sys.database_files'

	set nocount on
	insert into @finfo execute (@Cmd)
	set nocount off

	declare @fname sysname, @fsizekb integer
	declare finfolist cursor local fast_forward for
		select fname, fsizekb from @finfo

	open finfolist
	fetch next from finfolist into @fname, @fsizekb

	while @@fetch_status=0
		begin
			set @SizePart = ''
			if @NewSizeKB is not null
				if @NewSizeKB > @fsizekb
					set @SizePart = 'SIZE = ' + convert(nvarchar, @NewSizeKB) + 'KB, '

			set @Cmd =
				'ALTER DATABASE [' + @DBName + '] MODIFY FILE (NAME=N''' + @fname + ''', ' +
				@SizePart +
				'MAXSIZE=UNLIMITED, FILEGROWTH=' + convert(nvarchar, @NewFileGrowthKB)  + 'KB)'

			print @Cmd
			execute (@Cmd)
			if @@error<>0
				return

			fetch next from finfolist into @fname, @fsizekb
		end

	deallocate finfolist

end
go

execute #setfsize 'model', 51200, 102400
execute #setfsize 'msdb',   null,  51200
execute #setfsize 'tempdb', null, 102400
go

drop procedure #setfsize
go
