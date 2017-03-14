declare
	@DataRoot nvarchar(512),
	@DefaultData nvarchar(512),
	@DefaultLog nvarchar(512)

execute master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\Setup',
	N'SQLDataRoot',
	@DataRoot OUTPUT, N'no_output'
    
execute master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\MSSQLServer',
	N'DefaultData',
	@DefaultData OUTPUT, N'no_output'

execute master.dbo.xp_instance_regread
	N'HKEY_LOCAL_MACHINE',
	N'Software\Microsoft\MSSQLServer\MSSQLServer',
	N'DefaultLog',
	@DefaultLog OUTPUT, N'no_output'
    
set @DefaultData = isnull(@DefaultData, @DataRoot + '\DATA')
set @DefaultLog  = isnull(@DefaultLog,  @DataRoot + '\DATA')
    
print 'Default instance''s data dir: [' + @DefaultData + ']'
print 'Default instance''s log dir:  [' + @DefaultLog +  ']'

declare
	@DataFile nvarchar(1024),
	@LogFile nvarchar(1024),
	@Cmd nvarchar(max)

set @DataFile = @DefaultData + N'\$(MaintDBName).mdf'
set @LogFile = @DefaultLog + N'\$(MaintDBName)_log.ldf'

set @Cmd = N'
create database [$(MaintDBName)]
on primary
(name=''$(MaintDBName)'', filename=''' + @DataFile + ''', SIZE=51200KB, FILEGROWTH=51200KB)
log on
(name=''$(MaintDBName)_log'', filename=''' + @LogFile + ''', SIZE=51200KB, FILEGROWTH=51200KB)
'

execute (@Cmd)

execute ('alter database [$(MaintDBName)] set arithabort on')
execute ('alter authorization on database::[$(MaintDBName)] to sa')

if serverproperty('EngineEdition')=4
	begin
		print 'This is an Express edition, so setting AUTO_CLOSE option to OFF...'
		alter database [$(MaintDBName)] set auto_close off
	end