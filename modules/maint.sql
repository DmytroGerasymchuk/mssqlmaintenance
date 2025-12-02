use [$(MaintDBName)]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

execute base.usp_prepare_object_creation 'maint', 'usp_updatestats'
go

create procedure maint.usp_updatestats
	@DBNamePattern varchar(max) as
begin
	execute base.usp_for_each_db @DBNamePattern, 'use [@DBName] execute sp_updatestats', @SkipReadOnly=1, @UseReplaceInsteadParametrization=1
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_index_maint'
go

create procedure maint.usp_index_maint
	@DBNamePattern varchar(255),
	@MinFragPrctToReorg float = 10.0,
	@MinFragPrctToRebuild float = 30.0,
	@DisableRecoveryModelSwitching bit = 0,
	@Verbose bit = 0 as
begin

	declare @Cmd varchar(max) =
		'execute maint.int_index_maint @DBName, ' +
		convert(varchar, @MinFragPrctToReorg) + ', ' +
		convert(varchar, @MinFragPrctToRebuild) + ', ' +
		case @DisableRecoveryModelSwitching when convert(bit, 0) then '0' else '1' end + ', ' +
		case @Verbose when convert(bit, 0) then '0' else '1' end

	execute base.usp_for_each_db @DBNamePattern, @Cmd, 1 -- Skip Read-Only

end
go

execute base.usp_prepare_object_creation 'maint', 'int_index_maint'
go

create procedure maint.int_index_maint
	@DBName varchar(255),
	@MinFragPrctToReorg float,
	@MinFragPrctToRebuild float,
	@DisableRecoveryModelSwitching bit,
	@Verbose bit as
		
begin

	declare
		@DBID integer = db_id(@DBName),
		@FullModel nvarchar(128) = 'FULL',
		@PrevModel nvarchar(128),
		@IsMirrored bit = 0,
		@SwitchBackRequired bit = 0,
		@Cmd varchar(max)
			
	begin try

		-- Voraussetzungen prüfen und ggf. Recovery-Model umschalten

		set @PrevModel = convert(nvarchar(128), databasepropertyex(@DBName, 'Recovery'))
		if @PrevModel is null
			raiserror('Error determining recovery model.', 16, 1)

		if exists (select 1 from sys.database_mirroring where database_id=@DBID and mirroring_guid is not null)
			set @IsMirrored = convert(bit, -1)

		if @PrevModel=@FullModel and @IsMirrored=convert(bit, 0) and  @DisableRecoveryModelSwitching=convert(bit, 0)
			begin
				print 'Switching the database to BULK_LOGGED recovery for performance reasons...'
				set @Cmd = 'alter database [' + @DBName + '] set recovery BULK_LOGGED'
				execute (@Cmd)
				set @SwitchBackRequired=convert(bit, -1)
			end

		-- die Information über Index-Fragmentierung einlesen

		declare @IndexList table (
			_object_id integer,
			_table_name varchar(255),
			_index_id integer,
			_index_name varchar(255),
			_allow_page_locks integer
		)

		set @Cmd = 'use [' + @DBName + ']
			select
				so.object_id as _object_id, ''[''+ ss.name + ''].['' + so.name + '']'' as _table_name,
				si.index_id as _index_id, ''[''+ si.name + '']'' as _index_name,
				si.allow_page_locks as _allow_page_locks
			from
				sys.objects so
					inner join sys.indexes si on so.object_id = si.object_id 
					inner join sys.schemas ss on ss.schema_id = so.schema_id
			where
				so.type=''U'' and -- user tables
				si.index_id>0 and -- 0 = heap
				si.is_disabled=0 -- makes no sense to work with disabled indexes'

		set nocount on
		insert into @IndexList execute (@Cmd)
		set nocount off

		declare IndexList cursor local fast_forward for
			select _object_id, _table_name, _index_id, _index_name, _allow_page_locks integer
			from @IndexList
			order by _table_name, _index_id

		declare
			@ObjectId integer,
			@TableName varchar(255),
			@IndexId integer,
			@IndexName varchar(255),
			@AllowPageLocks integer

		open IndexList
		fetch next from IndexList into @ObjectId, @TableName, @IndexId, @IndexName, @AllowPageLocks

		while @@fetch_status=0
			begin
				declare OneIndex cursor local fast_forward for  
					-- there may be multiple entries per partition, for example in columnstore indices
					select partition_number, max(avg_fragmentation_in_percent) as avg_fragmentation_in_percent 
					from sys.dm_db_index_physical_stats(@DBID, @ObjectId, @IndexId, null, null)
					group by partition_number
					order by partition_number

				declare
					@PartitionNumber integer,
					@FragPrct float

				open OneIndex
				fetch next from OneIndex into @PartitionNumber, @FragPrct

				while @@fetch_status = 0
					begin
						declare @Msg varchar(max) =
							convert(varchar, getdate(), 120) + ' ' +
							'Table: ' + @TableName + ' ' +
							'Index: ' + @IndexName + ' ' +
							'Partition#: ' + convert(varchar, @PartitionNumber) + ' ' +
							'Fragmentation%: ' + convert(varchar, @FragPrct) + ' ' +
							'Action: '

						declare @Action varchar(255) = 'REBUILD'

						if @FragPrct < @MinFragPrctToReorg -- zu kleine Fragmentierung
							begin
								set @Cmd = ''
								set @Action = 'NONE'
							end
						else
							begin
								set @Cmd = 'use [' + @DBName + '] alter index ' + @IndexName + ' on ' + @TableName + ' '

								if @FragPrct <  @MinFragPrctToRebuild -- schon ordentlich fragmentiert, aber nicht genug für REBUILD
									set @Action = 'REORGANIZE'

								if @PartitionNumber > 1
									set @Action = @Action + ' PARTITION=' + convert(varchar, @PartitionNumber)

								set @Cmd = @Cmd + @Action
							end

						if @Action = 'REORGANIZE' and @AllowPageLocks = 0
							begin
								set @Cmd = ''
								set @Action = 'REORG_ABORT (Reason: ALLOW_PAGE_LOCKS = OFF)'
							end

						set @Msg = @Msg + @Action

						if @Verbose=convert(bit, -1) or @Action<>'NONE'
							print @Msg

						if @Cmd<>''
							execute (@Cmd)

						fetch next from OneIndex into @PartitionNumber, @FragPrct
					end
	
				deallocate OneIndex

				fetch next from IndexList into @ObjectId, @TableName, @IndexId, @IndexName, @AllowPageLocks
			end

		deallocate IndexList

		-- fertig, ggf. Ogiginal-Einstellungen wiederherstellen
				
		if @SwitchBackRequired=convert(bit, -1)
			begin
				print 'Restoring original recovery setting (' + @PrevModel + ')...'
				set @Cmd = 'alter database [' + @DBName + '] set recovery ' + @PrevModel
				execute (@Cmd)
			end

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'

		if @SwitchBackRequired=convert(bit, -1)
			begin
				print 'Restoring original recovery setting (' + @PrevModel + ')...'
				set @Cmd = 'alter database [' + @DBName + '] set recovery ' + @PrevModel
				execute (@Cmd)
			end

		raiserror(@EM, 16, 1)
	end catch
		
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_db_check'
go

create procedure maint.usp_db_check
	@DBNamePattern varchar(max) as
begin
	execute base.usp_for_each_db @DBNamePattern, 'dbcc checkdb(@DBName) with physical_only, no_infomsgs'
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_db_shrink'
go

create procedure maint.usp_db_shrink
	@DBNamePattern varchar(max) as
begin
	execute base.usp_for_each_db @DBNamePattern, 'dbcc shrinkdatabase(@DBName, truncateonly)', @SkipReadOnly=1
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_history_cleanup'
go

create procedure maint.usp_history_cleanup as
begin

	begin try

		declare @HistoryRetention integer = convert(integer, base.udf_get_config_value('maint.history_retention'))
		if @HistoryRetention is null raiserror('maint.history_retention is not defined.', 16, 1)

		declare
			@CutoffDate date = dateadd(week, -@HistoryRetention, getdate()),
			@RetRes integer

		print 'Cutoff date is: ' + convert(varchar, @CutoffDate, 120)

		-- maintenance plan logs
		execute @RetRes = msdb.dbo.sp_maintplan_delete_log @oldest_time=@CutoffDate
		if @RetRes<>0 raiserror('Failure on call to msdb.dbo.sp_maintplan_delete_log.', 16, 1)
		
		-- job history
		execute @RetRes = msdb.dbo.sp_purge_jobhistory @oldest_date=@CutoffDate
		if @RetRes<>0 raiserror('Failure on call to msdb.dbo.sp_purge_jobhistory.', 16, 1)

		-- backup history
		execute @RetRes = msdb.dbo.sp_delete_backuphistory @CutoffDate
		if @RetRes<>0 raiserror('Failure on call to msdb.dbo.sp_delete_backuphistory.', 16, 1)

		-- dbmail
		-- 1. sent items
		execute @RetRes = msdb.dbo.sysmail_delete_mailitems_sp @sent_before=@CutoffDate
		if @RetRes<>0 raiserror('Failure on call to msdb.dbo.sysmail_delete_mailitems_sp.', 16, 1)
		-- 2. dbmail log
		execute @RetRes = msdb.dbo.sysmail_delete_log_sp @logged_before=@CutoffDate
		if @RetRes<>0 raiserror('Failure on call to msdb.dbo.sysmail_delete_log_sp.', 16, 1)
	
	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_prepare_object_creation 'maint', 'usp_backup_init'
go

create procedure maint.usp_backup_init
	@BackupMode char(1),
	@BackupPath varchar(255) output,
	@BackupRetentionDays integer output as
begin

	if @BackupMode not in ('F', 'D', 'T')
		raiserror('@BackupMode value is out of scope.', 16, 1)

	set @BackupPath = isnull(base.udf_get_config_value('maint.backup_path'), '$$$NOTSET$$$')
	if @BackupPath='$$$NOTSET$$$' raiserror('maint.backup_path is not defined or is not set yet to the proper value.', 16, 1)

	set @BackupRetentionDays = convert(integer, base.udf_get_config_value('maint.backup_retention_days'))
	if @BackupRetentionDays is null raiserror('maint.backup_retention_days is not defined.', 16, 1)

	-- just check - must be present, if needed by backup later
	if base.udf_get_config_value('maint.notify_operator') is null raiserror('maint.notify_operator is not defined.', 16, 1)
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_backup'
go

create procedure maint.usp_backup
	@DBNamePattern varchar(max),
	@BackupMode char(1) = 'F',
	@Verify bit = 1 as
	
begin

	begin try
	
		declare
			@BackupPath varchar(255),
			@TempPath varchar(255) = base.udf_get_config_value('maint.backup_temp_path'),
			@BackupRetentionDays integer,
			@Cmd varchar(max)

		execute maint.usp_backup_init @BackupMode, @BackupPath output, @BackupRetentionDays output

		set @Cmd =
			'execute maint.int_backup @DBName, ' +
			'''' + @BackupPath + ''', ' +
			case when @TempPath is null then 'NULL' else '''' + @TempPath + '''' end + ', ' +
			convert(varchar, @BackupRetentionDays) + ', ' +
			'''' + @BackupMode + ''', ' +
			case @Verify when convert(bit, 0) then '0' else '1' end

		execute base.usp_for_each_db @DBNamePattern, @Cmd
	
	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch
		
end
go

execute base.usp_prepare_object_creation 'maint', 'int_backup'
go

create procedure maint.int_backup
	@DBName varchar(255),
	@BaseDir varchar(255),
	@TempDir varchar(255),
	@BackupRetentionDays integer,
	@BackupMode char(1),
	@Verify bit as
	
begin

	begin try

		print 'Starting backup for the database: ' + @DBName
		
		declare
			@Suffix varchar(5),
			@Extension varchar(4)
		
		if @BackupMode='F'
			begin
				print 'Requested mode: full backup.'
				set @Suffix = '_full'
				set @Extension = '.bak'
			end
		if @BackupMode='D'
			begin
				print 'Requested mode: differential backup.'
				set @Suffix = '_diff'
				set @Extension = '.bak'
			end
		if @BackupMode='T'
			begin
				print 'Requested mode: transaction log backup.'
				set @Suffix = '_tran'
				set @Extension = '.trn'
				
				if databasepropertyex(@DBName, 'Recovery')='SIMPLE'
					begin
						print 'Database runs in SIMPLE recovery mode. Log backup is not possible. Returning.'
						return
					end
			end

		declare
			@SelfDone bit = convert(bit, -1),
			@RetRes integer,
			@NotifyOperator varchar(255) = base.udf_get_config_value('maint.notify_operator'),
			@MailProfile varchar(255) = base.udf_get_mail_profile(),
			@MailSubj varchar(max),
			@MailBody varchar(max)

		-- overwrite the retention days, if needed, with individual setting

		set @BackupRetentionDays =
			isnull
				(
					(select lngBackupRetentionDays from base.tblIndividualBackupSetting where strDBName=@DBName),
					@BackupRetentionDays
				)

		print 'Number of backup retention days for this database: ' + convert(varchar, @BackupRetentionDays)

		-- prepare backup path
		
		print 'Checking backup path...'
		
		declare	@TargetDir varchar(255) = @BaseDir + '\' + @DBName
		
		execute @RetRes = master.dbo.xp_create_subdir @TargetDir
		if @RetRes<>0 raiserror('Failure on call to master.dbo.xp_create_subdir.', 16, 1)
		
		-- prepare target file name
		
		declare
			@CurTS datetime = getdate(),
			@TargetFileNameOnly varchar(max),
			@TargetFile varchar(max)
	
		set @TargetFileNameOnly =
			@DBName +
			'_' + convert(varchar, @CurTS, 112) +
			'_' + replace(convert(varchar, @CurTS, 108), ':', '') +
			@Suffix +
			@Extension

		set @TargetFile = @TargetDir + '\' + @TargetFileNameOnly

		-- prepare backup file name

		declare @BackupFile varchar(max) = isnull(@TempDir, @TargetDir) + '\' + @TargetFileNameOnly

		-- proceed with backup
		
		print convert(varchar, getdate(), 120) + ' Backup will be written to: ' + @BackupFile
		
		if @BackupMode='F'
			backup database @DBName to disk=@BackupFile with stats=20

		if @BackupMode='D'
			backup database @DBName to disk=@BackupFile with differential, stats=20

		if @BackupMode='T'
			begin
				begin try
					backup log @DBName to disk=@BackupFile with stats=20
				end try
				
				begin catch
					declare @iEM varchar(max) = base.udf_errmsg()
					print convert(varchar, getdate()) + ' Error encountered: ' + @iEM
					-- maybe there was no full database backup at all yet?
					if (select max(backup_finish_date) from msdb..backupset where type = 'D' and database_name = @DBName) is null
						begin
							print 'No previous full database backup detected. Treating this as the cause of the error.'
							print 'Trying to perform a full database backup.'
							
							set @SelfDone = convert(bit, 0)

							set @MailSubj = 'Backup: Log to Full Conversion: ' + @DBName
							execute msdb.dbo.sp_notify_operator @profile_name=@MailProfile, @name=@NotifyOperator, @subject=@MailSubj
							
							execute maint.int_backup @DBName, @BaseDir, @TempDir, @BackupRetentionDays, 'F', @Verify
						end
					else
						begin -- unfortunately NO
							if @@trancount<>0 rollback transaction
							raiserror(@iEM, 16, 1)
						end
				end catch
			end

		if @SelfDone = convert(bit, -1)
			begin
				print convert(varchar, getdate(), 120) + ' Writing of backup completed.'

				if @Verify=convert(bit, -1)
					begin
						print 'Verifying backup media...'
						restore verifyonly from disk=@BackupFile with stats=20
					end

				if @BackupFile<>@TargetFile
					begin
						print convert(varchar, getdate(), 120) + ' Moving backup file "' + @BackupFile + '" to the target "' + @TargetFile + '"...'
						execute file_ops.mv @BackupFile, @TargetFile
						print convert(varchar, getdate(), 120) + ' File movement completed.'
					end

				declare @DelDate datetime = dateadd(day, -@BackupRetentionDays, convert(date, getdate()))

				declare @BackupFileList table ([Name] varchar(255))

				set nocount on
				insert into @BackupFileList select lower([Name]) from file_ops.ls(@TargetDir, '*.*')
				set nocount off

				declare
					@TXLogsDetected bit,
					@DiffBackupsDetected bit

				set @TXLogsDetected = case
					when exists (
						select 1 from @BackupFileList
						where
							[Name] like '%[_]tran.trn' and
							[Name] >= lower(@DBName) + '_' + convert(varchar, @DelDate, 112) + '_000000_tran.trn'
					) then convert(bit, -1)
					else convert(bit, 0) end

				set @DiffBackupsDetected = case
					when exists (
						select 1 from @BackupFileList
						where
							[Name] like '%[_]diff.bak' and
							[Name] >= lower(@DBName) + '_' + convert(varchar, @DelDate, 112) + '_000000_diff.bak'
					) then convert(bit, -1)
					else convert(bit, 0) end

				if @TXLogsDetected=convert(bit, -1) or @DiffBackupsDetected=convert(bit, -1)
					begin
						print 'Relevant transaction log and/or differential backup files were detected in the target directory.'
						print 'Trying to find the last full backup file before @DelDate (' + convert(varchar, @DelDate, 112) + ')...'

						declare @LastFullBackupFileName varchar(255)
						
						select top 1 @LastFullBackupFileName = [Name]
						from @BackupFileList
						where
							[Name] like '%[_]full.bak' and
							[Name] < lower(@DBName) + '_' + convert(varchar, @DelDate, 112) + '_000000_full.bak'
						order by [Name] desc
						
						print '...result: ' + isnull(@LastFullBackupFileName, 'NULL')
						
						if @LastFullBackupFileName is null
							begin
								print 'No full backup files older than ' + convert(varchar, @DelDate, 112) + ' were found.'

								-- determine if database was already backed up with "FULL" on this server before @DelDate
								if not exists (
									select 1 from msdb..backupset
									where
										[type] = 'D' and database_name = @DBName and
										backup_finish_date < @DelDate)
									print 'No full backups were recorded on this server before @DelDate. It''s OK.'
								else
									begin
										print 'Full backups were recorded on this server before @DelDate. There may be some error!'

										print 'Checking if there are any files on the day before @DelDate...'
										if not exists
											(
												select 1
												from @BackupFileList
												where
													[Name] like lower(@DBName) + '[_]' + convert(varchar, dateadd(day, -1, @DelDate), 112) + '[_]______[_]____.%'
											)
											print 'No files at all were found on the day before @DelDate. It''s OK.'
										else
											begin
												print 'Some files found - yes, this is an error.'

												set @MailSubj = 'Backup: Broken Backup Sequence: ' + @DBName
												set @MailBody =
													'Inconsistency in the backup directory encountered:' + char(13) +
													char(13) +
													'Transaction log and/or differential backups were detected on the disk, and @DelDate=' + convert(varchar, @DelDate, 112) + ', ' +
													'but no full backups were found before this date.' + char(13) +
													char(13) +
													'In this situation, it is not possible to restore the database using only existing files.'
								
												execute msdb.dbo.sp_notify_operator @profile_name=@MailProfile, @name=@NotifyOperator, @subject=@MailSubj, @body=@MailBody
											end
									end
							end
						else
							begin
								print 'Full backup files older than ' + convert(varchar, @DelDate, 112) + ' were found!'
								print 'Cut-off Date/Time will be corrected to preserve the last full backup file (' + @LastFullBackupFileName + ').'
								
								declare
									@TSShift integer = 5,
									@DateStr char(8),
									@TimeStr char(6)

								set @DateStr = substring(@LastFullBackupFileName, len(@LastFullBackupFileName) - @TSShift - (4 + 6 + 1 + 8) + 1, 8)
								set @TimeStr = substring(@LastFullBackupFileName, len(@LastFullBackupFileName) - @TSShift - (4 + 6) + 1, 6)
								
								set @DelDate =
									dateadd(minute, -2,
										convert(datetime, @DateStr, 112) +
										convert(datetime,
											substring(@TimeStr, 1, 2) + ':' +
											substring(@TimeStr, 3, 2) + ':' +
											substring(@TimeStr, 5, 2),
											108)
									)
							end
					end
			
				print 'Files will be deleted in ' + @TargetDir + ' which are older than ' + convert(varchar, @DelDate, 120) + '...'
				
				declare @CurFileName nvarchar(max)
				declare FilesToDelete cursor local fast_forward for
					select [Name]
					from
						(
							select
								[Name],
								substring([Name], len([Name]) - 5 - (4 + 6 + 1 + 8) + 1, 15) as DateTimeStr
							from file_ops.ls(@TargetDir, '*.*')
						) Aux
					where
						DateTimeStr<(convert(varchar, @DelDate, 112) + '_' + replace(convert(varchar, @DelDate, 108), ':', ''))
					order by Name

				open FilesToDelete
				fetch next from FilesToDelete into @CurFileName

				while @@fetch_status=0
					begin
						declare @CurFileFullName nvarchar(max) = @TargetDir + '\' + @CurFileName
						print 'Removing: ' + @CurFileName
						execute file_ops.rm @CurFileFullName
						fetch next from FilesToDelete into @CurFileName
					end

				deallocate FilesToDelete
			end
		
	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch
		
end
go

execute base.usp_prepare_object_creation 'maint', 'int_log_shrink'
go

create procedure maint.int_log_shrink
	@DBName varchar(255),
	@MaxLogSize integer as
	
begin

	begin try
	
		declare	@Cmd nvarchar(max)
	
		create table #LogFiles (name varchar(255))
		
		set @Cmd =
			'use [' + @DBName + '] ' +
			'insert into #LogFiles select name from sys.database_files ' +
			'where type=1'
		set nocount on
		execute (@Cmd)
		set nocount off

		declare LogFiles cursor local for
			select name from #LogFiles order by 1
			
		declare @CurFileName varchar(255)
		
		open LogFiles
		fetch next from LogFiles into @CurFileName

		set @Cmd = 'use [' + @DBName + '] dbcc shrinkfile(@FileName, ' + convert(varchar, @MaxLogSize) + ')'
		
		while @@fetch_status=0
			begin
				print 'Command: ' + @Cmd + '; @FileName=' + @CurFileName
				execute sp_executesql @Cmd, N'@FileName varchar(255)', @CurFileName
				fetch next from LogFiles into @CurFileName
			end
			
		deallocate LogFiles
	
		drop table #LogFiles
			
	end try

	begin catch
		if object_id('tempdb..#LogFiles') is not null
			drop table #LogFiles

		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch
	
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_log_shrink'
go

create procedure maint.usp_log_shrink
	@DBNamePattern varchar(max),
	@MaxLogFileSize integer as
	
begin

	begin try

		if @MaxLogFileSize is null
			raiserror('The configuration value Maintenance.MaxLogSize was not found.', 16, 1)

		declare @Stmt nvarchar(max) = 'execute maint.int_log_shrink @DBName, ' + convert(varchar, @MaxLogFileSize)

		execute base.usp_for_each_db @DBNamePattern, @Stmt, @SkipReadOnly = 1
		
	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch
		
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_log_shrink_force_one_db'
go

create procedure maint.usp_log_shrink_force_one_db
	@DBName varchar(255),
	@MaxLogFileSize integer as
	
begin

	begin try

		declare @VLFNumber integer

		print 'Processing database: ' + @DBName

		execute @VLFNumber = Tools.usp_get_vlf_number @DBName
		print ''
		print 'Number of VLFs before: ' + convert(varchar, @VLFNumber)

		-- try to reduce log size
		-- this may bring nothing, if any VLF is active outside of the target log size
		print ''
		print 'Attempting to shrink transaction log files for the 1st time...'
		print ''
		execute maint.int_log_shrink @DBName, @MaxLogFileSize

		-- back up transaction log to ensure that possibly active VLF at the
		-- end of TX log is empty and does not block log shrinking
		print ''
		print 'Enforcing log backup...'
		print ''
		execute maint.usp_backup @DBName, 'T'

		-- try to reduce log size again
		print ''
		print 'Attempting to shrink transaction log files for the 2nd time...'
		print ''
		execute maint.int_log_shrink @DBName, @MaxLogFileSize

		execute @VLFNumber = Tools.usp_get_vlf_number @DBName
		print ''
		print 'Number of VLFs after: ' + convert(varchar, @VLFNumber)

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch
		
end
go

execute base.usp_update_module_info 'maint', 1, 7
go
