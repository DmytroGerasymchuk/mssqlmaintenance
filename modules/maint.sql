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
	execute base.usp_for_each_db @DBNamePattern, 'use [@DBName] execute sp_updatestats', 1, 1
end
go

execute base.usp_prepare_object_creation 'maint', 'usp_index_maint'
go

create procedure maint.usp_index_maint
	@DBName varchar(255),
	@MinFragPrctToReorg float = 10.0,
	@MinFragPrctToRebuild float = 30.0,
	@DisableRecoveryModelSwitching bit = 0,
	@Verbose bit = 0 as
		
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
			_index_name varchar(255)
		)

		set @Cmd = 'use [' + @DBName + ']
			select
				so.object_id as _object_id, ''[''+ ss.name + ''].['' + so.name + '']'' as _table_name,
				si.index_id as _index_id, ''[''+ si.name + '']'' as _index_name
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
			select _object_id, _table_name, _index_id, _index_name
			from @IndexList
			order by _table_name, _index_id

		declare
			@ObjectId integer,
			@TableName varchar(255),
			@IndexId integer,
			@IndexName varchar(255)

		open IndexList
		fetch next from IndexList into @ObjectId, @TableName, @IndexId, @IndexName

		while @@fetch_status=0
			begin

			declare OneIndex cursor local fast_forward for  
				select partition_number, avg_fragmentation_in_percent 
				from sys.dm_db_index_physical_stats(@DBID, @ObjectId, @IndexId, null, null)
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

				set @Msg = @Msg + @Action

				if @Verbose=convert(bit, -1) or @Action<>'NONE'
					print @Msg

				if @Cmd<>''
					execute (@Cmd)

				fetch next from OneIndex into @PartitionNumber, @FragPrct
			end
	
			deallocate OneIndex

			fetch next from IndexList into @ObjectId, @TableName, @IndexId, @IndexName
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

execute base.usp_update_module_info 'maint', 1, 1
go
