use [$(MaintDBName)]
go

execute base.usp_prepare_object_creation 'perf', 'usp_index_show_disabled'
go

create procedure perf.usp_index_show_disabled
	@DBName varchar(255) as
begin

	declare @Cmd nvarchar(max)

	set @Cmd =
		N'use [' + @DBName + ']
		select object_name(object_id) as TableName, name as IndexName, type_desc as IndexType
		from sys.indexes where is_disabled=1 order by 1, 2'

	execute (@Cmd)

end
go

execute base.usp_prepare_object_creation 'perf', 'usp_index_show_missing'
go

create procedure perf.usp_index_show_missing
	@DBNamePattern varchar(255) = '%',
	@TopRows integer = 10 as
begin

	select top (@TopRows)
		db_name(mid.database_id) as affected_db,

		migs.unique_compiles,
		migs.user_seeks,
		migs.user_scans,
		migs.last_user_seek,
		migs.last_user_scan,
		migs.avg_total_user_cost,
		migs.avg_user_impact,
		mid.statement as affected_table,
		mid.equality_columns,
		mid.inequality_columns,
		mid.included_columns,

		'create index [' + db_name() + 'PerfIndex' + convert(varchar, mid.index_handle) + '] on ' +
			right(mid.statement, len(mid.statement) - charindex('.', mid.statement)) + ' ' +
			'(' +
				isnull(mid.equality_columns, '') +
				case
					when mid.inequality_columns is not null then
						case when mid.equality_columns is null then ''
						else ', ' end + mid.inequality_columns
					else ''
				end +
			')' +
			case
				when mid.included_columns is not null then
					' include (' + mid.included_columns + ')'
				else ''
			end
			as proposed_statement
	from
		sys.dm_db_missing_index_group_stats migs
			inner join sys.dm_db_missing_index_groups mig on migs.group_handle=mig.index_group_handle
			inner join sys.dm_db_missing_index_details mid on mig.index_handle=mid.index_handle
	where
		db_name(mid.database_id) like @DBNamePattern
	order by
		migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) desc

end
go

execute base.usp_prepare_object_creation 'perf', 'usp_index_show_usage'
go

create procedure perf.usp_index_show_usage
	@DBName varchar(255),
	@SchemaName varchar(255),
	@TableName varchar(255) as
begin

	declare @Cmd nvarchar(max)

	set @Cmd =
		N'use [' + @DBName + ']

		select
			coalesce(si.name, si.type_desc) collate database_default as index_name,
			si.index_id,
			us.user_seeks, us.user_scans, us.user_lookups, us.user_updates,
			us.last_user_seek, us.last_user_scan, us.last_user_lookup, us.last_user_update
		from
			(select * from sys.dm_db_index_usage_stats where database_id=db_id()) us
				right join sys.indexes si on us.object_id=si.object_id and us.index_id=si.index_id
		where
			si.object_id=object_id(''[' + @SchemaName + '].[' + @TableName+ ']'') and
			si.is_hypothetical=0
		order by
			si.index_id'

	execute (@Cmd)

end
go

execute base.usp_prepare_object_creation 'perf', 'usp_mdw_active_user_requests'
go

create procedure perf.usp_mdw_active_user_requests
	@When datetime = null,
	@InstanceName varchar(255) = null,
	@MDWDBName varchar(255) = 'MDW' as
begin

	begin try

		set @InstanceName=upper(isnull(@InstanceName, @@servername))

		declare @Cmd nvarchar(max)

		create table #ThisInstanceSnapshots (snapshot_id integer)
		set @Cmd='select snapshot_id from [' + @MDWDBName + '].core.snapshots where upper(instance_name)=@InstanceName'
		insert into #ThisInstanceSnapshots execute sp_executesql @Cmd, N'@InstanceName varchar(255)', @InstanceName

		create table #ARSnapshots (snapshot_id integer, snap_time_stamp datetime)
		set @Cmd='select distinct snapshot_id, snap_time_stamp from [' + @MDWDBName + '].custom_snapshots.active_user_requests'
		insert into #ARSnapshots execute sp_executesql @Cmd, N'@InstanceName varchar(255)', @InstanceName

		declare @SnapshotId integer =
			(
				select top 1 ARS.snapshot_id -- letztmöglicher vor dem angegebenen Auswertungs-Zeitpunkt
				from
					#ARSnapshots ARS
						inner join #ThisInstanceSnapshots TIS on ARS.snapshot_id=TIS.snapshot_id
				where
					ARS.snap_time_stamp<=coalesce(@When, getdate())
				order by
					ARS.snap_time_stamp desc
			)

		set @Cmd = 'select * from [' + @MDWDBName + '].custom_snapshots.active_user_requests where snapshot_id=@SnapshotId'
		execute sp_executesql @Cmd, N'@SnapshotId integer', @SnapshotId

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_update_module_info 'perf', 1, 2
go
