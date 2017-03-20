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

execute base.usp_update_module_info 'perf', 1, 0
go
