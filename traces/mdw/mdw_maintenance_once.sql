-- ersetzen Sie ggf. auf den Namen Ihrer MDW-Datenbank
use MDW
go

create nonclustered index [MaintDBPerfIndex001] on [snapshots].[query_stats]
(
	[sql_handle] asc,
	[statement_start_offset] asc,
	[statement_end_offset] asc,
	[plan_generation_num] asc,
	[plan_handle] asc,
	[creation_time] asc
)
GO
