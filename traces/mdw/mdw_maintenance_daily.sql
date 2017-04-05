-- ersetzen Sie ggf. auf den Namen Ihrer MDW-Datenbank
use MDW
go

-- Wartung für die Standard-Tabellen
-- (die sind immer da)
alter index all on [snapshots].[performance_counter_values] rebuild
alter index all on [snapshots].[os_wait_stats] rebuild
alter index all on [snapshots].[notable_query_plan] rebuild
go

-- Wartung für die MSSQL*Maintenance-Tabellen
-- (sind da, wenn zusätzliche Kollektoren installiert wurden)
alter table [custom_snapshots].[active_user_requests] rebuild
go