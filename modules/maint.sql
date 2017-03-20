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

execute base.usp_update_module_info 'maint', 1, 0
go
