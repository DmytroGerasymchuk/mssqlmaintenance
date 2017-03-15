USE [msdb]
GO

if serverproperty('EngineEdition')=4
	print 'This is an Express edition. Nothign should be done.'
else
	begin

		declare
			@OperatorName nvarchar(255) = N'$(MaintDBName)-DBA',
			@OperatorMail nvarchar(255) = N'$(DBAOperatorMail)'

		if exists (select * from msdb.dbo.sysoperators where name=@OperatorName)
			begin
				print 'Operator "' + @OperatorName + '" already exists. Updating...'

				execute msdb.dbo.sp_update_operator
					@name=@OperatorName,
					@email_address=@OperatorMail
			end
		else
			begin
				print 'Creating operator "' + @OperatorName + '"...'

				execute msdb.dbo.sp_add_operator
					@name=@OperatorName, 
					@enabled=1, 
					@weekday_pager_start_time=90000, 
					@weekday_pager_end_time=180000, 
					@saturday_pager_start_time=90000, 
					@saturday_pager_end_time=180000, 
					@sunday_pager_start_time=90000, 
					@sunday_pager_end_time=180000, 
					@pager_days=0, 
					@email_address=@OperatorMail, 
					@category_name=N'[Uncategorized]'
			end

	end
GO
