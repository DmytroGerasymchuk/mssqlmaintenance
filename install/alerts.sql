create procedure #add_alert
	@NameSuffix varchar(255),
	@MessageId integer,
	@Severity integer as
begin

	declare
		@OperatorName nvarchar(255) = '$(MaintDBName)-DBA',
		@AlertName nvarchar(255) = '$(MaintDBName) ' + @NameSuffix

	if not exists (select * from msdb.dbo.sysalerts where name=@AlertName)
		begin
			print 'Creating alert "' + @AlertName + '"...'
			
			execute msdb.dbo.sp_add_alert
				@name=@AlertName, 
				@message_id=@MessageId, 
				@severity=@Severity, 
				@enabled=1, 
				@delay_between_responses=3600, 
				@include_event_description_in=1, 
				@category_name=N'[Uncategorized]'

			execute msdb.dbo.sp_add_notification
				@alert_name=@AlertName,
				@operator_name=@OperatorName,
				@notification_method=1 -- E-Mail
		end
	else
		print 'Alert "' + @AlertName + '" already exists.'

end
go

execute #add_alert 'Filegroup Overflow',		@MessageId=1105,	@Severity=0
execute #add_alert 'TX Log Full',				@MessageId=9002,	@Severity=0
execute #add_alert 'No Catalog Entry Found',	@MessageId=608,		@Severity=0
execute #add_alert 'Severity 21 Errors',		@MessageId=0,		@Severity=21
go

drop procedure #add_alert
go