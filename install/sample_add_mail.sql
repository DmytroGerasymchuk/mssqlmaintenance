execute sp_configure 'show advanced options', 1
reconfigure with override
execute sp_configure 'Database Mail XPs', 1
reconfigure with override

declare @EMailAddr varchar(255)

set @EMailAddr = 'MaintDB-' + replace(@@servername, '\', '-') + '@sandbox.com'

execute msdb.dbo.sysmail_add_account_sp
	@account_name='MaintDB-MailAccount',
	@email_address=@EMailAddr,
	@mailserver_name='localhost'
	
execute msdb.dbo.sysmail_add_profile_sp
	@profile_name='MaintDB-MailProfile'
	
execute msdb.dbo.sysmail_add_profileaccount_sp
	@profile_name='MaintDB-MailProfile',
	@account_name='MaintDB-MailAccount',
	@sequence_number=1
	
execute msdb.dbo.sp_send_dbmail
	@profile_name='MaintDB-MailProfile',
	@recipients='sqldba@sandbox.com',
	@subject='Test Mail'

EXEC msdb.dbo.sp_set_sqlagent_properties @email_save_in_sent_folder=1
EXEC master.dbo.xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', N'UseDatabaseMail', N'REG_DWORD', 1
EXEC master.dbo.xp_instance_regwrite N'HKEY_LOCAL_MACHINE', N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent', N'DatabaseMailProfile', N'REG_SZ', N'MaintDB-MailProfile'
