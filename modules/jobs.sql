use [$(MaintDBName)]
go

if exists (select 1 from sys.schemas where name='jobs')
	begin
		print '"jobs" schema detected. This means, "jobs" module is already installed.'
		print 'Not everything from "jobs" module may be installed multiple times.'
		print 'Setting NOEXEC=ON...'
		set noexec on
	end
go

create schema jobs
go

create table jobs.tblSysjobNamePattern (
	strPattern varchar(255) not null,
	strDescription varchar(255) not null,

	constraint PK_jobs_tblSysjobnamePattern primary key (strPattern)
)
go

set nocount on
insert into jobs.tblSysjobNamePattern (strDescription, strPattern)
	values
		('MDW data collectors', 'collection_set_%_noncached_collect_and_upload'),
		('MDW data collectors', 'collection_set_%_collection'),
		('MDW data collectors', 'collection_set_%_upload'),

		('MDW maintenance', 'mdw_purge_data_[[]%]'),
		('MDW maintenance', 'sysutility_get_%'),

		('MSSQL*Maintenance', db_name() + ': %'),

		('SQL Server Built-In', 'Database Mirroring Monitor Job'),
		('SQL Server Built-In', 'syspolicy_purge_history'),
		('SQL Server Built-In', 'SSIS Server Maintenance Job')
set nocount off
go

set noexec off
print 'NOEXEC is now set to OFF...'
go

execute base.usp_prepare_object_creation 'jobs', 'udf_is_system_job'
go

create function jobs.udf_is_system_job(@JobName varchar(255))
returns bit as
begin

	if exists (select 1 from jobs.tblSysjobNamePattern snp where @JobName like snp.strPattern)
		return convert(bit, -1)

	return convert(bit, 0)

end
go

execute base.usp_prepare_object_creation 'jobs', 'udf_standard_log_name'
go

create function jobs.udf_standard_log_name(@JobName varchar(255), @StepName varchar(255))
returns varchar(255) as
begin

	return '$(UserJobLogDir)\[' + base.udf_to_safe_os_string(@JobName) + '].[' + base.udf_to_safe_os_string(@StepName) + '].txt'

end
go

execute base.usp_prepare_object_creation 'jobs', 'usp_check_log_names'
go

create procedure jobs.usp_check_log_names
	@AutoFix bit = 0,
	@Silent bit = 0 as
begin

	declare @Changes table (
		JobName varchar(255),
		StepId integer,
		StepName varchar(255),
		LogNameIS varchar(255),
		LogNameMUST varchar(255)
	)

	set nocount on
	insert into @Changes
	select *
	from
		(
			select
				SJ.[name],
				SJS.step_id,
				SJS.step_name,
				SJS.output_file_name as LogNameIS,
				jobs.udf_standard_log_name(SJ.[name], SJS.step_name) as LogNameMUST
			from
				msdb..sysjobs SJ
					inner join msdb..sysjobsteps SJS on SJ.job_id=SJS.job_id
			where
				jobs.udf_is_system_job(SJ.[name])=convert(bit, 0) and
				SJS.output_file_name is not null
		) Aux
	where
		Aux.LogNameIS<>Aux.LogNameMUST
	set nocount off

	if @Silent = convert(bit, 0)
		select * from @Changes order by 1, 2

	declare @NumChanges integer = isnull((select count(*) from @Changes), 0)

	if @AutoFix = 0
		return @NumChanges

	begin try

		begin transaction
		
			declare JobSteps cursor local fast_forward for
				select JobName, StepId, LogNameMUST from @Changes
				order by 1, 2

			declare
				@CurJobName varchar(255),
				@CurStepId integer,
				@LogNameMUST varchar(255),
				@RetRes integer

			open JobSteps
			fetch next from JobSteps into @CurJobName, @CurStepId, @LogNameMust

			while @@fetch_status=0
				begin
					execute @RetRes = msdb..sp_update_jobstep
						@job_name=@CurJobName,
						@step_id=@CurStepId,
						@output_file_name=@LogNameMUST

					if @RetRes<>0
						raiserror('sp_update_jobstep failed!', 16, 1)

					fetch next from JobSteps into @CurJobName, @CurStepId, @LogNameMust
				end

			deallocate JobSteps

		commit transaction

		return @NumChanges

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch
end
go

execute base.usp_prepare_object_creation 'jobs', 'usp_get_current_job_state'
go

create procedure jobs.usp_get_current_job_state
	@JobName varchar(255) as
begin

	declare @job_id uniqueidentifier = (select job_id from msdb.dbo.sysjobs where name=@JobName)
	if @job_id is null
		begin
			raiserror('Cannot get job_id by name.', 16, 1)
			return -1
		end

	create table #xp_results (
		job_id                uniqueidentifier not null,
		last_run_date         int              not null,
		last_run_time         int              not null,
		next_run_date         int              not null,
		next_run_time         int              not null,
		next_run_schedule_id  int              not null,
		requested_to_run      int              not null,
		request_source        int              not null,
		request_source_id     sysname          collate database_default null,
		running               int              not null,
		current_step          int              not null,
		current_retry_attempt int              not null,
		job_state             int              not null
	)

	declare @is_sysadmin int, @job_owner sysname

	set @is_sysadmin = isnull(is_srvrolemember('sysadmin'), 0)
	set @job_owner = suser_sname()

	set nocount on
	insert into #xp_results
		execute master.dbo.xp_sqlagent_enum_jobs @is_sysadmin, @job_owner, @job_id
	set nocount off

	if (select count(*) from #xp_results)<>1
		begin
			raiserror('Cannot get job state.', 16, 1)
			return -1
		end

	declare @running int, @current_step int
	select @running = running, @current_step = current_step from #xp_results

	drop table #xp_results

	return case @running when 0 then 0 else @current_step end

end
go

declare @TmpVer varchar(50) = convert(varchar, serverproperty('ProductVersion'))
if convert(integer, left(@TmpVer, charindex('.', @TmpVer) - 1)) < 11 -- 11 = SQL Server 2012
	begin
		print 'Some code can be executed only on SQL Server 2012 and higher!'
		print 'Setting NOEXEC=ON...'
		set noexec on
	end
go

execute base.usp_prepare_object_creation 'jobs', 'udf_get_step_id_by_name'
go

create function jobs.udf_get_step_id_by_name(@JobName varchar(255), @StepName varchar(255))
returns integer as
begin

	return
		(
			select SJS.step_id
			from msdb.dbo.sysjobs SJ inner join msdb.dbo.sysjobsteps SJS on SJ.job_id=SJS.job_id
			where SJ.name=@JobName and SJS.step_name=@StepName
		)

end
go

execute base.usp_prepare_object_creation 'jobs', 'usp_analyze_run_history'
go

create procedure jobs.usp_analyze_run_history
	@JobName sysname,
	@StepId integer = null,
	@StepName sysname = null,
	@MinimalDeviation float = 0.0,
	@IncludeFailed bit = 1,
	@DiagStepMinutes float = 10.0 as
begin

	if not ((@StepId is not null and @StepName is null) or (@StepId is null and @StepName is not null))
		begin
			raiserror('StepId or StepName must be not null. Not both null and not both not null.', 16, 1)
			return
		end

	if @StepId is null
		begin
			set @StepId = jobs.udf_get_step_id_by_name(@JobName, @StepName)
			if @StepId is null
				begin
					raiserror('StepId not found by StepName.', 16, 1)
					return
				end
		end

	if @StepId=0 -- job itself
		set @StepId=-1

	;with StepDuration as (
		select
			Aux.*,
			convert(numeric(10, 2),
				Aux.duration_minutes -
				avg(duration_minutes) over (partition by step_id order by instance_id rows between 60 preceding and current row)
			) as dev_minutes
		from
			(
				select
					SJH.instance_id,
					SJH.job_id,
					SJH.step_id,
					SJH.run_date,
					SJH.run_status,
					case SJH.run_status
						when 0 then 'Failed'
						when 1 then 'Succeeded'
						when 2 then 'Retry'
						when 3 then 'Canceled'
						else '???'
					end as run_descr,
					(
						(SJH.run_duration/10000)	* 3600 +
						(SJH.run_duration/100%100)	* 60 +
						(SJH.run_duration%100)		* 1
					) / 60.0 as duration_minutes,
					format(SJH.run_duration, '00:00:00') as duration_str,
					format(SJH.run_time, '00:00:00') as run_time_str
				from
					msdb.dbo.sysjobhistory SJH
						inner join msdb.dbo.sysjobs SJ on SJH.job_id=SJ.job_id
				where
					SJ.name=@JobName and
					SJH.step_id in (0, @StepId) and
					(@IncludeFailed=convert(bit, -1) or (@IncludeFailed=convert(bit, 0) and SJH.run_status=1))
			) Aux
	),
	JobDuration as (
		select row_number() over (order by instance_id) as execution_id, *
		from StepDuration
		where step_id=0
	)
	select
		JobD.run_date,
		JobD.run_time_str as run_time_job,
		JobD.run_descr as status_job,
		JobD.duration_str as duration_job,
		JobD.dev_minutes as dev_minutes_job,
		replicate('-', JobD.duration_minutes / @DiagStepMinutes) + '|' as diagram_job,
		StepD.run_time_str as run_time_step,
		StepD.run_descr as status_step,
		StepD.duration_str as duration_step,
		StepD.dev_minutes as dev_minutes_step,
		replicate('-', StepD.duration_minutes / @DiagStepMinutes) + '|' as diagram_step,
		convert(numeric(10, 2), (1 - (JobD.duration_minutes - StepD.duration_minutes) / nullif(JobD.duration_minutes, 0)) * 100) as step_of_job_prct
	from
		JobDuration JobD
			left join JobDuration PrevJobD on JobD.job_id=PrevJobD.job_id and PrevJobD.execution_id=(JobD.execution_id - 1)
			left join StepDuration StepD on JobD.job_id=StepD.job_id and StepD.step_id=@StepId and
				StepD.instance_id between isnull(PrevJobD.instance_id, 0) and JobD.instance_id
	where
		abs(JobD.dev_minutes) >= @MinimalDeviation or abs(StepD.dev_minutes) >= @MinimalDeviation
	order by
		JobD.instance_id desc, StepD.instance_id desc

end
go

set noexec off
print 'NOEXEC is now set to OFF...'
go

execute base.usp_prepare_object_creation 'jobs', 'usp_add_step'
go

create procedure jobs.usp_add_step
	@JobName varchar(255),
	@StepName varchar(255),
	@Command nvarchar(max),
	@Subsystem nvarchar(40) = 'TSQL',
	@DatabaseName sysname = null,
	@DatabaseUserName sysname = null,
	@ProxyName sysname = null,
	@ErrorIfStepAlreadyExists bit = 0,
	@Position integer = null as
begin

	begin try

		begin transaction

			-- Voraussetzungen prüfen

			declare @StepId integer = jobs.udf_get_step_id_by_name(@JobName, @StepName)

			if @StepId is not null
				begin
					print 'The job "' + @JobName + '" already has the step "' + @StepName + '".'
					raiserror('Job step already exists.', 16, 131)
				end

			declare
				@NumberSteps integer =
					(
						select count(*)
						from msdb.dbo.sysjobs SJ inner join msdb.dbo.sysjobsteps SJS on SJ.job_id=SJS.job_id
						where SJ.name=@JobName
					),
				@OutputFileName nvarchar(200) = jobs.udf_standard_log_name(@JobName, @StepName),
				@RetRes integer

			-- Neuen Job-Step hinzufügen

			-- INFO aus der Doku für On Success Action:
			-- 1 = quit with success
			-- 3 = go to the next step

			declare @OnSuccessAction integer

			if @Position is null or @NumberSteps = 0
				set @OnSuccessAction = 1
			else
				set @OnSuccessAction = 3

			execute @RetRes = msdb.dbo.sp_add_jobstep
				@job_name = @JobName,
				@step_id = @Position,
				@step_name = @StepName,
				@on_success_action = @OnSuccessAction,
				@subsystem = @Subsystem,
				@command = @Command,
				@database_name = @DatabaseName,
				@database_user_name = @DatabaseUserName,
				@output_file_name = @OutputFileName,
				@flags = 2,
				@proxy_name = @ProxyName

			if @RetRes<>0
				raiserror('sp_add_jobstep failed.', 16, 1)

			-- Wenn der Job-Step am Ende in einen nicht-leeren Job hinzugefügt wurde,
			-- so muss der vorangehende, vorher der letzte und nun vorletzt gewordene step,
			-- auf "On Success = Go to the next step" gesetzt werden, ansonsten
			-- wird ja unser neuer Step niemals ausgeführt!

			if @Position is null
				begin
					execute @RetRes = msdb.dbo.sp_update_jobstep
						@job_name = @JobName,
						@step_id = @NumberSteps,
						@on_success_action = 3

					if @RetRes<>0
						raiserror('sp_update_jobstep failed.', 16, 1)
				end

		commit transaction

		print 'Added new step "' + @StepName + '" to the job "' + @JobName + '".'

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		if not (error_state()=131 and @ErrorIfStepAlreadyExists=convert(bit, 0))
			begin
				print convert(varchar, getdate()) + ' Error encountered!'
				raiserror(@EM, 16, 1)
			end
	end catch

end
go

execute base.usp_prepare_object_creation 'jobs', 'usp_wait_for_completion'
go

create procedure jobs.usp_wait_for_completion
	@JobNameLikePattern varchar(255),
	@MaximumWaitTimeMinutes integer,
	@SelfId uniqueidentifier = null as
begin

	begin try

		declare JobNames cursor local fast_forward for
			select [name] from msdb.dbo.sysjobs
			where [name] like @JobNameLikePattern and ((@SelfId is null) or (@SelfId is not null and [job_id]<>@SelfId))
			order by 1

		declare
			@MaximumWaitTime integer = @MaximumWaitTimeMinutes * 60,
			@CurrentWaitTime integer = 0

		print convert(varchar, getdate()) + ' Staring wait loop...'

		while @CurrentWaitTime < @MaximumWaitTime
			begin

				declare
					@CurJobName varchar(255),
					@JobsAreRunning integer = 0

				open JobNames
				fetch next from JobNames into @CurJobName

				while @@fetch_status=0
					begin
						declare @Result integer
						execute @Result = jobs.usp_get_current_job_state @CurJobName
						if @Result <> 0
							begin
								--print convert(varchar, getdate()) + ' Job [' + @CurJobName + '] seems to be running...'
								set @JobsAreRunning = 1
								break
							end

						fetch next from JobNames into @CurJobName
					end

				close JobNames

				if @JobsAreRunning = 0
					begin
						print convert(varchar, getdate()) + ' Nothing is running.'
						return -- wenn nix läuft, dann sind wir mit dem Warten fertig
					end

				waitfor delay '00:00:10'
				set @CurrentWaitTime = @CurrentWaitTime + 10

			end

			-- zu lange gewartet - die Jobs laufen aber immer noch...
			raiserror('Maximum Wait Time was reached.', 16, 1)

	end try

	begin catch
		if @@trancount<>0 rollback transaction
		declare @EM varchar(max) = base.udf_errmsg()
		print convert(varchar, getdate()) + ' Error encountered!'
		raiserror(@EM, 16, 1)
	end catch

end
go

execute base.usp_update_module_info 'jobs', 1, 2
go
