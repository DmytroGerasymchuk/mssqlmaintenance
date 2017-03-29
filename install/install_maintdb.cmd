@echo off

setlocal

set InstPath=%~d0%~p0
set ModulesPath=%~d0%~p0..\modules
set JobsPath=%~d0%~p0..\jobs

set DefaultTargetInstance=(local)
set MaintDBName=MaintDB
set MaintDBDiskRoot=D:\%MaintDBName%
set DefaultDBAOperatorMail=sqldba@sandbox.com

echo =============================================
echo Installation of MSSQL*Maintenance SQL Scripts
echo =============================================

echo.
set /p TargetInstance=Target instance [%DefaultTargetInstance%]:
if [%TargetInstance%]==[] set TargetInstance=%DefaultTargetInstance%

echo.
set /P DBAOperatorMail=DBA operator mail [%DefaultDBAOperatorMail%]:
if [%DBAOperatorMail%]==[] set DBAOperatorMail=%DefaultDBAOperatorMail%

echo.
echo Choose Authentication:
echo 1. Windows Integrated
echo 2. SQL
choice /C 12 /M "Choose desired option"

if %errorlevel%==1 (
	set AuthOption=-E
	goto :cai
)

set AuthOption=-U sa

:repeat_pw
set /p SAPW=SA Password:
if ["%SAPW%"]==[""] (
	echo Empty password is not allowed! Please try again.
	goto repeat_pw
)

powershell -Command "$newY = $Host.UI.RawUI.CursorPosition.Y - 1; $Host.UI.RawUI.CursorPosition = New-Object System.Management.Automation.Host.Coordinates 0, $newY;"
echo SA Password:********************

:cai

set InstSubDir=%TargetInstance:\=_%
set JobLogDir=%MaintDBDiskRoot%\%InstSubDir%\JobLogs
set WmiXmlDir=%MaintDBDiskRoot%\%InstSubDir%\WMI

echo.
echo ===================
echo Confirm and Install
echo ===================
echo.
echo From path............: %InstPath%
echo  - Modules...........: %ModulesPath%
echo.
echo To server............: %TargetInstance%
echo MaintDB Name.........: %MaintDBName%
echo Job Log Dir..........: %JobLogDir%
echo WMI XML Dir..........: %WmiXmlDir%
echo DBA Operator Mail....: %DBAOperatorMail%
echo Authentication option: %AuthOption%
echo.
choice /C YN /M "Proceed"

if %errorlevel% neq 1 goto end

if defined SAPW (
	set AuthOption=%AuthOption% -P %SAPW%
)

echo.
choice /C YN /M "Apply preparation script for some initial best practices"
if %errorlevel% neq 1 goto forw

echo.
echo Executing preparation script for some initial best practices...
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%InstPath%server_preparation.sql"
if %errorlevel% equ 0 goto forw

echo.
echo Execution failed!
choice /C CA /M "(C)ontinue or (A)bort"
if %errorlevel% neq 1 goto end

:forw
echo.
choice /C YN /M "Create maintenance database for you"
if %errorlevel% neq 1 goto nocrdb

echo.
echo Choose maintenance database settings:
echo 1 = default
echo 2 = user-defined
choice /C 12 /M "Your choice"

if %errorlevel% equ 2 goto udcrdb

sqlcmd -S %TargetInstance% %AuthOption% -b -i "%InstPath%create_database.sql"
if %errorlevel% neq 0 goto end
goto ecrdb

:udcrdb
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%InstPath%create_database_UserDefined.sql"
if %errorlevel% neq 0 goto end
goto ecrdb

:ecrdb
echo Database has been created.
pause

:nocrdb

echo.
echo ======================== Prerequisites ========================
echo ***************************************************************
echo Please copy the library "maintdb_file_ops.dll"
echo into the "C:\TEMP" folder on the target system.
echo After that, press any key to continue the installer execution.
echo ***************************************************************
pause
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%InstPath%prerequisites.sql"
if %errorlevel% neq 0 goto end

echo.
echo ======================== Operators and Alerts ========================
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%InstPath%operators.sql"
if %errorlevel% neq 0 goto end
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%InstPath%alerts.sql"
if %errorlevel% neq 0 goto end

echo.
echo ======================== Modules ========================
for /f "usebackq eol=# tokens=1,2 delims==" %%a in (`type "%ModulesPath%\config.ini"`) do call :process_one_module "%%a" "%%b"

echo.
echo ======================== Jobs ========================
for /f "usebackq eol=# tokens=1,2 delims==" %%a in (`type "%JobsPath%\config.ini"`) do call :process_one_job "%%a" "%%b"

echo.
echo ======================== Initial Actions ========================
sqlcmd -S %TargetInstance% %AuthOption% -b -Q "execute msdb.dbo.sp_start_job '$(MaintDBName): History Cleanup'"

echo.
echo %date% %time% All finished.

:end
pause
exit

:process_one_module
if [%~2] neq [1] goto :eof
echo %date% %time% BEGIN module installation: [%~1]
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%ModulesPath%\%~1.sql"
if %errorlevel% neq 0 goto end
echo %date% %time% END module installation: [%~1]
goto :eof

:process_one_job
if [%~2] neq [1] goto :eof
echo %date% %time% BEGIN job installation: [%~1]
sqlcmd -S %TargetInstance% %AuthOption% -b -i "%JobsPath%\%~1.sql"
if %errorlevel% neq 0 goto end
echo %date% %time% END job installation: [%~1]
goto :eof
