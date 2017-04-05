@echo off

setlocal

set /P RootPMPath="Root path for PerfMon traces (empty to exit):"
if [%RootPMPath%]==[] goto :end
set RootPMPath=%RootPMPath:\=\\%

echo.
set /P AWKSQLI="SQL Server Instance (empty for default instance):"
if [%AWKSQLI%]==[] (
	set AWKSQLI=SQLServer
) else (
	set AWKSQLI=MSSQL$%AWKSQLI%
)
echo SQL Server name prefix will be used for PerfMon: %AWKSQLI%

echo.
choice /M "Ready to install! Continue or abort" /C CA
if %errorlevel% neq 1 goto :end

echo.
echo Creating personalized XML template...
"%~dp0gawk.exe" -f personalize.awk -v sqlprefix=%AWKSQLI% -v rootpath="%RootPMPath%" "%~dp0PerfMonTemplate.xml" > "%~dp0perfmon-temp.xml"
if %errorlevel% neq 0 (
	echo Error encountered!
	goto :end
)

echo.
echo Creating root path for PerfMon traces, if not exists...
if not exist "%RootPMPath%\." mkdir "%RootPMPath%"
if %errorlevel% neq 0 (
	echo Error encountered!
	goto :end
)

echo.
echo Importing personalized template with logman...
logman import -name "MSSQL Maintenance" -xml "%~dp0perfmon-temp.xml"
if %errorlevel% neq 0 (
	echo Error encountered!
	goto :end
)

del "%~dp0perfmon-temp.xml"
if %errorlevel% neq 0 (
	echo Error encountered!
	goto :end
)

echo.
echo Success!

:end
echo.
pause