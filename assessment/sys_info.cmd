@echo off

setlocal

set TempFile="%~dp0sys_info_t.txt"
set FinalRepFile="%~dp0sys_info_%COMPUTERNAME%.txt"

if exist %FinalRepFile% del %FinalRepFile%

echo %COMPUTERNAME% >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic OS get Caption,OtherTypeDescription,CSDVersion,OSArchitecture > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

echo Windows activation information - if "No Instance(s) Available", then no product keys found at all: >> %FinalRepFile%
wmic path SoftwareLicensingProduct where (ProductKeyID != null) get Name,Description,LicenseStatus,LicenseStatusReason,GracePeriodRemaining > %TempFile% 2>&1
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic ComputerSystem get Model,Manufacturer,TotalPhysicalMemory > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic CPU get NumberOfCores,NumberOfLogicalProcessors > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic DiskDrive get Index,Size,Caption > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic Volume get Name,Label,Capacity,FreeSpace,FileSystem,BlockSize > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

echo Pagefile settings - if "No Instance(s) Available", then automatic pagefile management: >> %FinalRepFile%
wmic pagefileset > %TempFile% 2>&1
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

set >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic Service get Name,StartMode,State > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

wmic Process get Caption,PeakWorkingSetSize,PeakPageFileUsage > %TempFile%
type %TempFile% >> %FinalRepFile%
echo. >> %FinalRepFile%

del %TempFile%

pause
