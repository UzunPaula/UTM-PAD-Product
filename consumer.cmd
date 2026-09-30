@echo off
cd /d "%~dp0"
set "CONSUMER_ID=%~1"
set "PRODUCER_ID=%~2"
if "%CONSUMER_ID%"=="" set "CONSUMER_ID=consumer-%RANDOM%"
if "%PRODUCER_ID%"=="" set "PRODUCER_ID=producer-1"
dotnet run --project DistributedApp/DistributedApp.Consumer -- %CONSUMER_ID% %PRODUCER_ID%
