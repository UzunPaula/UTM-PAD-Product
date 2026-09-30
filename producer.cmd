@echo off
cd /d "%~dp0"
set "PRODUCER_ID=%~1"
set "PRODUCER_PORT=%~2"
if "%PRODUCER_ID%"=="" set "PRODUCER_ID=producer-1"
if "%PRODUCER_PORT%"=="" set "PRODUCER_PORT=5080"
set "Producer__Id=%PRODUCER_ID%"
dotnet run --project DistributedApp/DistributedApp.Producer --urls http://127.0.0.1:%PRODUCER_PORT%
