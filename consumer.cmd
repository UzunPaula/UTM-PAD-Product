@echo off
cd /d "%~dp0"
set "CONSUMER_ID=%~1"
set "PRODUCER_ID=%~2"
set "CONSUMER_MODE=%~3"
if "%CONSUMER_ID%"=="" (
    echo Usage: consumer.cmd ^<consumerId^> [producerId] [fail^|drop-ack-once]
    exit /b 2
)
if "%PRODUCER_ID%"=="" set "PRODUCER_ID=producer-1"
dotnet run --project DistributedApp/DistributedApp.Consumer -- "%CONSUMER_ID%" "%PRODUCER_ID%" "%CONSUMER_MODE%"
