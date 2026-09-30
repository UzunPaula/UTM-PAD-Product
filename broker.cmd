@echo off
cd /d "%~dp0"
dotnet run --project DistributedApp/DistributedApp.Broker
