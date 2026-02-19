@echo off
%~d0
cd %~dp0

set "PG_DIR=%~d0\postgre"
set "PG_BIN=%PG_DIR%\bin"
set "PG_DATA=%PG_DIR%\data"
set "PG_LOG=%PG_DIR%\logfile.log"

if not exist "%PG_BIN%\pg_ctl.exe" (
    echo pg_ctl.exe not found in %PG_BIN% - check extraction.
    pause
    exit
)

pg_ctl -D "%PG_DATA%" -l "%PG_LOG%" stop >nul 2>&1
del /Q "%PG_DATA%\postmaster.pid" >nul 2>&1

reg query "HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64" /v Installed >nul 2>&1
if %errorlevel% neq 0 (
    echo VC++ Redist not found - installing...
    %~dp0vc_redist.x64.exe /q /norestart
    if %errorlevel% neq 0 (
        echo Install failed - check manually.
        pause
        exit
    )
)

"%PG_BIN%\pg_ctl.exe" -D "%PG_DATA%" -l "%PG_LOG%" start
if %errorlevel% neq 0 (
    echo Postgres start failed - check %PG_LOG%
    pause
    exit
)

netsh advfirewall firewall add rule name="Portable Postgres" dir=in action=allow protocol=TCP localport=6969 >nul 2>&1
if %errorlevel% neq 0 echo Firewall rule add failed - run as admin

if not exist trading_bot_rs.exe (
    echo Bot exe not found - rebuild needed.
    pause
    exit
)
trading_bot_rs.exe