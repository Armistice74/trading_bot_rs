@echo off
%~d0
cd %~dp0postgres\bin
if not exist pg_ctl.exe (
    echo pg_ctl.exe not found in %cd% - check extraction.
    pause
    exit
)
pg_ctl -D ..\data -l ..\logfile.log stop >nul 2>&1
del /Q ..\data\postmaster.pid >nul 2>&1

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

pg_ctl -D ..\data -l ..\logfile.log start
if %errorlevel% neq 0 (
    echo Postgres start failed - check %~dp0postgres\logfile.log
    pause
    exit
)

netsh advfirewall firewall add rule name="Portable Postgres" dir=in action=allow protocol=TCP localport=6969 >nul 2>&1
if %errorlevel% neq 0 echo Firewall rule add failed - run as admin

cd %~dp0trading_bot_rs
if not exist trading_bot_rs.exe (
    echo Bot exe not found - rebuild needed.
    pause
    exit
)
trading_bot_rs.exe