@echo off
setlocal enabledelayedexpansion

:: Set paths relative to bat location (in \trading_bot_rs\)
set "PG_DIR=%~dp0..\postgre"
set "PG_BIN=%PG_DIR%\bin"
set "PG_DATA=%PG_DIR%\data"
set "PG_LOG=%PG_DIR%\logfile.log"
set "VC_REDIST=%~dp0vc_redist.x64.exe"

:: Hardcoded details from config.toml
set "SUPERUSER=postgres"
set "SUPER_PASS=12345"
set "BOT_USER=trading_bot"
set "BOT_PASS=12345"
set "BOT_DB=trading_bot_db"
set "PG_PORT=6969"

:: Check VC++ Redist
reg query "HKLM\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\x64" /v Installed >nul 2>&1
if %errorlevel% neq 0 (
    echo VC++ Redist not found - installing...
    "%VC_REDIST%" /q /norestart
    if %errorlevel% neq 0 (
        echo Install failed - check manually and run as admin.
        pause
        exit /b 1
    )
    echo VC++ Redist installed.
) else (
    echo VC++ Redist already installed.
)

:: Check if data dir initialized
if not exist "%PG_DATA%\PG_VERSION" (
    echo Initializing PostgreSQL data cluster...
    "%PG_BIN%\initdb.exe" -D "%PG_DATA%" -U %SUPERUSER% --pwfile=<(echo %SUPER_PASS%)
    if %errorlevel% neq 0 (
        echo initdb failed - check paths and permissions.
        pause
        exit /b 1
    )
    echo Data cluster initialized.
)

:: Edit postgresql.conf for port and listen
set "CONF_FILE=%PG_DATA%\postgresql.conf"
echo port = %PG_PORT% >> "%CONF_FILE%"
echo listen_addresses = '*' >> "%CONF_FILE%"
echo Config files updated.

:: Edit pg_hba.conf for md5 auth (add if needed)
set "HBA_FILE=%PG_DATA%\pg_hba.conf"
findstr /C:"host    all             all             127.0.0.1/32            md5" "%HBA_FILE%" >nul 2>&1
if %errorlevel% neq 0 (
    echo host    all             all             127.0.0.1/32            md5 >> "%HBA_FILE%"
    echo host    all             all             ::1/128                 md5 >> "%HBA_FILE%"
    echo pg_hba.conf updated for md5 auth.
)

:: Clean any stale PID
del /Q "%PG_DATA%\postmaster.pid" >nul 2>&1

:: Start server temporarily
echo Starting PostgreSQL temporarily...
"%PG_BIN%\pg_ctl.exe" -D "%PG_DATA%" -l "%PG_LOG%" start
if %errorlevel% neq 0 (
    echo Start failed - check %PG_LOG%.
    pause
    exit /b 1
)
timeout /t 5 /nobreak >nul

:: Set PGPASSWORD for non-interactive psql
set "PGPASSWORD=%SUPER_PASS%"

:: Check if user exists
"%PG_BIN%\psql.exe" -U %SUPERUSER% -p %PG_PORT% -d postgres -t -c "SELECT 1 FROM pg_roles WHERE rolname='%BOT_USER%'" | findstr "1" >nul 2>&1
if %errorlevel% neq 0 (
    echo Creating user %BOT_USER%...
    "%PG_BIN%\psql.exe" -U %SUPERUSER% -p %PG_PORT% -d postgres -c "CREATE USER %BOT_USER% WITH PASSWORD '%BOT_PASS%';"
)

:: Check if DB exists
"%PG_BIN%\psql.exe" -U %SUPERUSER% -p %PG_PORT% -d postgres -t -c "SELECT 1 FROM pg_database WHERE datname='%BOT_DB%'" | findstr "1" >nul 2>&1
if %errorlevel% neq 0 (
    echo Creating database %BOT_DB%...
    "%PG_BIN%\psql.exe" -U %SUPERUSER% -p %PG_PORT% -d postgres -c "CREATE DATABASE %BOT_DB% OWNER %BOT_USER%;"
    "%PG_BIN%\psql.exe" -U %SUPERUSER% -p %PG_PORT% -d postgres -c "GRANT ALL PRIVILEGES ON DATABASE %BOT_DB% TO %BOT_USER%;"
)

:: Connect to DB and grant schema privileges
"%PG_BIN%\psql.exe" -U %SUPERUSER% -p %PG_PORT% -d %BOT_DB% -c "GRANT ALL ON SCHEMA public TO %BOT_USER%;"

:: Unset PGPASSWORD
set "PGPASSWORD="

:: Stop server
echo Stopping PostgreSQL...
"%PG_BIN%\pg_ctl.exe" -D "%PG_DATA%" -l "%PG_LOG%" stop
if %errorlevel% neq 0 (
    echo Stop failed - check %PG_LOG%.
    pause
    exit /b 1
)

echo Initialization complete. Run start_bot.bat to start server and bot.
pause