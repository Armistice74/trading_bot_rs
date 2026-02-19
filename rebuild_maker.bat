@echo off
cd /d C:\Users\Edward\Desktop\crypto_bots\bots\trading_bot_rs
cargo clean
cargo build --release
move /Y target\release\trading_bot_rs.exe .
if %errorlevel% equ 0 (
    echo Build and move successful.
) else (
    echo Error during process - check console.
)
pause