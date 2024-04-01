@echo off
setlocal enabledelayedexpansion

rem Get the command to run from the first argument
rem set "command=%1"
rem if "%command%"=="" (
rem     echo No command provided
rem     exit /b 1
rem )

rem Set the number of shells to run (default is 4)
set "num_shells=%2"
if "%num_shells%"=="" set "num_shells=4"

rem Activate the Python environment
call .\.env\scripts\activate

rem Loop to run the command in multiple shells
for /l %%i in (1,1,%num_shells%) do (
    start cmd /c "python ip_search.py"
)

endlocal
