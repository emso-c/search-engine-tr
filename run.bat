@echo off
setlocal enabledelayedexpansion

rem Activate the Python environment
call .\.env\scripts\activate

start cmd /c "python ip_search.py"
start cmd /c "python page_search.py"
start cmd /c "python url_frontier_search.py"

endlocal
