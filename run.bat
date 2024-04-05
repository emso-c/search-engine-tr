@echo off
setlocal enabledelayedexpansion

rem Activate the Python environment
call .\.env\scripts\activate

start cmd /c "title Crawler - IP Search & python ip_search.py"
start cmd /c "title Crawler - Page Search & python page_search.py" 
start cmd /c "title Crawler - URL Frontier & python url_frontier_search.py"

endlocal
