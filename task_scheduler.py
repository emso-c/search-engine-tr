import os
import schedule
import time

command_map = [
    {
        "command": "python indexer.py",
        "schedule": schedule.every(10).minutes
    },
    {
        "command": "python backlink_analyser.py",
        "schedule": schedule.every(15).minutes
    }
]

for command in command_map:
    command["schedule"].do(lambda: os.system(command["command"]))
    print(f"Added {command['command']} to scheduler")
    
while True:
    schedule.run_pending()
    time.sleep(1)