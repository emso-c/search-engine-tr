import os
import schedule
import time

command_map = [
    {
        "command": "python indexer.py",
        "schedule": schedule.every(7).minutes
    },
    {
        "command": "python backlink_analyser.py",
        "schedule": schedule.every(10).minutes
    }
]

for command in command_map:
    os.system(command["command"])
    command["schedule"].do(lambda: os.system(command["command"]))
    print(f"Added {command['command']} to scheduler")
    
while True:
    schedule.run_pending()
    time.sleep(1)