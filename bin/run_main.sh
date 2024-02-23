run_command='python main.py'

program_output=$($run_command)

parsed_output='Date: '$(date)'

>'$run_command'
'$program_output''

echo "$parsed_output" > logs\\output\\$(date +%s).log
echo $program_output
