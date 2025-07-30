# Level 1: Basic File Modification Agent

## Task Description
Your AI agent will receive a user prompt describing what changes need to be made to a file in the codebase. The agent should analyze the request and make the appropriate modifications that will change the filesystem.

### Input
Your script will be called with two arguments:
```bash
python agent/main.py "user prompt describing the task" "/absolute/path/to/working/directory"
```

### User prompt
```
Please add $10 to all purchase amounts in 'data/data_1.csv' because there was an error in recording
```

## Files available
- `src/**`

## Testing
You can test your script with:
```bash
uv run python tests/level_1/test.py
```

The script tests that the correct chagnes have been applied.