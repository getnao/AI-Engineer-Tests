# Level 2: Large File Processing Agent

## Task Description
Your agent will receive the same type of user prompt as Level 1, but now must work with files containing millions of rows. The agent needs to process these large files efficiently, in reasonable time.

### Input
Your script will be called with two arguments:
```bash
python agent/main.py "user prompt describing the task" "/absolute/path/to/working/directory"
```

### User prompt
```
Please add $10 to all purchase amounts in 'data/data_1.csv' because there was an error in recording
```

## Files Available
- `src/**`

## Testing
You can test your script with:
```bash
uv run python tests/level_1/test.py
```

The script tests that the correct chagnes have been applied.