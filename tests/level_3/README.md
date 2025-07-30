# Level 3: Find a needle in a haystack

## Task Description
Your AI agent will receive a user prompt describing a search query. The agent should analyze the request and make the appropriate modifications that will change the filesystem.

### Input
Your script will be called with two arguments:
```bash
python agent/main.py "user query" "/absolute/path/to/working/directory"
```

### User prompt
```
Can you find in which SQL query is defined the total revenue field?
```

## Files available
- `src/**`