# Example: Browse and Summarize

## Goal

Navigate to a webpage, extract its main content, and provide a summary.

## When to Use

- Researching documentation or articles
- Gathering information from multiple sources
- Quick content analysis

## Prompt

```
Use agent-browser to open https://docs.example.com/api/authentication, 
read the main content, and summarize the key authentication methods.
```

## Expected Behavior

The agent will:
1. Open the URL with `agent-browser open`
2. Take a snapshot with `agent-browser snapshot -i`
3. Read relevant text with `agent-browser get text @ref`
4. Close the browser with `agent-browser close`
5. Provide a summary

## Example Session

```bash
# Agent executes:
agent-browser open https://docs.example.com/api/authentication
agent-browser snapshot -i
# Sees: heading "Authentication" [ref=e1], section [ref=e2]...
agent-browser get text @e2
# Returns the content
agent-browser close
```

## Common Pitfalls

- **Dynamic content:** If content loads via JavaScript, add `agent-browser wait --load networkidle` after open
- **Cookie banners:** May need to dismiss them first with `agent-browser click @bannerCloseButton`
