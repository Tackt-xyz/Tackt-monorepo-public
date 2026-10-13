# Example: Login and Scrape Protected Content

## Goal

Authenticate to a website and extract data from protected pages.

## When to Use

- Scraping data behind login walls
- Automating authenticated workflows
- Testing auth flows

## Prompt

```
Use agent-browser to:
1. Login to https://app.example.com with username "testuser" and password "testpass"
2. Navigate to the reports page
3. Extract all report titles and dates into a list
```

## Expected Behavior

The agent will:
1. Open login page
2. Fill credentials and submit
3. Wait for dashboard
4. Navigate to reports
5. Extract data using get commands
6. Return structured data

## Example Session

```bash
# Login
agent-browser open https://app.example.com/login
agent-browser snapshot -i
# Sees: textbox "Username" [ref=e1], textbox "Password" [ref=e2], button "Sign In" [ref=e3]

agent-browser fill @e1 "testuser"
agent-browser fill @e2 "testpass"
agent-browser click @e3
agent-browser wait --url "**/dashboard"

# Save auth state for future use
agent-browser state save auth.json

# Navigate to reports
agent-browser open https://app.example.com/reports
agent-browser wait --load networkidle
agent-browser snapshot -i

# Extract data
agent-browser get text @e5   # First report title
agent-browser get text @e6   # First report date
# ... repeat for all items

agent-browser close
```

## Reusing Auth State

```bash
# Next time, skip login:
agent-browser state load auth.json
agent-browser open https://app.example.com/reports
# Already authenticated!
```

## Common Pitfalls

- **2FA:** May need manual intervention or app-specific tokens
- **Session expiry:** Re-authenticate if state is old
- **Rate limiting:** Add delays between requests if scraping many pages
- **Dynamic tokens:** Some sites require fresh login each session
