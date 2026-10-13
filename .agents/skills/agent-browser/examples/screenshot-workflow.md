# Example: Screenshot Workflow

## Goal

Capture screenshots of a web application for documentation.

## When to Use

- Creating user guides
- Bug reporting with visual evidence
- Documenting UI changes

## Prompt

```
Use agent-browser to take screenshots of the dashboard at https://app.example.com:
1. The main dashboard view
2. The settings panel (click Settings first)
3. Save them as dashboard.png and settings.png
```

## Expected Behavior

The agent will:
1. Open the app and wait for load
2. Take first screenshot
3. Navigate to settings
4. Take second screenshot
5. Close browser

## Example Session

```bash
agent-browser open https://app.example.com/dashboard
agent-browser wait --load networkidle
agent-browser screenshot dashboard.png

agent-browser snapshot -i
# Finds: button "Settings" [ref=e5]
agent-browser click @e5
agent-browser wait --load networkidle
agent-browser screenshot settings.png

agent-browser close
```

## Screenshot Options

```bash
# Full page (scrolls to capture everything)
agent-browser screenshot --full full-page.png

# Specific element only
agent-browser screenshot @e1 element.png

# With specific viewport
agent-browser set viewport 1920 1080
agent-browser screenshot desktop.png

agent-browser set device "iPhone 14"
agent-browser screenshot mobile.png
```

## Common Pitfalls

- **Loading spinners:** Always `wait --load networkidle` before screenshot
- **Cookie banners:** Dismiss or accept them first
- **Auth required:** Use `state save/load` for authenticated pages
