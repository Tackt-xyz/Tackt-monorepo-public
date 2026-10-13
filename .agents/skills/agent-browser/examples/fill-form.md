# Example: Fill and Submit Form

## Goal

Fill out a web form with provided data and submit it.

## When to Use

- Creating test accounts
- Submitting feedback or support tickets
- Automating data entry

## Prompt

```
Use agent-browser to fill out the contact form at https://example.com/contact with:
- Name: John Doe
- Email: john@example.com
- Message: Testing the contact form
Then submit it and confirm success.
```

## Expected Behavior

The agent will:
1. Open the form page
2. Snapshot to find form fields
3. Fill each field using refs
4. Click submit button
5. Wait for response and verify success

## Example Session

```bash
agent-browser open https://example.com/contact
agent-browser snapshot -i
# Sees: textbox "Name" [ref=e1], textbox "Email" [ref=e2], 
#       textarea "Message" [ref=e3], button "Submit" [ref=e4]

agent-browser fill @e1 "John Doe"
agent-browser fill @e2 "john@example.com"
agent-browser fill @e3 "Testing the contact form"
agent-browser click @e4

agent-browser wait --text "Thank you"
agent-browser snapshot -i
# Confirms success message
agent-browser close
```

## Common Pitfalls

- **Hidden fields:** Some forms have hidden spam protection - use full snapshot without `-i` if needed
- **Multi-step forms:** Re-snapshot after each step
- **Dropdowns:** Use `agent-browser select @ref "value"` not fill
- **Checkboxes:** Use `agent-browser check @ref` not click
