# Verify agent-browser Installation

Run these commands to confirm agent-browser is working:

## 1. Check version

```bash
agent-browser --version
```

✅ Should output version number

## 2. Test basic navigation

```bash
agent-browser open https://example.com
agent-browser get title
agent-browser close
```

✅ Should output "Example Domain"

## 3. Test snapshot

```bash
agent-browser open https://example.com
agent-browser snapshot -i
agent-browser close
```

✅ Should show interactive elements with refs like `@e1`, `@e2`

## 4. Test screenshot

```bash
agent-browser open https://example.com
agent-browser screenshot test.png
agent-browser close
ls test.png
rm test.png
```

✅ Should create and show test.png file
