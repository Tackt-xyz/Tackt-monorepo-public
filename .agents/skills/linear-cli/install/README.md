# Installing linear-cli

## Prerequisites

- Linear API key (get from Linear Settings → API → Personal API keys)

## Quick Install

### macOS (Homebrew)

```bash
brew install schpet/tap/linear
```

### Any OS (Deno)

```bash
deno install -A --reload -f -g -n linear jsr:@schpet/linear-cli
```

### Manual (Prebuilt Binaries)

Download from [GitHub Releases](https://github.com/schpet/linear-cli/releases/latest)

## Configuration

1. **Set API key:**
   ```bash
   export LINEAR_API_KEY="lin_api_xxxxxxxxxxxxx"
   ```
   
   Add to your shell profile (`~/.bashrc`, `~/.zshrc`) for persistence.

2. **Run setup:**
   ```bash
   linear config
   ```
   
   This creates `.linear.toml` with your team and preferences.

## Verify Installation

```bash
linear --version
linear auth whoami
```

Expected: Shows your Linear user info and workspace.

## Troubleshooting

### "command not found: linear"

**Homebrew:** Run `brew link schpet/tap/linear`

**Deno:** Add Deno bin to PATH:
```bash
export PATH="$HOME/.deno/bin:$PATH"
```

### "LINEAR_API_KEY not set"

Set the environment variable:
```bash
export LINEAR_API_KEY="lin_api_xxxxxxxxxxxxx"
```

### "Team not found" errors

Run `linear config` to set your default team.

## Uninstall

### Homebrew
```bash
brew uninstall linear
```

### Deno
```bash
deno uninstall linear
```
