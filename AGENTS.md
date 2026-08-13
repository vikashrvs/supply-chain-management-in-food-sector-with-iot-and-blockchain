# AI Agent Instructions

This project keeps a mandatory change memory at:

```text
MEMORY_OF_CHANGES.md
```

## Mandatory Rule

Every AI agent must update `MEMORY_OF_CHANGES.md` after making any project change.

This includes changes to:

- Code
- Database/schema
- Frontend UI
- Backend APIs
- MQTT/IoT logic
- Blockchain logic
- Scripts such as `start.bat` or `stop.bat`
- Documentation
- Folder structure
- Configuration

## Required Log Format

Append a new entry using this structure:

```text
### Example N
User: What the user asked for.

Agent: Agent name.

Work Done: Short summary of actual changes made.

Files Changed:
- `path/to/file`

Verification:
- What was tested or checked.

Notes:
- Anything the next AI should remember.
```

## Important

Do not skip this file. If no code changed, no new entry is required. If any file, folder, database, or script changed, `MEMORY_OF_CHANGES.md` must be updated before the final response.

