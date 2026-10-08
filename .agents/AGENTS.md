# AGENTS.md — STRICT AI AGENT INSTRUCTIONS

# NOTE - MEMORY_OF_CHANGES.md is a file and every log should be maintained in this file.

This file contains mandatory instructions for EVERY AI agent working on this project.

## 1. MANDATORY LOGGING — NO EXCEPTIONS

Every AI agent MUST update `MEMORY_OF_CHANGES.md` after EVERY project-related session.

This applies even if the agent:
- changes no files
- only inspects or analyzes the project
- plans work
- reviews code
- debugs
- tests
- performs security review
- fails to complete the task
- becomes blocked
- decides no change is necessary

There must NEVER be an undocumented AI-agent session.

If the agent cannot update `MEMORY_OF_CHANGES.md`, it MUST report that failure and must not claim the task is fully complete.

## 2. AGENT IDENTITY IS REQUIRED

Every entry MUST contain the actual agent name, for example:
- Codex
- Claude Code
- Antigravity
- Gemini
- Other AI Agent

Do not use only "AI Agent" when the actual name is known.

## 3. DATE AND TIME ARE REQUIRED

Every entry MUST contain the actual date and time of the work.

Format:
`YYYY-MM-DD HH:MM:SS TZ`

Example:
`2026-08-18 21:45:32 IST`

Never invent a timestamp.

## 4. LOG BEFORE FINAL RESPONSE

Before giving the final response, the agent MUST:
1. Review what it actually did.
2. Update `MEMORY_OF_CHANGES.md`.
3. Record files inspected.
4. Record files created/modified/deleted.
5. Record verification.
6. Record failures/blockers.
7. Confirm the log was recorded.

Do not claim completion if the mandatory log was not successfully updated.

## 5. NO-CHANGE SESSIONS MUST BE LOGGED

If nothing changed, still create an entry.

Example:

### Entry N
Date: 2026-08-18 22:00:00 IST
Agent: Codex
User Request: Review backend architecture
Session Type: Analysis

Work Done:
No project files changed. Reviewed the backend architecture.

Files Inspected:
- `backend/main.py`

Files Changed:
None

Verification:
Architecture reviewed.

Issues / Blockers:
None

Next Step:
Await further instructions.

Log Status:
`RECORDED`

## 6. FAILED OR BLOCKED WORK MUST BE LOGGED

Record:
- what was attempted
- what failed
- error/problem
- files affected
- whether the project was left safe
- recommended next step

Never hide failed work.

## 7. NEVER FABRICATE VERIFICATION

Only record tests, commands, builds, scans, or checks that were actually performed.

Use `NOT RUN`, `NOT VERIFIED`, or `BLOCKED` when appropriate.

## 8. REQUIRED WORK-LOG FIELDS

Every entry MUST contain:
- Entry number
- Date
- Time
- Time zone
- Agent name
- User request
- Session type
- Objective
- Work done
- Files inspected
- Files created
- Files modified
- Files deleted
- Verification
- Security
- Issues/blockers
- Decisions/assumptions
- Next step
- Log status

## 9. ENTRY NUMBERING

Entries must remain sequential:
Entry 1
Entry 2
Entry 3
...

Never overwrite or delete historical entries unless the user explicitly requests it.

## 10. ENGINEERING DISCIPLINE

Before significant changes:
1. Understand the existing implementation.
2. Identify affected components.
3. Identify risks.
4. Plan the change.
5. Make the minimum necessary change.
6. Verify the result.
7. Update documentation when needed.
8. Update `MEMORY_OF_CHANGES.md`.

Do not unnecessarily rewrite working code.

## 11. SECURITY

Treat AI-generated code as untrusted until reviewed.

Never expose or commit:
- passwords
- API keys
- tokens
- private keys
- credentials
- secrets

Security-sensitive work must include security notes in the work log.

## 12. FINAL MANDATORY CHECK

Before ending every project-related session, verify:
- Agent name recorded
- Date/time recorded
- User request recorded
- Work recorded
- Files recorded
- Verification recorded
- Failures/blockers recorded
- `MEMORY_OF_CHANGES.md` updated
- No unverified claim was made

`AGENTS.md` controls HOW agents work.
`MEMORY_OF_CHANGES.md` records WHAT agents did.
