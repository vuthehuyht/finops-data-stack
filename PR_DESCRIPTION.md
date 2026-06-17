## WHAT
- Migrated the codebase knowledge base (markdown documentation files) from `.ai/` directory to `.agents/` directory:
  - `.ai/architecture.md` ➔ `.agents/architecture.md`
  - `.ai/coding-rules.md` ➔ `.agents/coding-rules.md`
  - `.ai/project.md` ➔ `.agents/project.md`
  - `.ai/testing.md` ➔ `.agents/testing.md`
- Updated all references to `.ai/` with `.agents/` across:
  - Claude Code skill configurations (`.claude/skills/dagster-asset/SKILL.md`, `.claude/skills/dbt-model/SKILL.md`, `.claude/skills/gen-pr/SKILL.md`).
  - Claude Code custom agents configurations (`.claude/agents/dbt-quality-reviewer.md`, `.claude/agents/pipeline-security-reviewer.md`).
  - Gemini/Antigravity Workspace Skill configuration (`.agents/skills/gen-pr/SKILL.md`).
  - General project context documentation (`CLAUDE.md`, `GEMINI.md`).
- Removed the obsolete `.ai/` directory.

## WHY
- Centralize project knowledge base files under the `.agents/` workspace structure to align with Google Antigravity CLI conventions and standard agent-facing workspaces.

## QA
- All referenced files exist and can be loaded successfully by agents.
- Clean git status with no leftover `.ai/` directory files.

## REFERENCE
- Task requirement: "chuyển hết các knownlegde base từ .ai vào .agents"
