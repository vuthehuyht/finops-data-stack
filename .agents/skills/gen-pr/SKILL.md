---
name: gen-pr
description: Generate a structured Pull Request (PR) description based on the active Git diff and the repository's PR template.
triggers:
  - "/gen-pr"
  - "generate pr description"
  - "tạo pr description"
  - "gen pr"
---

# Generate PR Description

Analyze the current git changes (staged, unstaged, or relative to the base branch) and generate a Pull Request description using the repository's PR template.

## Conventions

- The PR title and generated description should be in English.
- The PR description must strictly follow the format defined in `.github/pull_request_template.md`.
- Code comments and explanations in the code remain in Vietnamese (per project rules), but the PR description itself is targeted for GitHub reviewers (English).
- Automatically exclude large non-code files (like lockfiles: `uv.lock`, data files, compiled assets) when reviewing the diff to save token window.

## Steps

1. Find the base branch (usually `main`, fallback to `master` if not found).
2. Retrieve the git diff using:
   ```bash
   git diff <base_branch>... -- . ":(exclude)uv.lock" ":(exclude)*.lock" ":(exclude).agents/*" ":(exclude).github/*"
   ```
   *Note: If the user specifically asks for staged changes, use `git diff --staged`. If they ask for unstaged/HEAD changes, use `git diff HEAD`.*
3. Read the PR template file: `.github/pull_request_template.md`.
4. Analyze the diff contents:
   - Identify which files were changed (dbt models, Dagster assets, ML scripts, configuration).
   - Summarize the logic modifications and the reason (WHY).
   - Detect the "Type of Change" checkboxes that apply.
5. Generate the PR description filling in all sections of the template.
6. Write the generated description to a temporary file named `PR_DESCRIPTION.md` at the project root for the user to review and copy, then output the final markdown content directly in the chat.
