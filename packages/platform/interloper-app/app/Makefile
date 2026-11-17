.PHONY: build

check:
	pnpm run lint
	pnpm exec nuxt typecheck

claude-commit:
	claude --model haiku -p "Create a git commit for all staged changes. Use a single-line commit message following the Conventional Commits format (e.g. feat:, fix:, chore:, refactor, etc...). Keep it compact. Do not add co-author information. Do not push." --dangerously-skip-permissions
