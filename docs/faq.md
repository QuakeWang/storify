# FAQ (short)

- **Auth fails even with env vars?** Double-check `STORAGE_*` overrides provider-specific vars; unset stale provider-specific env to avoid conflicts. For profiles, re-run `storify config show --show-secrets` to confirm values.
- **Diff says size limit exceeded?** Use `storify diff --size-limit <MB> -f left right` to override the guard.
- **Recursive semantics?** `-R` applies to `ls`, `put`, `rm`, and `find`; tree uses `-d` to bound depth.
- **Windows path issues?** Prefer forward slashes in commands; if using PowerShell, quote globs (`'**/*.log'`).
