# Usage

This page lists the common Storify CLI commands with short, copy-pastable examples.

## Listing and navigation
- List directory: `storify ls path/to/dir`
- Detailed list: `storify ls -L path/to/dir`
- Recursive list: `storify ls -R path/to/dir`
- Tree view: `storify tree path/to/dir` or limit depth `storify tree -d 1 path/to/dir`

## Transfer
- Download: `storify get remote/path local/path`
- Upload file: `storify put local/file remote/path`
- Upload directory recursively: `storify put -R local/dir remote/dir`
- Copy within storage: `storify cp source/path dest/path`
- Move/rename: `storify mv source/path dest/path`

## Create and delete
- Create directory: `storify mkdir path/to/dir`
- Create nested directories: `storify mkdir -p path/to/nested/dir`
- Touch file (create if missing): `storify touch path/to/file`
- Truncate file: `storify touch -t path/to/file`
- Delete file: `storify rm path/to/file`
- Delete recursively: `storify rm -R path/to/dir`
- Delete recursively without confirmation: `storify rm -Rf path/to/dir`

## View, search, and inspect
- Show file contents: `storify cat path/to/file`
- Head: `storify head path/to/file` (default 10 lines), or `storify head -n 20 path/to/file`
- Tail: `storify tail path/to/file` (default 10 lines), or `storify tail -n 20 path/to/file`
- Grep: `storify grep "pattern" path/to/file`, case-insensitive `-i`, show line numbers `-n`, recursive `-R`
- Find by glob: `storify find path/ --name '**/*.log'`
- Find by regex: `storify find path/ --regex '.*\\.(csv|parquet)$'`
- Filter by type: `storify find path/ --type f` (f=file, d=dir, o=other)
- Disk usage: `storify du path/to/dir` or summary only with `-s`
- Stat metadata: `storify stat path/to/file` (human), `--json`, or `--raw`

## Diff
- Unified diff (3 lines context default): `storify diff left/file right/file`
- Custom context: `storify diff -U 1 left/file right/file`
- Ignore trailing whitespace: `storify diff -w left/file right/file`
- Guard against large files and force: `storify diff --size-limit 1 -f left right`

## Append
- Append a local file: `storify append remote/path --src ./local.txt`
- Append via stdin: `echo "line" | storify append remote/path --stdin`
- Alias form (local first, remote second): `storify append ./local.txt remote/path`
- Require existing file: add `-c/--no-create`
- Auto-create parent directories (filesystem providers): add `-p/--parents`

## Options cheat sheet
- `-R`: recursive (works with `ls`, `put`, `rm`, `find`)
- `-L`: long/detailed listing
- `-d`: tree depth
- `-f`: force (skip confirmations where applicable)
- `--json` / `--raw`: structured output for `stat`
