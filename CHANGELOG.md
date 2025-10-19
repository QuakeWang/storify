## release v0.1.2 2025-10-19

### 🚀 Features

- Implement tree command to display directory structure (#52)

- Add find command with --name/--regex/--type and behavior tests; docs updated (#51)

- Add azblob cloud object storage interface (#50)

- Add recursive search functionality to grep command (#49)

- Enhance configuration management with profile support and CLI prompts (#48)

- Implement profile store for enhanced storage configuration management (#46)

- Add grep command (#41)

- Add tail command (#40)

- Add head command (#35)

### 🚜 Refactor

- Enhance storage configuration handling and environment variable resolution (#45)

- Reorganize storage configuration and update environment variable handling (#44)

- Refactor tail to reuse multi-file helper (#42)

### ♻️ CI 

- Fix CI workflows (#39)

## release v0.1.1 2025-09-07

### 🚀 Features

- Implement mkdir command and operations tests (#26)

- Add stat command to display object metadata and integrate tests (#29)

- Add support for COS storage provider in configuration and documentation (#33)

- Add cat command (#30)

- Add mv command (#32)

### ♻️ CI 

- Add delete operations tests and integrate into behavior tests (#19)

- Add upload operations tests and integrate into behavior tests (#21)

- Add download operations tests and integrate into behavior tests (#25)

### 🐛 Bug Fixes

- Fix cp root directory error issue (#18)

- Fix download root directory error (#20)

- Enhance download operations to handle malformed keys and improve path normalization (#34)

- The endpoint use https is invalid (#31)

### 🚜 Refactor

- Update the integration tests  to behavior tests (#22)

- Rename project from Ossify to Storify and update related configurations (#27)

- update dependencies and version in Cargo files (#38)