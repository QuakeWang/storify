# Welcome 👋

Thanks a lot for considering contributing to storify. We believe people like you would make storify a great tool for unified object storage management. We intend to build a community where individuals can have open talks, show respect for one another, and speak with true ❤️. Meanwhile, we are to keep transparency and make your effort count here.

You can find our contributors at [contributors](https://github.com/QuakeWang/storify/graphs/contributors). When you dedicate to storify for a few months and keep bringing high-quality contributions (code, docs, advocate, etc.), you will be a candidate of a committer.

Please read the guidelines, and they can help you get started. Communicate respectfully with the developers maintaining and developing the project. In return, they should reciprocate that respect by addressing your issue, reviewing changes, as well as helping finalize and merge your pull requests.

Follow our [README](https://github.com/QuakeWang/storify#readme) to get the whole picture of the project.

## Your First Contribution

It can feel intimidating to contribute to a complex project, but it can also be exciting and fun. These general notes will help everyone participate in this communal activity.

- Small changes make huge differences. We will happily accept a PR making a single character change if it helps move forward. Don't wait to have everything working.
- Check the closed issues before opening your issue.
- Try to follow the existing style of the code.
- More importantly, when in doubt, ask away.

Pull requests are great, but we accept all kinds of other help if you like. Such as

- Improve the documentation. [Submit documentation](https://github.com/QuakeWang/storify/tree/main/docs) updates, enhancements, designs, or bug fixes, and fixing any spelling or grammar errors will be very much appreciated.
- Submitting bug reports. To report a bug or a security issue, you can [open a new GitHub issue](https://github.com/QuakeWang/storify/issues/new).

## License

storify uses the [Apache 2.0 license](https://github.com/QuakeWang/storify/blob/main/LICENSE) to strike a balance between open contributions and allowing you to use the software however you want.

## Getting Started

### Submitting Issues

- Check if an issue already exists. Before filing an issue report, see whether it's already covered. Use the search bar and check out existing issues.
- File an issue:
  - To report a bug, a security issue, or anything that you think is a problem and that isn't under the radar, go ahead and [open a new GitHub issue](https://github.com/QuakeWang/storify/issues/new).
  - In the given templates, look for the one that suits you.
- What happens after:
  - Once we spot a new issue, we identify and categorize it as soon as possible.
  - Usually, it gets assigned to other developers. Follow up and see what folks are talking about and how they take care of it.
  - Please be patient and offer as much information as you can to help reach a solution or a consensus. You are not alone and embrace team power.

### Before PR

- Make sure all files have proper license header.
- Make sure all your codes are formatted and follow the [coding style](https://github.com/rust-lang/rust/blob/master/src/doc/style/style-guide.md).
- Make sure all unit tests are passed using `cargo test` or `cargo nextest run`.
- Make sure all clippy warnings are fixed (you can check it locally by running `cargo clippy --all-targets -- -D warnings`).

#### `pre-commit` Hooks

You could setup the [`pre-commit`](https://pre-commit.com/#plugins) hooks to run these checks on every commit automatically.

1. Install `pre-commit`

        pip install pre-commit

    or

        brew install pre-commit

2. Install the `pre-commit` hooks

        $ pre-commit install
        pre-commit installed at .git/hooks/pre-commit

        $ pre-commit install --hook-type commit-msg
        pre-commit installed at .git/hooks/commit-msg

        $ pre-commit install --hook-type pre-push
        pre-commit installed at .git/hooks/pre-push

Now, `pre-commit` will run automatically on `git commit`.

### Title

The titles of pull requests should be prefixed with category names listed in [Conventional Commits specification](https://www.conventionalcommits.org/en/v1.0.0)
like `feat`/`fix`/`docs`, with a concise summary of code change following. AVOID using the last commit message as pull request title.

### Description

- Feel free to go brief if your pull request is small, like a typo fix.
- But if it contains large code change, make sure to state the motivation/design details of this PR so that reviewers can understand what you're trying to do.
- If the PR contains any breaking change or API change, make sure that is clearly listed in your description.

### Commit Messages

All commit messages SHOULD adhere to the [Conventional Commits specification](https://conventionalcommits.org/).

## Development Setup

### Prerequisites

- Rust 1.80+ (nightly recommended for latest features)
- Cargo
- Git

### Local Development

1. Clone the repository:
   ```bash
   git clone https://github.com/QuakeWang/storify.git
   cd storify
   ```

2. Install dependencies:
   ```bash
   cargo build
   ```

3. Run tests:
   ```bash
   cargo test
   ```

4. Format code:
   ```bash
   cargo fmt --all
   ```

5. Check code quality:
   ```bash
   cargo clippy --all-targets -- -D warnings
   ```

### Testing

- Unit tests: `cargo test`
- Behavior tests: `cargo test --test behavior`

### Storage Provider Testing

To test with different storage providers, set up the corresponding environment variables:

```bash
# OSS
export STORAGE_PROVIDER=oss
export STORAGE_BUCKET=your-bucket
export STORAGE_ACCESS_KEY_ID=your-key
export STORAGE_ACCESS_KEY_SECRET=your-secret

# S3
export STORAGE_PROVIDER=s3
export STORAGE_BUCKET=your-bucket
export STORAGE_ACCESS_KEY_ID=your-key
export STORAGE_ACCESS_KEY_SECRET=your-secret

# MinIO (for local testing)
export STORAGE_PROVIDER=minio
export STORAGE_BUCKET=test
export STORAGE_ENDPOINT=http://127.0.0.1:9000
export STORAGE_ACCESS_KEY_ID=minioadmin
export STORAGE_ACCESS_KEY_SECRET=minioadmin
export STORAGE_REGION=us-east-1

# Local filesystem (for testing)
export STORAGE_PROVIDER=fs
export STORAGE_ROOT_PATH=./test-storage
```

## Project Structure

```
storify/
├── src/
│   ├── cli.rs          # Command-line interface
│   ├── config.rs       # Configuration management
│   ├── error.rs        # Error handling
│   ├── storage/        # Storage operations
│   │   ├── operations/ # Storage operation traits and implementations
│   │   └── utils/      # Storage utilities
│   └── utils.rs        # General utilities
├── tests/
│   ├── behavior/       # Behavior tests
│   └── data/           # Test data files
├── .github/
│   ├── workflows/      # CI/CD workflows
│   └── services/       # GitHub Actions services
└── docs/               # Documentation (future)
```

## Contributing Guidelines

### Code Style

- Follow Rust coding conventions
- Use meaningful variable and function names
- Add comprehensive documentation for public APIs
- Include unit tests for new functionality
- Use `snafu` for error handling with `wrap_err!` macro

### Error Handling

- Use the `snafu` crate for error definitions
- Wrap underlying errors using the `wrap_err!` macro
- Provide clear, actionable error messages
- Avoid exposing sensitive information in error messages

### Testing

- Add behavior tests for CLI commands
- Test with multiple storage providers when applicable
- Ensure backward compatibility
- Add unit tests for new functionality

### Documentation

- Update README.md for user-facing changes
- Add rustdoc comments for public APIs
- Update .env.example for new environment variables
- Document breaking changes clearly

### Performance

- Consider performance implications of changes
- Use async operations where appropriate
- Avoid blocking operations in async contexts
- Profile code for performance bottlenecks

### Security

- Never log sensitive information (keys, tokens, etc.)
- Validate all user inputs
- Use secure defaults
- Follow security best practices

## Communication

- GitHub Issues: For bug reports and feature requests
- GitHub Discussions: For general questions and discussions
- Pull Requests: For code contributions

## Development Workflow

1. **Fork the repository** on GitHub
2. **Create a feature branch** from `main`
3. **Make your changes** following the coding guidelines
4. **Add tests** for new functionality
5. **Run the test suite** to ensure everything works
6. **Submit a pull request** with a clear description

## Review Process

- All PRs require at least one review
- CI checks must pass before merging
- Code coverage should not decrease significantly
- Breaking changes require additional discussion

Thank you for contributing to storify! 🚀
