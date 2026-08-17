# Contributing

The most useful way to contribute right now is by opening issues, not pull requests. Bug reports and feature requests are read and prioritized; the project isn't currently taking external code contributions.

## Reporting a bug

Open an issue using the bug report template and include:

- A minimal reproduction (code snippet or steps)
- What you expected vs. what happened
- Your Go version, OS, and the relevant parts of your `hive.Config` (mode, replication factor, cluster size)

If you want to verify a bug locally before reporting it:

```bash
go build ./...
go vet ./...
go test ./...
go test -race ./...
```

## Requesting a feature

Open an issue using the feature request template. Describe the problem you're trying to solve, not just the API you imagine.
