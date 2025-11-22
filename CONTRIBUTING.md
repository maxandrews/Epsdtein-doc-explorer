# Contributing to Epstein Document Network Explorer

Thank you for your interest in contributing! This document provides guidelines for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How to Contribute](#how-to-contribute)
- [Development Workflow](#development-workflow)
- [Code Style](#code-style)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)

---

## Code of Conduct

This project is dedicated to providing accurate, accessible information about public documents. Contributors are expected to:

- Be respectful and constructive in discussions
- Focus on factual accuracy
- Avoid speculation or unverified claims
- Respect privacy where appropriate
- Follow all applicable laws and platform guidelines

---

## Getting Started

### Prerequisites

- Node.js 18.x or higher
- npm 9.x or higher
- Git
- Basic understanding of TypeScript and React

### Local Setup

```bash
# Fork the repository on GitHub
# Clone your fork
git clone https://github.com/YOUR_USERNAME/Epstein-doc-explorer.git
cd Epstein-doc-explorer

# Add upstream remote
git remote add upstream https://github.com/jslabxyz/Epstein-doc-explorer.git

# Install dependencies
npm install
cd network-ui && npm install && cd ..

# Start development servers
npx tsx api_server.ts          # Terminal 1
cd network-ui && npm run dev   # Terminal 2
```

See [Setup Guide](./docs/SETUP.md) for detailed instructions.

---

## How to Contribute

### Types of Contributions

| Type | Description |
|------|-------------|
| **Bug Fixes** | Fix issues in existing functionality |
| **Features** | Add new features or enhancements |
| **Documentation** | Improve docs, README, comments |
| **Performance** | Optimize queries, rendering, etc. |
| **Testing** | Add or improve tests |
| **Data Quality** | Improve entity deduplication, tagging |

### Finding Issues

- Check [GitHub Issues](https://github.com/jslabxyz/Epstein-doc-explorer/issues) for open tasks
- Look for issues labeled `good first issue` or `help wanted`
- Review the [Roadmap](#roadmap) for planned features

### Reporting Bugs

1. Search existing issues to avoid duplicates
2. Create a new issue with:
   - Clear title describing the bug
   - Steps to reproduce
   - Expected vs actual behavior
   - Browser/environment details
   - Screenshots if applicable

### Suggesting Features

1. Check existing issues and discussions
2. Create a feature request issue with:
   - Clear description of the feature
   - Use case and motivation
   - Potential implementation approach
   - Any relevant mockups or examples

---

## Development Workflow

### Branch Strategy

```
main                 # Production-ready code
├── feature/xxx      # New features
├── fix/xxx          # Bug fixes
├── docs/xxx         # Documentation changes
└── refactor/xxx     # Code refactoring
```

### Creating a Branch

```bash
# Sync with upstream
git fetch upstream
git checkout main
git merge upstream/main

# Create feature branch
git checkout -b feature/your-feature-name
```

### Making Changes

1. Make focused, incremental changes
2. Test your changes locally
3. Update documentation if needed
4. Ensure code follows style guidelines

### Testing Changes

```bash
# Run the API server
npx tsx api_server.ts

# Run frontend dev server
cd network-ui && npm run dev

# Run linting
cd network-ui && npm run lint

# Build to check for errors
cd network-ui && npm run build
```

---

## Code Style

### TypeScript/JavaScript

- Use TypeScript for all new code
- Enable strict mode
- Use meaningful variable names
- Prefer `const` over `let`
- Use async/await over callbacks

```typescript
// Good
const fetchRelationships = async (limit: number): Promise<Relationship[]> => {
  const response = await fetch(`/api/relationships?limit=${limit}`);
  return response.json();
};

// Avoid
function fetchRelationships(limit, callback) {
  fetch('/api/relationships?limit=' + limit)
    .then(r => r.json())
    .then(callback);
}
```

### React Components

- Use functional components with hooks
- Keep components focused and small
- Extract reusable logic into custom hooks
- Use TypeScript interfaces for props

```typescript
// Good
interface ActorCardProps {
  name: string;
  connectionCount: number;
  onSelect: (name: string) => void;
}

const ActorCard: React.FC<ActorCardProps> = ({ name, connectionCount, onSelect }) => {
  return (
    <div onClick={() => onSelect(name)}>
      <h3>{name}</h3>
      <span>{connectionCount} connections</span>
    </div>
  );
};
```

### CSS/Styling

- Use TailwindCSS utility classes
- Keep custom CSS minimal
- Follow mobile-first approach
- Use semantic class names for custom CSS

### SQL Queries

- Use parameterized queries (prevent SQL injection)
- Add indexes for frequently queried columns
- Use EXPLAIN to analyze query performance
- Document complex queries

```typescript
// Good - parameterized
db.prepare('SELECT * FROM actors WHERE name = ?').get(actorName);

// Bad - string interpolation (SQL injection risk)
db.prepare(`SELECT * FROM actors WHERE name = '${actorName}'`).get();
```

---

## Commit Guidelines

### Commit Message Format

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no code change |
| `refactor` | Code restructuring |
| `perf` | Performance improvement |
| `test` | Adding tests |
| `chore` | Build, config, etc. |

### Examples

```
feat(api): add keyword search to relationships endpoint

fix(graph): prevent node overlap on initial render

docs(readme): add deployment instructions

refactor(sidebar): extract filter controls to separate component
```

### Guidelines

- Use present tense ("add" not "added")
- Use imperative mood ("move" not "moves")
- Keep first line under 72 characters
- Reference issues in footer (`Fixes #123`)

---

## Pull Request Process

### Before Submitting

- [ ] Code follows project style guidelines
- [ ] All existing tests pass
- [ ] New functionality has tests (if applicable)
- [ ] Documentation updated (if applicable)
- [ ] Commit messages follow guidelines
- [ ] Branch is up to date with main

### Creating a Pull Request

1. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Open a Pull Request on GitHub

3. Fill out the PR template:
   - Clear title describing the change
   - Description of what/why
   - Link to related issues
   - Screenshots for UI changes
   - Testing instructions

### PR Review Process

1. Maintainers will review your PR
2. Address any feedback or requested changes
3. Once approved, maintainers will merge
4. Your branch will be deleted after merge

### Review Checklist

Reviewers will check:
- [ ] Code quality and style
- [ ] Functionality works as described
- [ ] No security vulnerabilities
- [ ] Performance impact considered
- [ ] Documentation complete
- [ ] Tests adequate

---

## Project Structure Reference

```
Epstein-doc-explorer/
├── api_server.ts              # Express API - main entry point
├── document_analysis.db       # SQLite database
├── tag_clusters.json          # Semantic clusters
│
├── analysis_pipeline/         # Document processing
│   ├── analyze_documents.ts   # AI extraction
│   ├── cluster_tags.ts        # Tag clustering
│   └── dedupe_with_llm.ts     # Entity dedup
│
├── network-ui/                # React frontend
│   ├── src/components/        # React components
│   ├── src/api.ts             # API client
│   └── src/types.ts           # TypeScript types
│
└── docs/                      # Documentation
```

---

## Roadmap

Planned improvements (contributions welcome):

### High Priority
- [ ] Add automated testing (Jest/Vitest)
- [ ] Implement CI/CD pipeline
- [ ] Add search within document text

### Medium Priority
- [ ] Community edits system (see COMMUNITY_EDITS_DESIGN.md)
- [ ] Export relationships to CSV/JSON
- [ ] Advanced filtering (date ranges, locations)

### Future
- [ ] Timeline visualization view
- [ ] GraphQL API alternative
- [ ] Mobile app (React Native)

---

## Getting Help

- **Questions:** Open a GitHub Discussion
- **Bugs:** Open a GitHub Issue
- **Security:** Email maintainers directly (do not open public issue)

---

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

Thank you for contributing!
