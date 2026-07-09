#!/usr/bin/env python3
"""Validate current-state documentation policy for SF Urban Health."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT_DOCS = {
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    "docs/README.md",
    "docs/ARCHITECTURE.md",
    "docs/DATA_MODEL.md",
    "docs/EDGE_CASES.md",
    "docs/DEVELOPMENT.md",
    "docs/DEPLOY.md",
    "reports/README.md",
}

ALLOWED_TEXT = {
    "airflow/packages.txt",
    "airflow/requirements.txt",
    "airflow/.astro/dag_integrity_exceptions.txt",
}

FORBIDDEN_TRACKED = (
    "ROADMAP.md",
    "TODO.md",
    "wiki/",
    "artifacts/",
    "learning/",
)

OBSOLETE_PATTERNS = {
    r"(?<![\w-])--since(?![\w-])": "obsolete extractor flag --since",
    r"(?<![\w-])--run-date(?![\w-])": "obsolete extractor flag --run-date",
    r"raw/(?:<dataset>|\{dataset\})/YYYY/MM/DD": "obsolete raw path layout",
    r"\bNdjsonS3Writer\b": "obsolete class name NdjsonS3Writer",
    r"\bRunResult\b": "obsolete result type RunResult",
    r"\bcandidate_watermark\b": "obsolete candidate_watermark name",
}

FORWARD_HEADINGS = re.compile(
    r"^#{1,6}\s+.*(?:roadmap|what'?s next|future|target[- ]state|"
    r"target architecture|migration plan|planned|proposed)",
    re.IGNORECASE | re.MULTILINE,
)

PRIVATE_LINK = re.compile(
    r"\]\([^)]*(?:ROADMAP\.md|TODO\.md|artifacts/|learning/|wiki/)[^)]*\)",
    re.IGNORECASE,
)

MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)


def repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    raise RuntimeError("could not locate repository root")


def git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def github_slug(value: str) -> str:
    value = re.sub(r"<[^>]+>", "", value).strip().lower()
    value = re.sub(r"[`*_~]", "", value)
    value = re.sub(r"[^\w\- ]", "", value)
    return re.sub(r"[\s-]+", "-", value).strip("-")


def anchors(path: Path) -> set[str]:
    return {github_slug(match.group(2)) for match in HEADING.finditer(path.read_text())}


def allowed_markdown(path: str) -> bool:
    return (
        path in ROOT_DOCS
        or (path.startswith("reports/pages/") and path.endswith(".md"))
        or (path.startswith("docs/decisions/") and path.endswith(".md"))
        or (path.startswith(".codex/skills/") and path.endswith("/SKILL.md"))
    )


def existing_markdown_docs(root: Path, tracked: set[str]) -> set[str]:
    docs = {path for path in tracked if path.endswith(".md")}
    docs.update(path for path in ROOT_DOCS if (root / path).exists())
    docs.update(
        str(path.relative_to(root)) for path in (root / "reports/pages").glob("*.md")
    )
    docs.update(
        str(path.relative_to(root)) for path in (root / "docs/decisions").glob("*.md")
    )
    docs.update(
        str(path.relative_to(root))
        for path in (root / ".codex/skills").glob("*/SKILL.md")
    )
    return {path for path in docs if (root / path).exists()}


def check_links(root: Path, source: Path, errors: list[str]) -> None:
    text = source.read_text()
    for raw_target in MARKDOWN_LINK.findall(text):
        target = raw_target.strip().split()[0].strip("<>")
        if target.startswith(("http://", "https://", "mailto:", "/")):
            continue
        if target.startswith("#"):
            linked_path = source
            anchor = target[1:]
        else:
            file_part, separator, anchor = target.partition("#")
            linked_path = (source.parent / unquote(file_part)).resolve()
            if not linked_path.exists():
                errors.append(
                    f"{source.relative_to(root)}: missing link target {file_part}"
                )
                continue
            if linked_path.is_dir() or not separator:
                continue
        if anchor and linked_path.suffix.lower() == ".md":
            normalized = github_slug(unquote(anchor))
            if normalized not in anchors(linked_path):
                errors.append(
                    f"{source.relative_to(root)}: missing anchor #{anchor} in "
                    f"{linked_path.relative_to(root)}"
                )


def documented_make_targets(text: str) -> set[str]:
    targets: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        for match in re.findall(r"`make\s+([A-Za-z0-9_-]+)`", line):
            targets.add(match)
        if stripped.startswith("make ") or stripped.startswith("$ make "):
            parts = stripped.removeprefix("$ ").split()
            if len(parts) >= 2:
                targets.add(parts[1])
    return targets


def check_skill(root: Path, errors: list[str]) -> None:
    skill_path = root / ".codex/skills/maintain-project-docs"
    validator = skill_path / "scripts/quick_validate.py"
    result = subprocess.run(
        [sys.executable, str(validator), str(skill_path)],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        details = (result.stdout + result.stderr).strip()
        errors.append(f"maintain-project-docs skill validation failed: {details}")


def main() -> int:
    root = repo_root()
    errors: list[str] = []
    tracked = {
        path for path in git(root, "ls-files").splitlines() if (root / path).exists()
    }

    gitignore = (root / ".gitignore").read_text().splitlines()
    for required in ("/ROADMAP.md", "/learning/"):
        if required not in gitignore:
            errors.append(f".gitignore: missing anchored rule {required}")

    for required in sorted(ROOT_DOCS):
        if not (root / required).exists():
            errors.append(f"missing required public doc: {required}")

    for path in sorted(tracked):
        if any(path == item or path.startswith(item) for item in FORBIDDEN_TRACKED):
            errors.append(f"forbidden tracked private/superseded path: {path}")
        if path.endswith(".md") and not allowed_markdown(path):
            errors.append(f"undocumented Markdown location: {path}")
        if path.endswith(".txt") and path not in ALLOWED_TEXT:
            errors.append(f"undocumented text-file location: {path}")

    agents = root / "AGENTS.md"
    claude = root / "CLAUDE.md"
    if agents.read_bytes() != claude.read_bytes():
        errors.append("CLAUDE.md must be an exact copy of canonical AGENTS.md")

    check_skill(root, errors)

    makefile = (root / "Makefile").read_text()
    make_targets = set(re.findall(r"^([A-Za-z0-9_-]+):", makefile, re.MULTILINE))

    checked_docs = sorted(
        path for path in existing_markdown_docs(root, tracked) if allowed_markdown(path)
    )
    for relative in checked_docs:
        path = root / relative
        text = path.read_text()
        check_links(root, path, errors)

        if relative in ROOT_DOCS:
            h1_count = len(re.findall(r"^#\s+", text, re.MULTILINE))
            if h1_count != 1:
                errors.append(f"{relative}: expected exactly one H1, found {h1_count}")

            if FORWARD_HEADINGS.search(text):
                errors.append(f"{relative}: forward-looking public heading")
            if PRIVATE_LINK.search(text):
                errors.append(f"{relative}: links to a private or superseded document")

            for pattern, message in OBSOLETE_PATTERNS.items():
                if re.search(pattern, text):
                    errors.append(f"{relative}: {message}")

        for target in documented_make_targets(text):
            if target not in make_targets:
                errors.append(
                    f"{relative}: documented make target does not exist: {target}"
                )

    if errors:
        print("documentation audit failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(f"documentation audit passed ({len(checked_docs)} Markdown files checked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
