---
name: maintain-project-docs
description: Keep SF Urban Health public documentation synchronized with implemented repository behavior. Use after changes to commands, architecture, configuration, schemas, DAGs, storage, deployment, dashboards, reports, CI, or user-visible behavior; use for documentation audits and before implementation handoff when any documented contract may have changed.
---

# Maintain Project Docs

Keep tracked documentation factual, compact, and derived from the current code.
Keep forward-looking material private.

## Workflow

1. Inspect `git status`, the implementation diff, and relevant source/config
   files. Treat code and executable configuration as truth, not older prose.
2. Identify affected public surfaces:
   - commands, dependencies, environment: `README.md`, `AGENTS.md`,
     `docs/DEVELOPMENT.md`;
   - DAGs, storage, data flow: `docs/ARCHITECTURE.md`;
   - schemas, grains, metrics: `docs/DATA_MODEL.md`;
   - failure or retry behavior: `docs/EDGE_CASES.md`;
   - deployment, CI, consumers: `docs/DEPLOY.md` and component README files.
3. Update the smallest set that fully describes the implemented change. Remove
   claims that ceased to be true.
4. Keep `AGENTS.md` canonical and copy it exactly to `CLAUDE.md` after edits.
5. Update the ignored `ROADMAP.md` only when it exists and the implementation
   changes milestone status or dependencies. Never require it in a fresh clone.
6. Run `make docs-check`. Fix every failure before handoff.
7. Summarize changed documentation. If none changed, state why the diff did not
   alter a documented contract.

## Public-documentation rules

- Describe only behavior available in the checked-in code and configuration.
- Do not publish roadmaps, target-state diagrams, migration sequences, proposed
  designs, private learning notes, or unimplemented acceptance criteria.
- Do not copy content from ignored `ROADMAP.md` or `learning/` into tracked
  files.
- Use present tense for behavior. Avoid "will", "planned", "future", and
  "coming next" claims in public project docs.
- Add a design-decision document only after its behavior is implemented. Never
  edit history to make a superseded decision look current; add a factual
  superseding decision when needed.
- Preserve one H1 per public documentation file, CommonMark syntax, stable
  filenames, and relative repository links.
- Keep implementation essays and completed-work diaries out of public docs;
  commits and pull requests retain that history.

## Private surfaces

`ROADMAP.md` and `learning/` are intentionally gitignored. They may contain
forward-looking or educational material. Confirm they remain ignored and
untracked; never force-add them.

## Mechanical audit

Run the bundled checker directly when debugging:

```bash
python3 .codex/skills/maintain-project-docs/scripts/audit_docs.py
```

It checks tracked-file policy, ignore rules, agent-guide synchronization, local
links and anchors, Make targets, obsolete terminology, and future-state headings.
