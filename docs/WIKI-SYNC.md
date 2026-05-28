# Syncing wiki source to GitHub Wiki

The [`wiki/`](../wiki/) folder is the **source of truth** for the
GitHub Wiki. It is tracked in the main repo so wiki content is
versioned alongside the code.

## What lives where

| Content | Location |
|---|---|
| Architecture, data model, edge cases, design rationale | `wiki/` → GitHub Wiki |
| Portfolio landing + quickstart | `README.md` |
| Live-dashboard hosting plan | [DEPLOY.md](DEPLOY.md) |
| High-level architecture diagram | [ARCHITECTURE.md](ARCHITECTURE.md) — pointer back to wiki |
| Dashboard local dev commands | `dashboard/README.md` |

## First-time setup (wiki does not exist yet)

GitHub creates the wiki git remote only after you enable the feature
and create the first page via the web UI.

1. Open **[Settings → General → Features](https://github.com/Ukiah-Heasley/sf-urban-health/settings)** → enable **Wikis**.
2. Go to the repo **Wiki** tab → **Create the first page** (any title/body — this initializes the remote).
3. Publish from `wiki/` (see below).

## Publish updates

From the repo root, after the wiki remote exists:

```bash
git clone https://github.com/Ukiah-Heasley/sf-urban-health.wiki.git /tmp/sf-urban-health.wiki
cp wiki/{Home,_Sidebar,Architecture,Data-Model,Edge-Cases,Dashboard,Design-Decisions,Developer-Setup}.md \
   /tmp/sf-urban-health.wiki/
cd /tmp/sf-urban-health.wiki
git add .
git commit -m "Sync wiki from main repo"
git push
```

Or copy into an existing wiki clone you already have checked out.

## Editing workflow

1. Edit files in `wiki/` on a branch in the main repo (reviewable, versioned).
2. Merge to `main`.
3. Push to the wiki remote (steps above).

Do **not** edit wiki pages only in the GitHub web UI — those changes
will be overwritten on the next sync from `wiki/`.
