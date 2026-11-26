# Publish the documentation with GitHub Pages

This repository already uses **MkDocs** (`mkdocs.yml` and `docs/`). The website build keeps the original Workflow Core documentation and adds the Distributed Hub track, with Material navigation, search, Mermaid diagrams and code copy controls.

## Site layout

```text
mkdocs.yml
docs/
├── index.md                       # Choose embedded or distributed runtime
├── distributed/
│   ├── index.md
│   ├── learning-path.md
│   ├── tutorials/                 # Day 1, Day 2, benchmarking
│   ├── architecture/              # Matching, leasing, system boundaries
│   ├── operations/                # Deployment, tuning, publishing
│   └── reference/                 # Flags, troubleshooting, source archive
├── getting-started.md             # Existing original Workflow Core docs
└── ...                            # All existing embedded-runtime pages
scaling/docs/                      # Engineering evidence: NOT moved or deleted
.github/workflows/docs-pages.yml  # Build check + GitHub Pages deploy
```

The `scaling/docs/` files are retained in place to preserve their relative links and extensive dated evidence; the website's reference section links to them. Do not copy generated MkDocs output (`site/`) into Git.

## Build and preview locally

Requires Python 3.12+ and a shell; from repository root:

```bash
python -m venv .venv
# Activate .venv for your platform, then:
python -m pip install -r docs/requirements.txt
mkdocs serve
```

Open `http://127.0.0.1:8000/`. Build without starting a server:

```bash
python scripts/check_docs_site.py
mkdocs build --clean
```

Generated HTML goes to `site/` (ignored by Git).

## Enable deployment from GitHub Actions

1. Go to **Settings → Pages → Build and deployment → Source** and choose **GitHub Actions**. This requires repository administrative access.
2. Merge the docs PR into the publishing branch (`master` in `.github/workflows/docs-pages.yml`).
3. Push a documentation change to `master` or run **Actions → Documentation (GitHub Pages) → Run workflow** on `master`.
4. Verify the **build** and **deploy** jobs pass. The deploy job needs `pages: write` and `id-token: write` and targets the `github-pages` environment.
5. Visit `https://thanhhaictld.github.io/workflow-core/` after GitHub Pages has deployed. If you choose a custom domain, update `site_url` in `mkdocs.yml` and Pages settings together.

Pull requests build/validate the site **without deploying**, preventing unreviewed documentation from publishing. There is no `gh-pages` push token or separate publishing branch: Actions uploads the rendered artifact and GitHub Pages deploys it.

## Add a page

1. Create a Markdown file under `docs/distributed/` in the appropriate section.
2. Add a label/path under `nav:` in `mkdocs.yml`.
3. Link to the page with a **relative Markdown path**, not the public URL. Keep links to the repo-only engineering archive as full GitHub source URLs.
4. Run `python scripts/check_docs_site.py` and `mkdocs build --clean`.
5. Submit a PR.

!!! tip "A safe way to restructure more source docs later"
    The website is already navigable. Do not physically move `scaling/docs/current-runtime.md` or the historical `scaling/docs/investigations/` files without a full inbound-link migration: those paths are used by source docs, tests and PRs.
