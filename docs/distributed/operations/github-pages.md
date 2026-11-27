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
python scripts/check_mermaid_output.py
```

Generated HTML goes to `site/` (ignored by Git).

## Why Mermaid currently appears as code on GitHub Pages

The repository's actual [Pages deployment from November 26, 2026](https://github.com/thanhhaictld/workflow-core/actions/runs/45849886423) ran **Jekyll** and generated `docs/_site/`. That is the default **Deploy from a branch** mode. It does **not** build `mkdocs.yml`, so Material's Mermaid JavaScript is never loaded even if GitHub's repository Markdown preview recognizes Mermaid code fences.

The correct publishing pipeline is the custom **Documentation (GitHub Pages)** GitHub Actions workflow. It installs `mkdocs-material`, runs `mkdocs build --clean`, verifies the generated Mermaid HTML, uploads `site/`, then deploys that static HTML.

**Change the repository's Pages Source, not the Mermaid syntax:**

1. Open **Settings → Pages → Build and deployment**.
2. Change **Source** from **Deploy from a branch** to **GitHub Actions**.
3. Merge the documentation PR into `master`. The workflow `.github/workflows/docs-pages.yml` intentionally does **not** publish unmerged PR content.
4. In **Actions → Documentation (GitHub Pages)**, confirm both jobs passed. The new Mermaid smoke test must pass in the **build** job.
5. Reload the site. Under **Distributed Hub → Overview** or **Architecture → Runtime overview**, the flowchart should appear as an interactive SVG, not a fenced code block.

If the job is still named **pages build and deployment** and its logs say `Jekyll`, GitHub Pages is continuing to publish from the branch/Jekyll source instead of the MkDocs Actions artifact.

See the [Material for MkDocs Mermaid reference](https://squidfunk.github.io/mkdocs-material/reference/diagrams/) for the supported fence configuration. GitHub Pages itself is a static host; it doesn't interpret Mermaid, but it serves Material's HTML and JavaScript which render Mermaid in the browser.

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
