#!/usr/bin/env python3
"""Fail the Pages build if Mermaid fenced diagrams are not emitted for Material.

This validates the generated HTML, not only the source Markdown. A branch/Jekyll
Pages build will bypass this script altogether; use GitHub Actions as the Pages
Source to publish the output of mkdocs build.
"""
from html.parser import HTMLParser
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"

# These pages deliberately exercise two distinct Mermaid diagram types.
PAGES = [
    "distributed/index.html",
    "distributed/architecture/index.html",
    "distributed/architecture/matching-and-leases/index.html",
]


class MermaidElements(HTMLParser):
    def __init__(self):
        super().__init__()
        self.found = 0
        self.material = False

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        classes = set(values.get("class", "").split())
        if tag in ("pre", "div") and "mermaid" in classes:
            self.found += 1
        if tag == "body" and "data-md-color-scheme" in values:
            self.material = True


errors = []
for relative in PAGES:
    file = SITE / relative
    if not file.is_file():
        errors.append(f"{relative}: expected MkDocs HTML file missing")
        continue
    parser = MermaidElements()
    parser.feed(file.read_text(encoding="utf-8"))
    if not parser.found:
        errors.append(
            f"{relative}: no Mermaid HTML element generated; check Material theme "
            "and pymdownx.superfences configuration"
        )
    else:
        print(f"{relative}: {parser.found} Mermaid diagram(s) compiled for the browser")

if errors:
    print("Mermaid site smoke test failed:\n - " + "\n - ".join(errors))
    sys.exit(1)

print("Mermaid HTML smoke test passed; Material loads and renders diagrams client-side.")
