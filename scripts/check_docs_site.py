#!/usr/bin/env python3
"""Validate curated MkDocs nav and local Distributed Hub links before Pages publishing."""
from pathlib import Path
import re
import sys
import urllib.parse
import yaml

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
CONFIG = ROOT / "mkdocs.yml"
errors = []

# MkDocs accepts the Python-name YAML tag for custom Mermaid superfences.
# SafeLoader does not, so use a narrow constructor for config *inspection*.
class ConfigLoader(yaml.SafeLoader):
    pass

ConfigLoader.add_multi_constructor(
    "tag:yaml.org,2002:python/name:",
    lambda loader, suffix, node: suffix,
)
config = yaml.load(CONFIG.read_text(encoding="utf-8"), Loader=ConfigLoader)

def check_nav(tree):
    if isinstance(tree, list):
        for child in tree:
            check_nav(child)
    elif isinstance(tree, dict):
        for child in tree.values():
            check_nav(child)
    elif isinstance(tree, str) and tree.endswith(".md"):
        path = DOCS / tree
        if not path.is_file():
            errors.append(f"mkdocs.yml nav missing: {tree}")

check_nav(config.get("nav", []))

# Inspect curated content only. The older upstream docs and engineering archive
# remain untouched and have their own historical cross-link assumptions.
for path in [DOCS / "index.md", *sorted((DOCS / "distributed").rglob("*.md"))]:
    if not path.exists():
        continue
    text = path.read_text(encoding="utf-8")
    inside_fence = False
    for lineno, line in enumerate(text.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("```") or stripped.startswith("~~~"):
            inside_fence = not inside_fence
            continue
        if inside_fence:
            continue
        for match in re.finditer(r"(?<!!)\[[^\]]+\]\(([^\s)]+)(?:\s+[^)]*)?\)", line):
            url = match.group(1)
            if url.startswith(("http://", "https://", "mailto:", "#", "data:", "/")):
                continue
            relative = urllib.parse.unquote(url.split("#", 1)[0].split("?", 1)[0])
            if not relative:
                continue
            target = (path.parent / relative).resolve()
            if not target.is_relative_to(DOCS.resolve()) or not target.exists():
                errors.append(f"{path.relative_to(ROOT)}:{lineno} -> {url}")

if errors:
    print("Documentation validation failed:")
    for error in errors:
        print(" -", error)
    sys.exit(1)

print("Documentation navigation and curated relative links: OK")
