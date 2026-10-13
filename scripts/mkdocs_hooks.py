"""MkDocs hooks for the documentation site (#40): links from docs/ to the rest of the repository.

A page in docs/ links to files beside it, which the site has, and to files elsewhere in the
repository (`../SECURITY.md`), which it does not. On GitHub both work; on the site the second kind
would be a dead link. `rewrite_links()` turns each into the file's address on GitHub, and refuses one
whose target is not in the repository, so a build with `--strict` fails on it as on any broken link.

The rewriting is a plain function, so the test of it needs no MkDocs (tests/integration).
"""
from __future__ import annotations

import pathlib
import re

REPO = pathlib.Path(__file__).resolve().parents[1]
BLOB = "https://github.com/Smart-Data-Engines/low-cost-and-low-latency-orderbook-dbengine/blob/master/"

# A Markdown link or image whose target starts with "../": the text, then the target without its
# anchor, then the anchor. Inline code and fenced blocks are skipped by the caller.
_LINK = re.compile(r"(\]\()(\.\./[^)#\s]+)(#[^)\s]*)?(\))")
_FENCE = re.compile(r"^(```|~~~)")


def rewrite_links(markdown: str, page: str, repo: pathlib.Path = REPO) -> str:
    """`markdown` of `page` (a path under docs/) with every link out of docs/ pointed at GitHub.

    Raises ValueError naming the page and the link when the target is not in the repository.
    """
    page_dir = (repo / "docs" / page).parent
    out, in_fence = [], False
    for line in markdown.split("\n"):
        if _FENCE.match(line.lstrip()):
            in_fence = not in_fence
        if in_fence or "](../" not in line:
            out.append(line)
            continue

        def replace(m: re.Match) -> str:
            target = (page_dir / m.group(2)).resolve()
            try:
                relative = target.relative_to(repo)
            except ValueError:
                raise ValueError(f"{page}: {m.group(2)} leaves the repository") from None
            if not target.exists():
                raise ValueError(f"{page}: {m.group(2)} is not in the repository")
            return f"{m.group(1)}{BLOB}{relative.as_posix()}{m.group(3) or ''}{m.group(4)}"

        out.append(_LINK.sub(replace, line))
    return "\n".join(out)


def on_page_markdown(markdown, page, config, files):  # noqa: ARG001 - MkDocs hook signature
    try:
        return rewrite_links(markdown, page.file.src_uri)
    except ValueError as exc:
        from mkdocs.exceptions import PluginError
        raise PluginError(str(exc)) from exc
