"""Check offline links, anchors, compiled source excerpts and deterministic book generation."""
from __future__ import annotations
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
import textwrap
from urllib.parse import unquote, urlsplit
import build

HERE = Path(__file__).resolve().parent


class Document(HTMLParser):
    def __init__(self, path):
        super().__init__(convert_charrefs=True)
        self.ids, self.links, self.chapters, self.regions, self.assets = [], [], [], {}, []
        self.current_region = None; self.in_code = False; self.code = []
        self.feed(path.read_text(encoding="utf-8"))

    def handle_starttag(self, tag, attributes):
        attrs = dict(attributes)
        if "id" in attrs: self.ids.append(attrs["id"])
        if "href" in attrs: self.links.append(attrs["href"])
        if "src" in attrs: self.assets.append(attrs["src"])
        if tag == "section" and "chapter" in attrs.get("class", "").split(): self.chapters.append(attrs["id"])
        if tag == "figure": self.current_region = attrs.get("data-region")
        if tag == "code" and self.current_region: self.in_code = True; self.code = []

    def handle_endtag(self, tag):
        if tag == "code" and self.in_code:
            self.regions.setdefault(self.current_region, []).append("".join(self.code)); self.in_code = False
        if tag == "figure": self.current_region = None

    def handle_data(self, value):
        if self.in_code: self.code.append(value)


def main():
    build.main()
    documents = {path.resolve(): Document(path) for path in HERE.glob("*.html")}
    for path, doc in documents.items():
        assert len(doc.ids) == len(set(doc.ids)), (path, "duplicate IDs")
        assert not doc.assets, (path, "external assets")
        assert "\ufffd" not in path.read_text(encoding="utf-8"), (path, "bad encoding")
        for link in doc.links:
            parsed = urlsplit(link)
            if parsed.scheme:
                assert parsed.scheme == "https", (path, link)
                continue
            target = (path.parent / unquote(parsed.path)).resolve() if parsed.path else path
            assert target.exists(), (path, link, target)
            if parsed.fragment and target.suffix == ".html":
                other = documents.get(target) or Document(target)
                assert unquote(parsed.fragment) in other.ids, (path, link)
        for key, excerpts in doc.regions.items():
            filename, region = key.split(":")
            source = (build.EXAMPLES / filename).read_text(encoding="utf-8")
            match = re.search(r"// book:start " + re.escape(region) + r"\n([\s\S]*?)^[ \t]*// book:end " + re.escape(region), source, re.M)
            assert match is not None
            assert excerpts == [textwrap.dedent(match[1]).rstrip()], (path, key)
    en = documents[(HERE / "book-en.html").resolve()]; ru = documents[(HERE / "book-ru.html").resolve()]
    assert en.chapters == ru.chapters and len(en.chapters) == 34
    assert all(anchor in en.chapters for anchor in ("cluster-clients", "entity-subscriptions", "large-client-messages"))
    manifest = json.loads((HERE / "book-manifest.json").read_text(encoding="utf-8"))
    for path, expected in manifest["source_sha256"].items():
        assert hashlib.sha256((build.REPO_ROOT / path).read_bytes()).hexdigest() == expected, path
    before = {path: path.read_bytes() for path in [*HERE.glob("*.html"), HERE / "book-manifest.json"]}
    build.main()
    assert all(path.read_bytes() == value for path, value in before.items()), "non-deterministic generation"
    print(json.dumps({"Passed": True, "ChaptersPerLanguage": len(en.chapters), "Documents": len(documents), "CheckedSourceFiles": len(manifest["source_sha256"])}))


if __name__ == "__main__":
    main()
