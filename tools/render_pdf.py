"""Render PHASE1_REPORT.md to PDF with mermaid diagrams as inline SVGs.

Pipeline: extract ```mermaid blocks → mmdc → SVG → swap into a tmp markdown →
pandoc → HTML → weasyprint → PDF.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


HERE = Path(__file__).resolve().parent.parent
MD = HERE / "PHASE1_REPORT.md"
PDF = HERE / "PHASE1_REPORT.pdf"

MERMAID_BLOCK = re.compile(r"```mermaid\s*\n(.*?)\n```", re.DOTALL)


def run(cmd: list[str], **kwargs) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, **kwargs)
    if res.returncode != 0:
        sys.stderr.write(f"FAIL: {' '.join(cmd)}\n{res.stderr}\n")
        sys.exit(res.returncode)
    return res.stdout


def render_mermaid_to_png(src: str, out_png: Path) -> None:
    """Render to PNG (not SVG): mermaid-cli's SVG embeds web fonts that
    weasyprint cannot resolve, leaving text invisible. PNG rasterises the
    text so it survives PDF embedding regardless of font availability."""
    with tempfile.NamedTemporaryFile("w", suffix=".mmd", delete=False) as f:
        f.write(src)
        mmd_path = f.name
    try:
        run(["mmdc", "-i", mmd_path, "-o", str(out_png), "-b", "white",
             "-q", "--scale", "3", "--width", "1600"])
    finally:
        os.unlink(mmd_path)


CSS = """
@page { size: A4; margin: 18mm 14mm; }
body { font-family: -apple-system, "Helvetica Neue", Arial, sans-serif;
       font-size: 10.5pt; line-height: 1.45; color: #222; }
h1 { font-size: 22pt; border-bottom: 2px solid #333; padding-bottom: 4px;
     page-break-before: always; }
h1:first-of-type { page-break-before: avoid; }
h2 { font-size: 16pt; margin-top: 1.6em; color: #1a3a6c; }
h3 { font-size: 13pt; margin-top: 1.3em; color: #2b4a7a; }
h4 { font-size: 11pt; }
code { font-family: "SF Mono", Menlo, Consolas, monospace; font-size: 9pt;
       background: #f4f4f4; padding: 1px 4px; border-radius: 3px; }
pre { background: #f4f4f4; padding: 8px 10px; border-radius: 4px;
      overflow-x: auto; font-size: 8.8pt; line-height: 1.35;
      white-space: pre-wrap; word-wrap: break-word; }
pre code { background: transparent; padding: 0; }
table { border-collapse: collapse; margin: 0.6em 0; font-size: 9.5pt;
        width: 100%; }
th, td { border: 1px solid #bbb; padding: 4px 8px; text-align: left;
         vertical-align: top; }
th { background: #eef; font-weight: 600; }
img { max-width: 100%; height: auto; display: block; margin: 0.8em auto; }
blockquote { border-left: 3px solid #aac; padding-left: 12px; color: #555;
             margin: 0.5em 0; }
hr { border: 0; border-top: 1px solid #ccc; margin: 1.6em 0; }
ul, ol { padding-left: 22px; }
"""


def main() -> int:
    if not MD.exists():
        sys.exit(f"missing {MD}")

    # Verify tools
    for tool in ("mmdc", "pandoc"):
        if shutil.which(tool) is None:
            sys.exit(f"required tool '{tool}' not on PATH")

    # Need DYLD_LIBRARY_PATH for weasyprint on macOS
    os.environ.setdefault("DYLD_LIBRARY_PATH", "/opt/homebrew/lib")
    try:
        import weasyprint  # noqa: F401
    except Exception as e:
        sys.exit(f"weasyprint not importable: {e}")

    md_text = MD.read_text()
    work = Path(tempfile.mkdtemp(prefix="phase1_pdf_"))
    print(f"workdir: {work}", file=sys.stderr)

    # Extract & render mermaid blocks
    rendered: list[Path] = []
    counter = {"i": 0}

    def replace(m: re.Match) -> str:
        counter["i"] += 1
        idx = counter["i"]
        png = work / f"diagram_{idx}.png"
        render_mermaid_to_png(m.group(1), png)
        rendered.append(png)
        # Use absolute path so pandoc resolves regardless of cwd
        return f"\n![Diagram {idx}]({png})\n"

    md_swapped = MERMAID_BLOCK.sub(replace, md_text)
    print(f"rendered {len(rendered)} mermaid diagrams", file=sys.stderr)

    md_tmp = work / "report.md"
    md_tmp.write_text(md_swapped)

    # Markdown → HTML via pandoc
    html_path = work / "report.html"
    css_path = work / "style.css"
    css_path.write_text(CSS)
    run([
        "pandoc", str(md_tmp),
        "-f", "gfm+yaml_metadata_block",
        "-t", "html5",
        "--standalone",
        "--metadata", "title=Phase 1 Report — NoSQL ETL Framework",
        "-c", str(css_path),
        "-o", str(html_path),
    ])

    # HTML → PDF via weasyprint
    from weasyprint import HTML
    HTML(filename=str(html_path)).write_pdf(str(PDF))
    print(f"wrote {PDF}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
