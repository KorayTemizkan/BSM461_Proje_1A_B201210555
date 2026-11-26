"""Rapor ve sunum Markdown kaynaklarından teslim PDF'lerini üretir."""

import re
from pathlib import Path

from markdown import markdown
from weasyprint import CSS, HTML


KOK = Path(__file__).resolve().parent
DOSYA_ADI = "BSM461_Proje_1A_B201210555"
SAYFA = "<!-- sayfa -->"
DEPO_DOSYALARI = "https://github.com/KorayTemizkan/BSM461_Proje_1A_B201210555/blob/main/"


def pdf_uret(kaynak, hedef):
    govde = (KOK / kaynak).read_text(encoding="utf-8")
    sinif = "sunum" if kaynak == "sunum.md" else "rapor"
    bolumler = [markdown(bolum.strip(), extensions=["tables", "extra"]) for bolum in govde.split(SAYFA)]
    icerik = "\n".join(f"<section>\n{bolum}\n</section>" for bolum in bolumler)
    icerik = re.sub(r'(<a href=")\.\./', lambda eslesme: eslesme[1] + DEPO_DOSYALARI, icerik)
    belge = (
        '<!doctype html><html lang="tr"><meta charset="utf-8">'
        f'<title>{DOSYA_ADI} · {sinif}</title>'
        f'<body class="{sinif}">{icerik}</body></html>'
    )
    HTML(string=belge, base_url=KOK).write_pdf(KOK / hedef, stylesheets=[CSS(KOK / "baski.css")])


if __name__ == "__main__":
    pdf_uret("rapor.md", f"{DOSYA_ADI}.pdf")
    pdf_uret("sunum.md", f"{DOSYA_ADI}_sunum.pdf")
