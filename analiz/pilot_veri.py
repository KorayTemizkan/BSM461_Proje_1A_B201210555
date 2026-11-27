"""Grup üreticisiyle tarih sabitlenmiş yerel pilotu üretir ve ham JSON kusurlarını sayar."""

import argparse
import csv
import json
import random
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "araclar"))
from grup import tohum
from uretici import toplu, uretec


GRUP = "B201210555"
ALANLAR = ("olay_zamani", "ip", "yontem", "yol", "durum", "bayt", "sure_ms", "ajan")


class YerelHamDepo:
    def __init__(self, kok):
        self.kok = kok

    def get_paginator(self, islem):
        assert islem == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix):
        assert Bucket == "ham"
        return iter([{"Contents": []}])

    def put_object(self, *, Bucket, Key, Body):
        assert Bucket == "ham"
        dosya = self.kok / Key
        dosya.parent.mkdir(parents=True, exist_ok=True)
        dosya.write_bytes(Body)


def kusur_say(ham):
    ozet = Counter()
    alan_sayilari = {alan: Counter() for alan in ALANLAR}
    gorulen = set()
    for dosya in sorted(ham.rglob("*.jsonl")):
        with dosya.open(encoding="utf-8") as akis:
            for satir in akis:
                kayit = json.loads(satir)
                ozet["ham_satir"] += 1
                if satir in gorulen:
                    ozet["ayni_json_tekrari"] += 1
                gorulen.add(satir)

                kusurlar = set()
                for alan, sayilar in alan_sayilari.items():
                    if alan not in kayit:
                        kusur = "eksik_alan"
                    elif kayit[alan] is None:
                        kusur = "null_deger"
                    elif isinstance(kayit[alan], str) and not kayit[alan].strip():
                        kusur = "bos_metin"
                    else:
                        continue
                    sayilar[kusur] += 1
                    kusurlar.add(kusur)
                ozet.update(kusurlar)
                if kusurlar:
                    ozet["eksik_null_veya_bos"] += 1

    if not ozet["ham_satir"]:
        raise ValueError(f"Ham JSONL dosyası bulunamadı: {ham}")
    return ozet, alan_sayilari


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dizin", type=Path, help="Henüz oluşturulmamış yerel pilot dizini")
    parser.add_argument("--bitis", default="2026-11-18", help="İlk dahil edilmeyen UTC günü")
    args = parser.parse_args()
    bitis = datetime.strptime(args.bitis, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    args.dizin.mkdir()
    ayar = SimpleNamespace(grup=GRUP, sablon="log", gun=7, toplu=10000,
                          kirli_orani=0.002, tekrar_orani=0.002, ekle=False)
    rastgele = random.Random(tohum(GRUP))
    olay = uretec(ayar.sablon, rastgele, GRUP)
    toplu(ayar, olay, rastgele, istemci=YerelHamDepo(args.dizin / "ham"), bitis=bitis)

    ozet, alan_sayilari = kusur_say(args.dizin / "ham/log")
    kusurlar = ("eksik_alan", "null_deger", "bos_metin")
    with (args.dizin / "veri-kalitesi.csv").open("w", encoding="utf-8", newline="") as akis:
        yazici = csv.writer(akis, lineterminator="\n")
        yazici.writerow(["kapsam", "olcut", "satir"])
        for olcut in ("ham_satir", "ayni_json_tekrari", *kusurlar, "eksik_null_veya_bos"):
            yazici.writerow(["tum_kayitlar", olcut, ozet[olcut]])
        for alan, sayilar in alan_sayilari.items():
            for kusur in kusurlar:
                yazici.writerow([alan, kusur, sayilar[kusur]])


if __name__ == "__main__":
    main()
