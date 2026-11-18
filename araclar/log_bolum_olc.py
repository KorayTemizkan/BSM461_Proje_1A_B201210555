"""Aynı sentetik log örneğinde üç Kafka anahtarını CRC32 ile karşılaştır."""
import argparse
import csv
import random
import sys
import zlib
from collections import Counter
from datetime import datetime, timedelta, timezone

from grup import normalle, tohum
from uretici import kafka_anahtari, uretec


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--grup", required=True)
    p.add_argument("--adet", type=int, default=50000)
    p.add_argument("--bolum", type=int, default=8)
    a = p.parse_args()
    if min(a.adet, a.bolum) <= 0:
        p.error("Adet ve bölüm sayısı pozitif olmalıdır.")
    grup = normalle(a.grup)
    olay = uretec("log", random.Random(tohum(grup)), grup)
    an = datetime(2026, 11, 1, tzinfo=timezone.utc)
    kayitlar = [olay(an + timedelta(milliseconds=i)) for i in range(a.adet)]

    w = csv.writer(sys.stdout)
    w.writerow([
        "grup", "olay", "anahtar", "bolum", "en_buyuk_anahtar_yuzde",
        "en_dolu_bolum_bolu_ortalama", "bolum_sayimlari",
    ])
    for alan in ("ip", "yol", "ip-yol"):
        anahtarlar = Counter(kafka_anahtari(o, alan) for o in kayitlar)
        sayim = [0] * a.bolum
        for anahtar, n in anahtarlar.items():
            sayim[zlib.crc32(anahtar) % a.bolum] += n
        w.writerow([
            grup, a.adet, alan, a.bolum,
            round(max(anahtarlar.values()) / a.adet * 100, 4),
            round(max(sayim) / (a.adet / a.bolum), 4),
            " ".join(map(str, sayim)),
        ])


if __name__ == "__main__":
    main()
