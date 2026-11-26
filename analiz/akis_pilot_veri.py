"""Üç deterministik watermark pilotu girdi serisini üretir veya Kafka'ya yollar."""

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "araclar"))
from grup import tohum
from uretici import kafka_anahtari, uretec, zaman_damgasi


def mesajlar(evre):
    grup = "B201210555"
    olay = uretec("log", random.Random(tohum(grup) + evre), grup)
    baslangic = datetime(2026, 11, 17, 16, 0, tzinfo=timezone.utc)
    if evre == 1:
        gruplar = [(100, baslangic)]
    elif evre == 2:
        gruplar = [(70, baslangic + timedelta(minutes=1)), (30, baslangic - timedelta(minutes=5))]
    else:
        gruplar = [(10, baslangic + timedelta(minutes=2)), (10, baslangic - timedelta(minutes=25))]

    satirlar = []
    for adet, zaman in gruplar:
        for sira in range(adet):
            an = zaman + timedelta(milliseconds=sira)
            kayit = {"olay_zamani": zaman_damgasi(an), **olay(an)}
            anahtar = kafka_anahtari(kayit, "ip-yol").decode("utf-8")
            satirlar.append(f"{anahtar}\t{json.dumps(kayit, ensure_ascii=False)}")
    if evre == 1:
        satirlar.append(satirlar[0])
    return satirlar


def kafka_gonder(satirlar, broker, konu):
    from confluent_kafka import Producer

    hatalar = []

    def teslim(hata, _mesaj):
        if hata is not None:
            hatalar.append(str(hata))

    uretici = Producer({
        "bootstrap.servers": broker,
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": 10000,
    })
    for satir in satirlar:
        anahtar, deger = satir.split("\t", 1)
        uretici.produce(konu, key=anahtar.encode(), value=deger.encode(), on_delivery=teslim)
    kalan = uretici.flush(12)
    if kalan or hatalar:
        raise RuntimeError(f"Kafka teslimi başarısız. Bekleyen: {kalan}, hatalar: {hatalar[:3]}")
    print(f"Kafka onayı · {len(satirlar)} mesaj")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evre", type=int, choices=(1, 2, 3))
    parser.add_argument("--kafka", help="Kafka broker adresi")
    parser.add_argument("--konu", help="Pilota ayrılmış Kafka konusu")
    args = parser.parse_args()
    if bool(args.kafka) != bool(args.konu):
        parser.error("--kafka ve --konu birlikte kullanılmalı")
    if args.kafka:
        kafka_gonder(mesajlar(args.evre), args.kafka, args.konu)
    else:
        print("\n".join(mesajlar(args.evre)))
