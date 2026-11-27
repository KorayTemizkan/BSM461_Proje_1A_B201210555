"""Spark pilotunun küçültülmüş sonuçlarından Z10 grafiklerini çizer."""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


def oku(dosya):
    with dosya.open(encoding="utf-8", newline="") as akis:
        return list(csv.DictReader(akis))


def duzenle(eksen, boyut):
    eksen.spines[["top", "right", "left", "bottom"]].set_visible(False)
    eksen.tick_params(length=0, pad=6, colors="#34485d")
    eksen.set_axisbelow(True)
    eksen.grid(axis=boyut, color="#d7e2eb", linewidth=0.8)
    eksen.set_facecolor("#f5f8fb")


def trafik_grafigi(satirlar, hedef):
    sayilar = {int(r["saat_tr"]): int(r["istek"]) for r in satirlar}
    saatler = list(range(24))
    adetler = [sayilar.get(saat, 0) for saat in saatler]
    zirve = max(adetler)

    sekil, eksen = plt.subplots(figsize=(9, 3.3), layout="constrained")
    duzenle(eksen, "y")
    eksen.bar(saatler, adetler, color=["#065d67" if n == zirve else "#71a8b5" for n in adetler])
    eksen.set(xticks=saatler, ylim=(0, zirve * 1.18), xlabel="Türkiye saati", ylabel="İstek sayısı")
    eksen.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
    for saat, adet in zip(saatler, adetler):
        if adet == zirve:
            eksen.text(saat, adet + zirve * 0.027, str(adet), ha="center", color="#124a56")
    sekil.savefig(hedef, dpi=180)
    plt.close(sekil)


def hata_grafigi(satirlar, hedef):
    satirlar = list(reversed(satirlar))
    rotalar = [r["rota"] for r in satirlar]
    adetler = [int(r["hata_5xx"]) for r in satirlar]
    zirve = max(adetler)

    sekil, eksen = plt.subplots(figsize=(9, 3.6), layout="constrained")
    duzenle(eksen, "x")
    eksen.barh(rotalar, adetler, color=["#065d67" if n == zirve else "#71a8b5" for n in adetler])
    eksen.set(xlim=(0, zirve * 1.14), xlabel="HTTP 5xx sayısı")
    eksen.xaxis.set_major_locator(MaxNLocator(integer=True, nbins=6))
    for rota, adet in zip(rotalar, adetler):
        eksen.text(adet + zirve * 0.015, rota, str(adet), va="center", color="#124a56")
    sekil.savefig(hedef, dpi=180)
    plt.close(sekil)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("veri", type=Path, help="Spark sorgularının CSV dizini")
    parser.add_argument("hedef", type=Path, help="Grafiklerin yazılacağı dizin")
    args = parser.parse_args()
    args.hedef.mkdir(parents=True, exist_ok=True)
    trafik_grafigi(oku(args.veri / "saatlik-trafik.csv"), args.hedef / "saatlik-trafik.png")
    hata_grafigi(oku(args.veri / "rota-5xx.csv"), args.hedef / "rota-5xx.png")


if __name__ == "__main__":
    main()
