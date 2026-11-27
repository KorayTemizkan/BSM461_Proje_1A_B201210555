"""B201210555 için ölçülen JSON boyutundan saklama hacmini hesaplar."""

import csv
import json
import sys
from pathlib import Path


KOK = Path(__file__).resolve().parents[1]
ORTALAMA_HIZ = 1500
TEPE_CARPANI = 8
SAKLAMA_GUNU = 30
PILOT_JSON_BAYT = 1_852_908
PILOT_ZSTD_BAYT = 150_036


def kapasite():
    ornekler = (KOK / "kanit/log-ornekleri.jsonl").read_bytes().splitlines()
    for satir in ornekler:
        json.loads(satir)
    boyutlar = [len(satir) for satir in ornekler]
    json_boyutu = sum(boyutlar) / len(boyutlar)
    jsonl_boyutu = json_boyutu + 1  # UTF-8 satır sonu
    zstd_orani = PILOT_ZSTD_BAYT / PILOT_JSON_BAYT

    hacimler = []
    for carpan in (1, 10, 100):
        ortalama = carpan * ORTALAMA_HIZ
        tepe = ortalama * TEPE_CARPANI
        gunde = ortalama * 86_400
        ham = gunde * SAKLAMA_GUNU * jsonl_boyutu
        hacimler.append({
            "senaryo_carpani": carpan,
            "ortalama_olay_s": ortalama,
            "tepe_olay_s": tepe,
            "olay_gun": gunde,
            "olay_30_gun": gunde * SAKLAMA_GUNU,
            "ortalama_json_mb_s": round(ortalama * json_boyutu / 1e6, 6),
            "tepe_json_mb_s": round(tepe * json_boyutu / 1e6, 6),
            "ham_jsonl_gb_30_gun": round(ham / 1e9, 6),
            "tahmini_parquet_zstd_gb_30_gun": round(ham * zstd_orani / 1e9, 6),
        })
    return boyutlar, hacimler


if __name__ == "__main__":
    boyutlar, hacimler = kapasite()
    print(
        f"{len(boyutlar)} olay, {sum(boyutlar)} UTF-8 bayt, "
        f"ortalama {sum(boyutlar) / len(boyutlar):.2f}, "
        f"aralık {min(boyutlar)}–{max(boyutlar)} bayt (LF hariç).",
        file=sys.stderr,
    )
    yazici = csv.DictWriter(sys.stdout, fieldnames=hacimler[0].keys(), lineterminator="\n")
    yazici.writeheader()
    yazici.writerows(hacimler)
