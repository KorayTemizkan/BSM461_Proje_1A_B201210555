"""Tarih bölümlerindeki küçük Parquet dosyalarını birleştirip sorguyu ölçer."""

from argparse import ArgumentParser
from statistics import median
from time import perf_counter, sleep
from uuid import uuid4

from pyspark.sql import SparkSession

from toplu_bicim_olcumu import TARIHE_BOLUNMUS_PARQUET, dosya_ozeti, hata_sorgusu


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--ui-bekle", type=int, default=0, choices=(0, 60))
    ayarlar = parser.parse_args()
    spark = (
        SparkSession.builder.appName("BSM461-Parquet-dosya-birlestirme")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        kaynak = TARIHE_BOLUNMUS_PARQUET
        hedef = f"s3a://islenmis/toplu-parquet-birlesik/{uuid4().hex}"
        once = spark.read.parquet(kaynak)
        spark.sparkContext.setJobGroup("birlesim", "Parquet dosyalarını tarihe göre birleştir")
        once.repartition("tarih").write.partitionBy("tarih").parquet(hedef)
        sonra = spark.read.parquet(hedef)

        spark.sparkContext.setJobGroup("satir-kontrolu", "Önce ve sonra satır sayısı")
        onceki_adet = once.count()
        if sonra.count() != onceki_adet:
            raise RuntimeError("Birleştirme satır sayısını değiştirdi.")

        sorgular = {"önce": hata_sorgusu(once), "sonra": hata_sorgusu(sonra)}
        spark.sparkContext.setJobGroup("isinma", "Her iki Parquet düzeni için ısınma")
        beklenen = {satir.rota: satir["count"] for satir in sorgular["önce"].collect()}
        if {satir.rota: satir["count"] for satir in sorgular["sonra"].collect()} != beklenen:
            raise RuntimeError("Birleştirme 5xx sorgusunun sonucunu değiştirdi.")

        sureler = {"önce": [], "sonra": []}
        for tur in range(1, 6):
            sira = ("önce", "sonra") if tur % 2 else ("sonra", "önce")
            for ad in sira:
                spark.sparkContext.setJobGroup(f"{ad}-{tur}", f"5xx sorgusu: {ad}, tur {tur}")
                baslangic = perf_counter()
                satirlar = sorgular[ad].collect()
                sureler[ad].append((perf_counter() - baslangic) * 1000)
                if {satir.rota: satir["count"] for satir in satirlar} != beklenen:
                    raise RuntimeError(f"{ad}: rota başına 5xx sayımı farklı.")

        print(f"Karşılaştırma: {onceki_adet} satır | iki düzen de aynı 5xx sayımını verdi")
        for ad, adres in (("önce", kaynak), ("sonra", hedef)):
            adet, bayt = dosya_ozeti(spark, adres)
            seri = " / ".join(f"{sure:.1f}" for sure in sureler[ad])
            print(f"{ad} | {adet} dosya | {bayt} bayt | {bayt / adet:.1f} bayt/dosya "
                  f"| süreler: {seri} ms | ortanca: {median(sureler[ad]):.1f} ms")

        if ayarlar.ui_bekle:
            print(f"Spark UI: http://localhost:4040/ · {ayarlar.ui_bekle} saniye açık", flush=True)
            sleep(ayarlar.ui_bekle)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
