"""Aynı web loglarıyla JSON ve üç Parquet kodeğini karşılaştırır."""

from statistics import median
from time import perf_counter
from uuid import uuid4

from pyspark.sql import SparkSession, functions as F


KAYNAK = "s3a://tablolar/toplu-log"
TARIHE_BOLUNMUS_PARQUET = "s3a://islenmis/toplu-log/"
DENEY_KOKU = "s3a://islenmis/toplu-bicim-deneyi"
BICIMLER = (
    ("JSON", "json", "none"),
    ("Parquet/Snappy", "parquet", "snappy"),
    ("Parquet/Zstandard", "parquet", "zstd"),
    ("Parquet/Gzip", "parquet", "gzip"),
)


def dosya_ozeti(spark, adres):
    yol = spark._jvm.org.apache.hadoop.fs.Path(adres)
    depo = yol.getFileSystem(spark._jsc.hadoopConfiguration())
    dosyalar = depo.listFiles(yol, True)
    adet = toplam_bayt = 0
    while dosyalar.hasNext():
        dosya = dosyalar.next()
        if dosya.getPath().getName().startswith("part-"):
            adet += 1
            toplam_bayt += dosya.getLen()
    if adet == 0:
        raise RuntimeError(f"Veri dosyası bulunamadı: {adres}")
    return adet, toplam_bayt


def hata_sorgusu(loglar):
    return (
        loglar.filter(F.col("durum").between(500, 599))
        .groupBy("rota").count()
    )


def main():
    spark = (
        SparkSession.builder.appName("BSM461-bicim-olcumu")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        loglar = spark.read.format("delta").load(KAYNAK)
        temiz_adet = loglar.count()
        kok = f"{DENEY_KOKU}/{uuid4().hex}"
        olcumler = {}
        for ad, bicim, kodek in BICIMLER:
            adres = f"{kok}/{bicim}-{kodek}"
            loglar.coalesce(1).write.format(bicim).option("compression", kodek).save(adres)
            okunan = spark.read.format(bicim).schema(loglar.schema).load(adres)
            adet = okunan.count()
            if adet != temiz_adet:
                raise RuntimeError(f"{ad}: {adet} satır okundu, {temiz_adet} bekleniyordu.")
            olcumler[ad] = {
                "sorgu": hata_sorgusu(okunan),
                "boyut": dosya_ozeti(spark, adres),
                "sureler": [],
            }

        beklenen = None
        for olcum in olcumler.values():
            sonuc = {satir.rota: satir["count"] for satir in olcum["sorgu"].collect()}
            if beklenen is None:
                beklenen = sonuc
            elif sonuc != beklenen:
                raise RuntimeError("Biçimler aynı sorgu sonucunu vermedi.")

        # İlk tur ısınma içindi. Ölçülen turlarda başlangıç sırasını değiştir.
        sirali = list(olcumler.values())
        for tur in range(3):
            for olcum in sirali[tur:] + sirali[:tur]:
                baslangic = perf_counter()
                satirlar = olcum["sorgu"].collect()
                olcum["sureler"].append((perf_counter() - baslangic) * 1000)
                if {satir.rota: satir["count"] for satir in satirlar} != beklenen:
                    raise RuntimeError("Tekrarlı ölçümde sorgu sonucu değişti.")

        print(f"Biçim deneyi: {temiz_adet} temiz satır | rota başına 5xx sayımı")
        print("biçim | dosya adedi | bayt | 3 sorgu süresi (ms) | medyan (ms)")
        for ad, olcum in olcumler.items():
            dosya_adedi, bayt = olcum["boyut"]
            sureler = " / ".join(f"{sure:.1f}" for sure in olcum["sureler"])
            print(f"{ad} | {dosya_adedi} | {bayt} | {sureler} | {median(olcum['sureler']):.1f}")

        bolunmus = spark.read.parquet(TARIHE_BOLUNMUS_PARQUET)
        dosya_adedi, bayt = dosya_ozeti(spark, TARIHE_BOLUNMUS_PARQUET)
        print(f"Tarihe bölünmüş Parquet: {dosya_adedi} dosya | {bayt} bayt | "
              f"ortalama {bayt / dosya_adedi:.1f} bayt/dosya")
        gun = bolunmus.agg(F.max("tarih").alias("gun")).first()["gun"]
        print(f"Plan · Parquet · yalnız {gun} tarihindeki 5xx hataları")
        hata_sorgusu(bolunmus.filter(F.col("tarih") == F.lit(gun))).explain(mode="formatted")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
