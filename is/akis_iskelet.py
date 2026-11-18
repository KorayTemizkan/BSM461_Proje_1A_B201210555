"""BSM 461, Şablon 1: Kafka → temiz loglar → pencereli metrikler.

Ders kitinin is/akis_iskelet.py giriş noktası. İlk canlı deneme:
    spark-submit is/akis_iskelet.py --sablon log --cikti s3 --deney ilk
"""
import argparse
import hashlib
import json
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession, Window, functions as F

SEMA = """
    olay_zamani STRING, yontem STRING, yol STRING, durum INT, sure_ms DOUBLE
"""
ARAMA_ESLEMESI = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "olay_zamani": {"type": "date"},
            "yontem": {"type": "keyword"},
            "yol": {"type": "keyword"},
            "durum": {"type": "integer"},
            "sure_ms": {"type": "double"},
        },
    },
}


def es_istek(adres, metod, yol, veri=None, icerik="application/json"):
    if isinstance(veri, dict):
        veri = json.dumps(veri, ensure_ascii=False).encode("utf-8")
    istek = Request(
        adres.rstrip("/") + yol, data=veri, method=metod, headers={"Content-Type": icerik}
    )
    with urlopen(istek, timeout=30) as yanit:
        govde = yanit.read()
    return json.loads(govde) if govde else None


def indeks_kur(adres, indeks):
    try:
        es_istek(adres, "PUT", f"/{indeks}", ARAMA_ESLEMESI)
    except HTTPError as hata:
        ayrinti = json.loads(hata.read())
        if ayrinti.get("error", {}).get("type") != "resource_already_exists_exception":
            raise RuntimeError(f"Elasticsearch indeks hatası: {ayrinti}") from hata


def loglari_indeksle(akis_partisyonu, adres, indeks):
    satirlar = []
    for kayit in akis_partisyonu:
        belge = kayit.asDict()
        kimlik = belge.pop("olay_id")
        satirlar.extend([
            json.dumps({"index": {"_id": kimlik}}),
            json.dumps(belge, ensure_ascii=False),
        ])
        if len(satirlar) == 1000:
            bulk_yaz(adres, indeks, satirlar)
            satirlar.clear()
    if satirlar:
        bulk_yaz(adres, indeks, satirlar)


def bulk_yaz(adres, indeks, satirlar):
    yanit = es_istek(
        adres, "POST", f"/{indeks}/_bulk",
        ("\n".join(satirlar) + "\n").encode("utf-8"), "application/x-ndjson",
    )
    if yanit["errors"]:
        hatalar = [i["index"]["error"] for i in yanit["items"] if "error" in i["index"]]
        raise RuntimeError(f"Elasticsearch bulk hatası: {hatalar[:3]}")


def temizle(ham):
    ham = ham.select(F.col("value").cast("string").alias("json"))
    alanlar = ham.select(
        F.sha2("json", 256).alias("olay_id"),
        F.from_json("json", SEMA).alias("log"),
    ).select("olay_id", "log.*")
    return (
        alanlar.withColumn("olay_zamani", F.try_to_timestamp("olay_zamani"))
        .withColumn("yol", F.trim("yol"))
        .filter(
            F.col("olay_zamani").isNotNull()
            & (F.length("yol") > 0)
            & F.col("durum").between(100, 599)
            & F.col("sure_ms").between(0.0, 1.7976931348623157e308)
        )
    )


def metrikler(loglar, pencere, watermark, esik):
    rota = F.regexp_replace("yol", r"^(/urun|/api/stok)/[0-9]+$", "$1/{id}")
    kapsam = F.array(
        F.struct(F.lit("tumu").alias("kapsam"), F.lit("").alias("rota")),
        F.struct(F.lit("rota").alias("kapsam"), rota.alias("rota")),
    )
    boyut = f"{pencere} seconds"
    return (
        loglar.withColumn("grup", F.explode(kapsam))
        .withWatermark("olay_zamani", watermark)
        .groupBy(F.window("olay_zamani", boyut).alias("pencere"), "grup")
        .agg(
            F.count("*").alias("istek"),
            F.sum(F.when(F.col("durum").between(500, 599), 1).otherwise(0)).alias("hata_5xx"),
            F.avg("sure_ms").alias("ortalama_ms"),
        )
        .select(
            F.col("pencere.start").alias("pencere_baslangic"),
            F.col("pencere.end").alias("pencere_bitis"),
            "grup.kapsam", "grup.rota", "istek", "hata_5xx", "ortalama_ms",
        )
        .withColumn("hata_orani", F.col("hata_5xx") / F.col("istek"))
        .withColumn(
            "uyari", (F.col("kapsam") == "tumu") & (F.col("hata_orani") >= esik)
        )
    )


def delta_ekle(df, yol, uygulama, batch_id):
    (
        df.write.format("delta").mode("append")
        .option("txnAppId", uygulama).option("txnVersion", batch_id)
        .save(yol)
    )


def kafka_oku(spark, a):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", a.kafka)
        .option("subscribe", a.konu)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", a.batch_limit)
        .load()
    )


def sorguyu_baslat(yazici, a):
    if a.bekleyeni_isle:
        return yazici.trigger(availableNow=True).start()
    return yazici.trigger(processingTime=f"{a.tetik} seconds").start()


def akis_baslat(spark, a, kok, ham):
    temiz_yol = f"{kok}/temiz"
    metrik_yol = f"{kok}/metrik"
    indeks = f"web-loglari-{a.deney}"
    uygulama = hashlib.sha256(kok.encode()).hexdigest()
    gecerli = temizle(ham)
    (
        spark.createDataFrame([], gecerli.schema).write.format("delta").mode("ignore")
        .save(temiz_yol)
    )
    if a.es:
        indeks_kur(a.es, indeks)

    def temiz_yaz(df, batch_id):
        df = df.persist()
        try:
            # Delta yeniden yürütülen işlemi atlasa bile Spark tüm state çıktısını tüketir.
            if not df.count():
                return
            delta_ekle(df, temiz_yol, f"{uygulama}-temiz", batch_id)
            if a.es:
                es_kayitlari = df.select(
                    "olay_id", F.date_format("olay_zamani", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                    .alias("olay_zamani"), "yontem", "yol", "durum", "sure_ms",
                )
                adres = a.es
                es_kayitlari.foreachPartition(
                    lambda kayitlar: loglari_indeksle(kayitlar, adres, indeks)
                )
        finally:
            df.unpersist()

    def metrik_yaz(df, batch_id):
        df = df.withColumn("batch_id", F.lit(batch_id)).persist()
        try:
            if not df.count():
                return
            delta_ekle(df, metrik_yol, f"{uygulama}-metrik", batch_id)
            if a.goster:
                df.filter("kapsam = 'tumu'").select(
                    "pencere_baslangic", "istek", "hata_5xx", "hata_orani"
                ).orderBy(F.desc("pencere_baslangic")).show(3, truncate=False)
                df.filter("kapsam = 'rota'").select(
                    "pencere_baslangic", "rota", "istek", "ortalama_ms"
                ).orderBy(F.desc("pencere_baslangic"), F.desc("ortalama_ms")).show(
                    5, truncate=False
                )
            uyarilar = df.filter("uyari").select(
                F.date_format("pencere_baslangic", "yyyy-MM-dd HH:mm:ss").alias(
                    "pencere_baslangic"
                ),
                "istek", "hata_5xx", "hata_orani",
            ).take(10)
            for uyari in uyarilar:
                print(
                    f"5xx eşiği · UTC {uyari.pencere_baslangic} · "
                    f"{uyari.hata_5xx}/{uyari.istek} · {uyari.hata_orani:.1%}",
                    flush=True,
                )
        finally:
            df.unpersist()

    try:
        temiz_yazici = (
            gecerli.withWatermark("olay_zamani", a.watermark)
            .dropDuplicatesWithinWatermark(["olay_id"]).writeStream
            .queryName("log-temiz").outputMode("append").foreachBatch(temiz_yaz)
            .option("checkpointLocation", f"{kok}/checkpoint/temiz")
        )
        temiz_sorgu = sorguyu_baslat(temiz_yazici, a)
        if a.bekleyeni_isle:
            print("Kafka'da bekleyen kayıtlar işleniyor.", flush=True)
            temiz_sorgu.awaitTermination()

        loglar = (
            spark.readStream.format("delta").option("withEventTimeOrder", "true")
            .load(temiz_yol)
        )
        metrik_yazici = (
            metrikler(loglar, a.pencere, a.watermark, a.esik).writeStream
            .queryName("log-metrik").outputMode("update").foreachBatch(metrik_yaz)
            .option("checkpointLocation", f"{kok}/checkpoint/metrik")
        )
        metrik_sorgu = sorguyu_baslat(metrik_yazici, a)
        if a.bekleyeni_isle:
            print("Temiz tablodaki bekleyen kayıtlar pencerelere işleniyor.", flush=True)
            metrik_sorgu.awaitTermination()
        else:
            print(f"Temiz loglar ve metrikler çalışıyor · {kok}", flush=True)
            spark.streams.awaitAnyTermination()
    finally:
        for aktif in spark.streams.active:
            aktif.stop()


def raporla(spark, kok):
    temiz_sayi = spark.read.format("delta").load(f"{kok}/temiz").count()
    son = Window.partitionBy("pencere_baslangic", "kapsam", "rota").orderBy(
        F.desc("batch_id")
    )
    metrik = (
        spark.read.format("delta").load(f"{kok}/metrik")
        .withColumn("surum_sirasi", F.row_number().over(son))
        .filter("surum_sirasi = 1").drop("surum_sirasi")
    )
    tumu = metrik.filter("kapsam = 'tumu'")
    sayilan = tumu.agg(F.sum("istek")).first()[0] or 0
    print(f"Temiz tekil olay: {temiz_sayi} | pencere sayımı: {sayilan}", flush=True)
    tumu.select(
        "pencere_baslangic", "istek", "hata_5xx", "hata_orani", "uyari"
    ).orderBy(F.desc("pencere_baslangic")).show(10, truncate=False)
    son_pencere = tumu.agg(F.max("pencere_baslangic")).first()[0]
    if son_pencere is not None:
        (
            metrik.filter((F.col("kapsam") == "rota") & (F.col("pencere_baslangic") == son_pencere))
            .select("rota", "istek", "ortalama_ms")
            .orderBy(F.desc("ortalama_ms"), "rota").show(10, truncate=False)
        )


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sablon", choices=["log"], default="log")
    p.add_argument("--cikti", choices=["s3"], default="s3", help="S3 üzerinde Delta çıktısı")
    p.add_argument("--goster", action="store_true", help="Güncellenen metrikleri konsolda göster")
    p.add_argument("--deney", required=True, help="Çalıştırmanın küçük harf, rakam ve tire içeren adı")
    p.add_argument("--kok", default="s3a://islenmis/akis/log")
    p.add_argument("--konu", default="web-loglari")
    p.add_argument("--kafka", default="kafka:29092")
    p.add_argument("--watermark", default="10 minutes")
    p.add_argument("--pencere", type=int, default=20, help="Sabit pencere (saniye)")
    p.add_argument("--tetik", type=int, default=5, help="Mikro-yığın aralığı (saniye)")
    p.add_argument("--esik", type=float, default=0.10, help="5xx uyarı eşiği (0–1)")
    p.add_argument("--batch-limit", type=int, default=20000)
    p.add_argument("--es", help="Elasticsearch adresi, örneğin http://elasticsearch:9200")
    mod = p.add_mutually_exclusive_group()
    mod.add_argument("--rapor", action="store_true", help="Kaydedilmiş sonuçları oku")
    mod.add_argument(
        "--bekleyeni-isle", action="store_true",
        help="Checkpoint'ten devam et, mevcut veriyi işle, raporu yaz ve kapan",
    )
    a = p.parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,47}", a.deney):
        p.error("Deney adı küçük harf, rakam ve tire içermelidir; en fazla 48 karakter.")
    if not 0 < a.esik <= 1 or min(a.pencere, a.tetik, a.batch_limit) < 1:
        p.error("Eşik 0–1 aralığında ve pencere, tetik, yığın sınırı pozitif olmalıdır.")

    spark = (
        SparkSession.builder.appName(f"web-log-{a.deney}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.databricks.delta.snapshotPartitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        kok = f"{a.kok.rstrip('/')}/{a.deney}"
        if a.rapor:
            raporla(spark, kok)
        else:
            akis_baslat(spark, a, kok, kafka_oku(spark, a))
            if a.bekleyeni_isle:
                raporla(spark, kok)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
