"""BSM461 web logları için toplu Spark işi."""

from pyspark.sql import SparkSession, functions as F


HAM = "s3a://ham/log/"
IP_BILGISI = "s3a://ham/boyut/log/ip_bilgisi.csv"
PARQUET = "s3a://islenmis/toplu-log/"
DELTA = "s3a://tablolar/toplu-log"

LOG_SEMASI = """
    olay_zamani STRING, ip STRING, yontem STRING, yol STRING,
    durum INT, bayt LONG, sure_ms DOUBLE, ajan STRING
"""


def main():
    spark = (
        SparkSession.builder.appName("BSM461-toplu-log")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    ham = spark.read.schema(LOG_SEMASI).option("recursiveFileLookup", "true").json(HAM)
    ham_adet = ham.count()
    tekil = ham.dropDuplicates().cache()
    tekil_adet = tekil.count()

    temiz = (
        tekil.withColumn("olay_zamani", F.try_to_timestamp("olay_zamani"))
        .withColumn("yol", F.trim("yol"))
        .dropna()
        .filter(
            (F.length(F.trim("ip")) > 0)
            & (F.length(F.trim("yontem")) > 0)
            & (F.length("yol") > 0)
            & (F.length(F.trim("ajan")) > 0)
            & F.col("durum").between(100, 599)
            & (F.col("bayt") >= 0)
            & F.col("sure_ms").between(0.0, 1.7976931348623157e308)
        )
        .cache()
    )
    temiz_adet = temiz.count()
    print(f"Ham satır: {ham_adet} | tekrar: {ham_adet - tekil_adet} | "
          f"geçersiz tekil: {tekil_adet - temiz_adet} | temiz: {temiz_adet}")
    tekil.unpersist()

    # Aynı IP için çelişen boyut bilgileri varsa grubu belirsiz işaretle.
    # Böylece sol birleştirme hiçbir isteği düşürmez veya çoğaltmaz.
    ip_bilgisi = (
        spark.read.option("header", "true")
        .schema("ip STRING, ulke STRING, kurum_turu STRING")
        .csv(IP_BILGISI).dropna().dropDuplicates()
        .groupBy("ip")
        .agg(
            F.count("*").alias("eslesme"),
            F.min("ulke").alias("ulke"),
            F.min("kurum_turu").alias("kurum_turu"),
        )
    )

    yerel_zaman = F.from_utc_timestamp("olay_zamani", "Europe/Istanbul")
    kayitlar = (
        temiz.join(F.broadcast(ip_bilgisi), "ip", "left")
        .select(
            "olay_zamani",
            F.to_date(yerel_zaman).alias("tarih"),
            F.hour(yerel_zaman).alias("saat_tr"),
            "yontem",
            F.regexp_replace("yol", r"^(/urun|/api/stok)/[0-9]+$", "$1/{id}").alias("rota"),
            "durum", "bayt", "sure_ms",
            F.when(F.col("eslesme") == 1, F.col("ulke")).otherwise("belirsiz").alias("ulke"),
            F.when(F.col("eslesme") == 1, F.col("kurum_turu")).otherwise("belirsiz").alias("kurum_turu"),
        )
        .cache()
    )

    yazilacak_adet = kayitlar.count()
    if yazilacak_adet != temiz_adet:
        raise RuntimeError(f"IP tablosuyla birleştirme satır sayısını değiştirdi: {temiz_adet} → {yazilacak_adet}")
    print(f"IP tablosuyla birleştirme sonrası: {yazilacak_adet} satır")
    kayitlar.write.mode("overwrite").partitionBy("tarih").parquet(PARQUET)
    print(f"Parquet yazıldı: {PARQUET}")
    kayitlar.write.format("delta").mode("overwrite").partitionBy("tarih").save(DELTA)
    print(f"Delta yazıldı: {DELTA}")
    kayitlar.unpersist()
    temiz.unpersist()

    tablo = spark.read.format("delta").load(DELTA)
    print("Soru 1 · Türkiye saatine göre istek sayısı")
    tablo.groupBy("saat_tr").count().withColumnRenamed("count", "istek").orderBy("saat_tr").show(24)

    print("Soru 2 · En çok 5xx hatası üreten rotalar")
    hata_raporu = (
        tablo.groupBy("rota")
        .agg(
            F.count("*").alias("istek"),
            F.count(F.when(F.col("durum").between(500, 599), 1)).alias("hata_5xx"),
        )
        .withColumn("hata_yuzde", F.round(100.0 * F.col("hata_5xx") / F.col("istek"), 2))
        .filter(F.col("hata_5xx") > 0)
        .orderBy(F.desc("hata_5xx"), "rota")
        .limit(10)
    )
    hata_raporu.show(truncate=False)
    hata_raporu.explain(mode="formatted")
    spark.stop()


if __name__ == "__main__":
    main()
