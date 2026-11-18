"""Başarısız bir Delta yazımından sonra son sürümün okunabildiğini sınar."""

from uuid import uuid4

from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession, functions as F



KAYNAK = "s3a://tablolar/toplu-log"
DENEY_KOKU = "s3a://tablolar/toplu-log-atomik-deney"
KASITLI_HATA = "BSM461_KASITLI_YAZMA_HATASI"


def surum_ve_adet(spark, yol):
    surum = spark.sql(f"DESCRIBE HISTORY delta.`{yol}`").agg(F.max("version")).first()[0]
    adet = spark.read.format("delta").load(yol).count()
    return surum, adet


def main():
    spark = (
        SparkSession.builder.appName("BSM461-Delta-atomik-yazma")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        loglar = spark.read.format("delta").load(KAYNAK)
        son_gun = loglar.agg(F.max("tarih")).first()[0]
        if son_gun is None:
            raise RuntimeError("Deney için temiz log bulunamadı. Önce toplu_log.py çalıştırın.")

        hedef = f"{DENEY_KOKU}/{uuid4().hex}"
        loglar.coalesce(1).write.format("delta").partitionBy("tarih").save(hedef)
        onceki_surum, onceki_adet = surum_ve_adet(spark, hedef)
        print(f"Yazma öncesi: sürüm {onceki_surum} | {onceki_adet} satır", flush=True)

        hatali = loglar.withColumn(
            "bayt",
            F.when(F.col("tarih") == F.lit(son_gun), F.raise_error(KASITLI_HATA))
            .otherwise(F.col("bayt")),
        )
        try:
            hatali.coalesce(1).write.format("delta").mode("overwrite").partitionBy("tarih").save(hedef)
        except (Py4JJavaError, PySparkException) as hata:
            if KASITLI_HATA not in str(hata):
                raise
            print(f"Beklenen yazma hatası: {KASITLI_HATA}")
        else:
            raise RuntimeError("Kasıtlı hata oluşmadı. Deney doğrulanamadı.")

        sonraki_surum, sonraki_adet = surum_ve_adet(spark, hedef)
        print(f"Yazma sonrası: sürüm {sonraki_surum} | {sonraki_adet} satır")
        spark.sql(f"DESCRIBE HISTORY delta.`{hedef}`").select("version", "operation").show()
        if (sonraki_surum, sonraki_adet) != (onceki_surum, onceki_adet):
            raise RuntimeError("Başarısız yazma sonrasında tablo sürümü veya satır sayısı değişti.")
        print("Atomik yazma deneyi doğrulandı. Deney tablosu:", hedef)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
