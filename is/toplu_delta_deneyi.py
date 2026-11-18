"""Web logu tablosunun bir kopyasında Delta zaman yolculuğunu doğrular."""

from uuid import uuid4

from pyspark.sql import SparkSession, functions as F


KAYNAK = "s3a://tablolar/toplu-log"
DENEY_KOKU = "s3a://tablolar/toplu-log-delta-deneyi"


def main():
    spark = (
        SparkSession.builder.appName("BSM461-Delta-deneyi")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        loglar = spark.read.format("delta").load(KAYNAK)
        gun = loglar.agg(F.min("tarih").alias("ilk_gun")).first()["ilk_gun"]
        if gun is None:
            raise RuntimeError("Deney için tarih bulunamadı. Önce toplu_log.py çalıştırın.")

        yol = f"{DENEY_KOKU}/{uuid4().hex}"
        loglar.write.format("delta").mode("errorifexists").partitionBy("tarih").save(yol)

        gun_filtresi = F.col("tarih") == F.lit(gun)
        onceki = spark.read.format("delta").load(yol)
        onceki_adet = onceki.count()
        silinecek_adet = onceki.filter(gun_filtresi).count()

        spark.sql(f"DELETE FROM delta.`{yol}` WHERE tarih = DATE '{gun.isoformat()}'").collect()
        guncel_adet = spark.read.format("delta").load(yol).count()
        eski_adet = spark.read.format("delta").option("versionAsOf", "0").load(yol).count()

        print(f"Deney tablosu: {yol}")
        print(f"Deney tablosundan çıkarılan gün: {gun} | kayıt: {silinecek_adet}")
        print(f"Sürüm 0 (zaman yolculuğu): {eski_adet} | sürüm 1 (güncel): {guncel_adet}")
        spark.sql(f"DESCRIBE HISTORY delta.`{yol}`").select("version", "operation").orderBy("version").show()

        if eski_adet != onceki_adet or guncel_adet != onceki_adet - silinecek_adet:
            raise RuntimeError("Delta sürüm sayımları beklenen değerlerle uyuşmadı.")
        print("Delta zaman yolculuğu doğrulandı.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
