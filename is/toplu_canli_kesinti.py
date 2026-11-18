"""Aynı kaynakla Delta ve normal Parquet üzerine yazımlarını kesme deneyi."""

from argparse import ArgumentParser
import re
import sys
from time import sleep

from py4j.protocol import Py4JJavaError
from pyspark.errors import PySparkException
from pyspark.sql import SparkSession, functions as F

from toplu_log import HAM, LOG_SEMASI


def yavas_bolum(satirlar):
    ilk = next(satirlar, None)
    if ilk is not None:
        print("YAZMA GOREVI BASLADI: Ctrl+C ile simdi kesin.", file=sys.stderr, flush=True)
        # Yazım çalışırken terminalden kesmek için her dolu görevde kısa bir süre bekle.
        sleep(30)
        yield ilk
        yield from satirlar


def kontrol(spark, yollar):
    delta = yollar["delta"]
    surum = spark.sql(f"DESCRIBE HISTORY delta.`{delta}`").agg(F.max("version")).first()[0]
    delta_adet = spark.read.format("delta").load(delta).count()
    print(f"Delta: sürüm {surum} | okunabilen {delta_adet} satır")
    try:
        parquet_adet = spark.read.parquet(yollar["parquet"]).count()
    except (PySparkException, Py4JJavaError) as hata:
        print(f"Parquet: okunamadı | {str(hata).splitlines()[0]}")
    else:
        print(f"Parquet: okunabilen {parquet_adet} satır")


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("islem", choices=("hazirla", "delta", "parquet", "kontrol"))
    parser.add_argument("--deney", default="canli-1")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z0-9-]+", args.deney):
        parser.error("Deney adı küçük harf, rakam ve tire içerebilir.")

    kok = f"s3a://tablolar/toplu-kesinti-deneyi/{args.deney}"
    yollar = {bicim: f"{kok}/{bicim}" for bicim in ("delta", "parquet")}
    spark = SparkSession.builder.appName(f"BSM461-kesinti-{args.islem}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.islem == "kontrol":
            kontrol(spark, yollar)
            return

        loglar = (
            spark.read.schema(LOG_SEMASI).option("recursiveFileLookup", "true").json(HAM)
            .select("olay_zamani", "yol", "durum", "bayt", "sure_ms")
        )
        if args.islem == "hazirla":
            print(f"Üzerine yazım için ham kaynak: {loglar.count()} satır", flush=True)
            baslangic = loglar.limit(1000).coalesce(1).cache()
            for bicim, yol in yollar.items():
                baslangic.write.format(bicim).mode("errorifexists").save(yol)
            kontrol(spark, yollar)
            return

        veri = spark.createDataFrame(loglar.rdd.mapPartitions(yavas_bolum), schema=loglar.schema)
        print(f"Üzerine yazım başlıyor: {args.islem}. Görev başlayınca Ctrl+C yapın.", flush=True)
        veri.write.format(args.islem).mode("overwrite").save(yollar[args.islem])
        print("Yazım tamamlandı. Kesinti ölçülmedi. Yeni bir deney adıyla tekrar hazırlayın.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
