# BSM461 · Depolama ve toplu işleme

**Şube 1A · Grup B201210555 · Yüksel’in yürüttüğü deneyler**

## 5. Depolama

### Dosya biçimi ve sıkıştırma (Z2)

Grup koduna göre üretilen yedi günlük web logu, yerel S3 uyumlu nesne deposuna JSON Lines olarak yazıldı. Kodun ilk doğrulaması 10.000 ham satırla yapıldı. Biçim deneyinde aynı 9.959 temiz satır her seçenekte bir veri dosyasına yazıldı. Biçim farkını ölçmek için JSON ve Parquet kopyaları aynı temiz tablodan oluşturuldu.

Sorgu tüm biçimlerde HTTP 500–599 kayıtlarını filtreleyip rota başına hata sayısını topladı. Sonuçlar dört biçimde aynı çıktı. Her biçime bir ısınma sorgusu uygulandı ve ardından üç çalışma süresi ölçüldü. Ölçüm, sorgudan sonuçlar Spark sürücüsüne ulaşana kadar geçen süredir.

| Biçim | Veri dosyası | Bayt | Sorgu süreleri (ms) | Ortalama (ms) | Ortanca (ms) |
|---|---:|---:|---|---:|---:|
| JSON, sıkıştırmasız | 1 | 1.852.908 | 52,6 / 75,6 / 61,6 | 63,27 | 61,6 |
| Parquet, Snappy | 1 | 194.160 | 53,2 / 59,4 / 38,7 | 50,43 | 53,2 |
| Parquet, Zstandard | 1 | 150.036 | 44,6 / 51,7 / 49,7 | 48,67 | 49,7 |
| Parquet, Gzip | 1 | 158.596 | 42,8 / 59,2 / 64,9 | 55,63 | 59,2 |

Bu veri ve sorgu için Zstandard, sıkıştırılmamış JSON’a göre yaklaşık %91,9 daha az alan kullandı. Üç Parquet seçeneği içinde en küçük dosyayı ve en düşük ortalama sorgu süresini verdi. Bu ölçüme dayanarak Zstandard arşiv çıktısı için tercih edilebilir. Ölçülen ana toplu hat Spark’ın mevcut Parquet ayarlarıyla yazılmıştır. Zstandard seçimi ana koda uygulanırsa dosya düzeni ve zaman ölçümleri yeniden alınmalıdır.

### Nesne deposu ve dosya düzeni (Z4)

| İçerik | Konum |
|---|---|
| Ham loglar | `s3://ham/log/tarih=…/` |
| IP boyut tablosu | `s3://ham/boyut/log/ip_bilgisi.csv` |
| Tarihe göre bölünmüş Parquet | `s3a://islenmis/toplu-log/tarih=…/` |
| Delta tablosu | `s3a://tablolar/toplu-log` |

İşlenmiş tablonun `tarih` alanı olay zamanından Türkiye saat dilimine göre hesaplandı. Tarihe göre bölümleme, tek bir gün sorgulandığında Spark’ın diğer tarih bölümlerini taramadan geçmesine imkân verir. Rota gibi çok sayıda değer alabilen bir alanla bölümleme küçük dosya sayısını artırabileceği için tarih kullanıldı.

İlk çıktıda 64 Parquet dosyasının toplam boyutu 387.674 bayt, ortalama dosya boyutu 6.057,4 bayttı. Tarihe göre yeniden dağıtıp ayrı bir Parquet dizinine yazarak 8 dosya elde ettik. Toplam boyut 217.743 bayta indi ve ortalama dosya boyutu 27.217,9 bayta yükseldi. Dosya sayısı %87,5, toplam boyut yaklaşık %43,8 azaldı. Bu sonuç küçük veri setindeki dosya düzeni ve depolama hacmini gösterir.

Nesne deposunda çok sayıda küçük nesne, listeleme ve açma işlemlerinin maliyetini artırabilir. Dizin görünümü anahtar öneklerinden oluşur. HDFS’deki atomik dizin yeniden adlandırmasına denk bir işlem bulunmadığından sıradan Parquet dosyalarına doğrudan yazarken okuyucunun hangi dosyaları tek bir yayımlanmış sonuç olarak göreceğini ayrıca yönetmek gerekir.

### Delta tablo biçimi (Z7)

Ana toplu sonuç Delta tablosuna yazıldı. Zaman yolculuğu için ayrı bir deney kopyası oluşturuldu. Kopyanın ilk sürümünden 11 Kasım 2026’ya ait kayıtlar çıkarıldı. `DESCRIBE HISTORY` iki işlemi gösterdi. `versionAsOf=0` ile ilk sürüm yeniden okundu.

| Delta sürümü | İşlem | Okunabilen satır |
|---|---|---:|
| 0 | İlk yazım (`WRITE`) | 9.959 |
| 1 | İlk günün çıkarılması (`DELETE`) | 8.631 |

Sürümler arasındaki 1.328 satırlık fark, çıkarılan güne ait kayıt sayısıyla eşleşti.

Ayrı bir Delta kopyasında üzerine yazma sırasında bir Spark görevi kasıtlı `BSM461_KASITLI_YAZMA_HATASI` istisnasıyla durduruldu. İş iptal edildi. Yazma öncesi ve sonrasında son commit **sürüm 0** olarak kaldı ve her iki okumada da **9.959 satır** elde edildi. İşlem geçmişinde yalnızca başarılı ilk yazım göründü. Bu kontrollü koşuda başarısız yazım yeni bir tablo sürümü yayımlamadı.

### Canlı yazma kesintisi (Adım 10)

Üretici 14 gün için 3.000.000 satır ekledi. Önceki 10.000 pilot satırı korunduğu için Spark, üzerine yazma kaynağında toplam **3.010.000 ham satır** saydı. Deney için Delta ve sıradan Parquet’e aynı **1.000 satırlık** başlangıç kopyası yazıldı. Her iki biçimde, büyük kaynaktan yerel iki görevli Spark işiyle üzerine yazma başlatıldı. Yazma görevinin başladığı bildirildikten sonra her iş kendi terminal oturumunda `Ctrl+C` ile durduruldu.

| Biçim | Kesinti öncesi | Kesinti sonrası |
|---|---|---|
| Delta | Sürüm 0, 1.000 satır | Sürüm 0, 1.000 satır okunabildi |
| Normal Parquet | 1.000 satır | Spark okuyamadı. `UNABLE_TO_INFER_SCHEMA` hatası verdi. |

Bu koşuda Delta’nın son commit’i okunabilir kaldı. Normal Parquet’in üzerine yazma hedefi, otomatik şema çıkarımıyla yeniden okunamadı. Parquet dizininde hangi geçici nesnelerin kaldığı ayrıca sayılmadı. Komutlar ve sonuçlar için kanıt dosyası `kanit/yuksel/delta-parquet-canli-kesinti.txt`.

## 7. Toplu Spark hattı (Z5)

`is/toplu_log.py` ham dosyaları şeması belirlenmiş biçimde okudu, tekrarları ayıkladı ve boş, eksik ya da geçersiz alanlar içeren tekil kayıtları çıkardı. HTTP durum kodu 100–599 aralığında olmalı ve süre ile bayt sayısı negatif olmamalıdır. Temiz olaylar `ip` üzerinden IP bilgi tablosuyla birleştirildi. 2.000 satırlık boyut tablosu için `broadcast` kullanıldı. Sol birleştirme sayesinde boyut eşleşmesi belirsiz kayıtlar işlenmeye devam edebildi. Ana çıktıdaki ülke ve kurum türü alanları boyut tablosundan gelir. Ham IP ve kullanıcı ajanı işlenmiş tabloya yazılmadı.

| Kontrol | Satır |
|---|---:|
| Ham satır | 10.000 |
| Tekrar olarak çıkarılan | 24 |
| Geçersiz tekil olarak çıkarılan | 17 |
| Temiz, birleştirme öncesi | 9.959 |
| Temiz, birleştirme sonrası | 9.959 |

17 geçersiz tekil satır bu betikte toplu sayıldı. Eksik alan ve boş değerlerin ayrı adetleri bu koşunun çıktısında bulunmuyor.

1. **Trafik hangi saatlerde yoğunlaşıyor?** Türkiye saatine göre 22.00–22.59 aralığı yedi günlük denemede toplam **778 istekle** en yoğun aralıktı.
2. **En çok 5xx hatası hangi rotada oluşuyor?** `/urun/{id}` **3.768 istekte 247 hata** (%6,56) ile hata adedi sıralamasında baştaydı. `/static/logo.png` için 525 istekte 41 hata (%7,81) ölçüldü. Hata adedi ve istek başına hata oranı ayrı göstergelerdir.

### Fiziksel plan

Tarihe bölünmüş Parquet üzerinde yalnızca 18 Kasım 2026’nın 5xx kayıtlarını sayan örnek sorguda `PartitionFilters` yalnızca istenen tarih bölümünün seçildiğini gösterdi. `PushedFilters` satırında Spark, `durum` sütunundaki 500–599 filtresini Parquet okuyucusuna iletti. `ReadSchema` veri dosyalarından yalnızca `rota` ve `durum` sütunlarının istendiğini gösterdi.

`HashAggregate` önce her bölümde rota bazında kısmi sayım yaptı. Ardından `Exchange`, aynı rotanın kısmi sonuçlarını bir araya getirmek için `hashpartitioning(rota, 8)` kullandı. Üstteki `HashAggregate` bu ara sayımları birleştirdi. Genel ilk 10 rota sorgusunda `TakeOrderedAndProject` hata adedine göre ilk 10 sonucu üretti.

### Önce ve sonra performans ölçümü

Dosya birleştirme öncesinde ve sonrasında aynı 9.959 satır yer aldı ve rota başına 5xx sayıları eşleşti. Her düzende bir ısınma sorgusundan sonra beş ölçüm alındı. Ölçüm sırası dönüşümlüydü.

| Parquet düzeni | Dosya | Bayt | Sorgu süreleri (ms) | Ortalama (ms) | Ortanca (ms) |
|---|---:|---:|---|---:|---:|
| Birleştirme öncesi | 64 | 387.674 | 68,9 / 56,8 / 72,5 / 58,2 / 56,4 | 62,56 | 58,2 |
| Birleştirme sonrası | 8 | 217.743 | 62,1 / 63,3 / 68,5 / 58,8 / 55,1 | 61,56 | 62,1 |

Ortalama 1,00 ms azalırken ortanca 3,9 ms arttı. Bu küçük denemede dosya sayısı ve boyutu düştü. Süre ölçümleri tutarlı bir sorgu hızlanması göstermedi.

Spark UI’de bir tekrarın **Stage 36** ayrıntısında tek bir başarılı görevin süresi **6 ms**, shuffle okuması **4,4 KiB ve 93 ara kayıt** olarak göründü. Bu değerler görev seviyesindedir. Yukarıdaki süreler ise sorgunun tamamına aittir.

### Grup raporuna ekleme

- Biçim karşılaştırması, dosya birleştirme, veri temizliği ve Delta sonuçlarını ortak `kanit/olcumler.csv` dosyasına aktarın.
- Fiziksel plan çıktısını, Spark UI görüntülerini ve Delta deneylerinin terminal çıktılarını `kanit/` altına ekleyin.
- 10.000 satırlık koşuyu biçim ve Spark performansı için pilot ölçüm olarak işaretleyin. Canlı kesinti için ayrı olarak 3.010.000 ham satırlık kaynak kullanıldı.
- Raporun 10. bölümü için eksik alan ile null değer adetlerini ayrı ölçün ve iki toplu sorgunun grafiklerini ekleyin.
- Yapay zekâ beyanında, toplu iş ve deney betiklerinin taslağı için ChatGPT’den yararlanıldığını ve ölçümlerin Yüksel’in Windows bilgisayarında alındığını belirtin.
