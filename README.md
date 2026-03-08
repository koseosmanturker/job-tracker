# Job Tracker

LinkedIn iş başvurularını Gmail üzerinden toplayıp `jobs.csv` içine yazan ve Flask dashboard üzerinden görüntüleyen bir takip projesi.

## Ozellikler

- Gmail API ile LinkedIn bildirim maillerini okur
- Mail iceriginden `company`, `job_title`, `location`, `job_url` alanlarini ayiklar
- Verileri `jobs.csv` dosyasina birlestirerek yazar (duplicate azaltma mantigi var)
- Web arayuzde filtreleme, siralama ve durum goruntuleme saglar
- CLI ile secili kayit icin `downloaded=True` isaretleme yapar

## Proje Yapisi

- `gmail_client.py`: Gmail API baglantisi, mesaj listeleme/cekme, header ve body okuma
- `linkedin_parser.py`: LinkedIn mail parse/normalization ve alan cikarma
- `repository.py`: CSV okuma-yazma, keyleme, merge/upsert mantigi
- `sync_service.py`: Uctaki senkronizasyon akisi (Gmail -> Parser -> CSV)
- `dashboard.py`: Flask web uygulamasi
- `templates/dashboard.html`: Dashboard arayuzu
- `set_downloaded.py`: Interaktif olarak `downloaded` alanini guncelleme

## Gereksinimler

- Python 3.10+
- Gmail API credentials (`credentials.json`)
- Ilk OAuth girisinden sonra olusan `token.json`

Onerilen paketler:

```bash
pip install flask python-dotenv google-api-python-client google-auth-httplib2 google-auth-oauthlib
```

## Calistirma Sirasi

### 1) Mail senkronizasyonu

```bash
python sync_service.py
```

Bu adim:
- Gmail'den uygun LinkedIn maillerini ceker
- Parse eder
- `jobs.csv` dosyasini gunceller

### 2) Dashboard acma

```bash
python dashboard.py
```

Sonra tarayicida:

`http://127.0.0.1:5000`

### 3) (Opsiyonel) Downloaded isaretleme

```bash
python set_downloaded.py
```

## GitHub'a Yukleme (Ozet)

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/<kullanici_adi>/job-tracker.git
git push -u origin main
```

## Guvenlik Notu

Asagidaki dosyalari repoya koyma:

- `credentials.json`
- `token.json`
- `.env` (varsa)

`.gitignore` icinde bunlarin oldugundan emin ol.

## Lisans

Bu proje su an lisans tanimi icermiyor. Istersen MIT lisansi ekleyebilirsin.
