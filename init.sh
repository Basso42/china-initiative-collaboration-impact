mkdir -p data
if [ ! -f data/all_works_16_20.parquet ]; then
    wget -O data/all_works_16_20.parquet https://minio.lab.sspcloud.fr/gamer35/public/all_works_16_20.parquet
else
    echo "File already exists, skipping download."
fi

pip install -r requirements.txt

