#!/bin/bash
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Використання: $0 <зашифрований_архів> <приватний_ключ>"
    exit 1
fi

ENCRYPTED_ARCHIVE="$1"
PRIVATE_KEY="$2"
DECRYPTED_FILE="decrypted_$(basename "$ENCRYPTED_ARCHIVE" _encrypted_parts.tar.gz)"

echo "Розпаковуємо архів $ENCRYPTED_ARCHIVE..."
tar -xzf "$ENCRYPTED_ARCHIVE"
echo "=== Файлы в temp_parts/ ==="
ls -la temp_parts/

echo "Дешифруємо частини..."
for enc_part in temp_parts/*.enc; do
    dec_part="${enc_part%.enc}"
    openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$dec_part"
done

echo "Об'єднуємо частини у архів reconstructed_archive.tar.gz..."
cat $(ls temp_parts/part_* | grep -v '\.enc$' | sort) > reconstructed_archive.tar.gz
echo "=== Размер reconstructed_archive.tar.gz ==="
ls -lh reconstructed_archive.tar.gz
file reconstructed_archive.tar.gz

echo "Перевіряємо що отримали..."
file reconstructed_archive.tar.gz

echo "Файл успішно відновлено!"
cp reconstructed_archive.tar.gz "$DECRYPTED_FILE"

# Очистка
rm -rf temp_parts/ reconstructed_archive.tar.gz

echo "Дешифрований файл: $DECRYPTED_FILE"
