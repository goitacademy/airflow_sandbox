#!/bin/bash
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Використання: $0 <зашифрований_архів> <приватний_ключ>"
    exit 1
fi

ENCRYPTED_ARCHIVE="$1"
PRIVATE_KEY="$2"
ORIGINAL_NAME=$(basename "$ENCRYPTED_ARCHIVE" _encrypted_parts.tar.gz)

echo "Розпаковуємо архів $ENCRYPTED_ARCHIVE..."
tar -xzf "$ENCRYPTED_ARCHIVE"
echo "=== Файлы в temp_parts/ ==="
ls -la temp_parts/

echo "Дешифруємо частини..."
for enc_part in temp_parts/*.enc; do
    dec_part="${enc_part%.enc}"
    openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$dec_part"
done

echo "Об'єднуємо частини..."
cat $(ls temp_parts/part_* | grep -v '\.enc$' | sort) > temp_restored.tar.gz

echo "Розпаковуємо відновлений архів..."
tar -xzf temp_restored.tar.gz

echo "Файл успішно відновлено: $ORIGINAL_NAME"

# Очистка
rm -rf temp_parts/ temp_restored.tar.gz

echo "Дешифрований файл: $ORIGINAL_NAME"