#!/bin/bash
set -euo pipefail

if [ $# -ne 2 ]; then
    echo "Використання: $0 <зашифрований_архів> <приватний_ключ>"
    exit 1
fi

ENCRYPTED_ARCHIVE="$1"
PRIVATE_KEY="$2"
ENCRYPTED_ARCHIVE_BASE=$(basename "$ENCRYPTED_ARCHIVE")
ORIGINAL_NAME="${ENCRYPTED_ARCHIVE_BASE%_encrypted_parts.tar.gz}"

echo "Розпаковуємо архів $ENCRYPTED_ARCHIVE..."
tar -xzf "$ENCRYPTED_ARCHIVE"
ls -la temp_parts/

echo "Дешифруємо частини..."
for enc_part in temp_parts/*.enc; do
    dec_part="${enc_part%.enc}"
    openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$dec_part"
done

echo "Об'єднуємо частини..."
cat $(ls temp_parts/part_* | grep -v '\.enc$' | sort) > "$ORIGINAL_NAME"

rm -rf temp_parts/
echo "Дешифрований файл: $ORIGINAL_NAME"