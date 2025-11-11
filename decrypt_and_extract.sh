#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Використання: $0 <зашифрований_архів> <приватний_ключ>"
    exit 1
fi

ENCRYPTED_ARCHIVE="$1"
PRIVATE_KEY="$2"
DECRYPTED_FILE="decrypted_$(basename "$ENCRYPTED_ARCHIVE" _encrypted_parts.tar.gz).py"

echo "Розпаковуємо архів $ENCRYPTED_ARCHIVE..."
tar -xzf "$ENCRYPTED_ARCHIVE"

echo "Дешифруємо частини..."
for enc_part in temp_parts/*.enc; do
    dec_part="${enc_part%.enc}"
    openssl rsautl -decrypt -inkey "$PRIVATE_KEY" -in "$enc_part" -out "$dec_part"
done

echo "Об'єднуємо частини у архів reconstructed_archive.tar.gz..."
cat temp_parts/part_* > reconstructed_archive.tar.gz

echo "Перевіряємо архів reconstructed_archive.tar.gz..."
tar -tzf reconstructed_archive.tar.gz

echo "Розпаковуємо архів для відновлення файлів..."
tar -xzf reconstructed_archive.tar.gz

echo "Файл успішно відновлено!"
mv "$(basename "$ENCRYPTED_ARCHIVE" _encrypted_parts.tar.gz)" "$DECRYPTED_FILE"

# Очистка
rm -rf temp_parts/ reconstructed_archive.tar.gz

echo "Дешифрований файл: $DECRYPTED_FILE"
