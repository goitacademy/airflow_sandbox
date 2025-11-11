#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Використання: $0 <файл_для_шифрування> <публічний_ключ>"
    exit 1
fi

INPUT_FILE="$1"
PUBLIC_KEY="$2"
BASE_NAME=$(basename "$INPUT_FILE")
OUTPUT_ARCHIVE="${BASE_NAME}_encrypted_parts.tar.gz"

echo "Архівування файлу $INPUT_FILE у $INPUT_FILE.tar.gz..."
tar -czf "$INPUT_FILE.tar.gz" "$INPUT_FILE"

echo "Розбиваємо архів на частини по 2K байтів..."
split -b 2K "$INPUT_FILE.tar.gz" "temp_parts/part_"

echo "Шифрування частин..."
for part in temp_parts/part_*; do
    openssl rsautl -encrypt -inkey "$PUBLIC_KEY" -pubin -in "$part" -out "${part}.enc"
done

echo "Створення фінального архіву з зашифрованими частинами..."
tar -czf "$OUTPUT_ARCHIVE" temp_parts/*.enc

echo "Очищення тимчасових файлів"
rm -rf temp_parts/ "$INPUT_FILE.tar.gz"

echo "Шифрування завершено, фінальний архів: $OUTPUT_ARCHIVE"
