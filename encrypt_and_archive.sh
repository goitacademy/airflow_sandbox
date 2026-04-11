#!/bin/bash
if [ $# -ne 2 ]; then
    echo "Використання: $0 <файл> <публічний_ключ>"
    exit 1
fi

INPUT_PATH="$1"
PUBLIC_KEY="$2"
BASE_NAME=$(basename "$INPUT_PATH")
OUTPUT_ARCHIVE="${BASE_NAME}_encrypted_parts.tar.gz"
TEMP_DIR="temp_parts"

mkdir -p "$TEMP_DIR"

echo "Розбиваємо файл на частини по 128 байтів..."
split -b 128 "$INPUT_PATH" "${TEMP_DIR}/part_"

echo "Шифрування частин..."
for part in "${TEMP_DIR}"/part_*; do
    echo "  Шифрування $(basename "$part")..."
    openssl rsautl -encrypt -pubin -inkey "$PUBLIC_KEY" -in "$part" -out "${part}.enc"
    rm "$part"
done

echo "Створення фінального архіву..."
tar -czf "$OUTPUT_ARCHIVE" "$TEMP_DIR"
rm -rf "$TEMP_DIR"

echo "✓ Готово: $OUTPUT_ARCHIVE"
echo "✓ Частин: $(tar -tzf "$OUTPUT_ARCHIVE" | grep '\.enc$' | wc -l)"