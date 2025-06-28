#!/usr/bin/env bash

# Перевірка параметрів
if [ $# -ne 2 ]; then
  echo "Використання: $0 <файл_для_шифрування> <публічний_ключ>"
  exit 1
fi

INPUT_FILE=$1
PUBLIC_KEY=$2

# Витягуємо ім'я файлу без шляху
BASENAME=$(basename "$INPUT_FILE")
ARCHIVE_NAME="${BASENAME}.tar.gz"
PART_SIZE=245

# Створюємо тимчасову папку
TEMP_DIR="temp_parts"
mkdir -p "$TEMP_DIR"

# Крок 1: Архівування файлу з іменем архіву, що відповідає імені файлу
echo "Архівування файлу $INPUT_FILE у $ARCHIVE_NAME..."
tar -czf "$ARCHIVE_NAME" "$INPUT_FILE"

# Крок 2: Розбиваємо архів на частини по $PART_SIZE байтів
echo "Розбиваємо архів на частини по $PART_SIZE байтів..."
split -b "$PART_SIZE" "$ARCHIVE_NAME" "$TEMP_DIR/part_"

# Крок 3: Шифруємо кожну частину публічним ключем
echo "Шифрування частин..."
for part in "$TEMP_DIR"/part_*; do
  openssl rsautl -encrypt -inkey "$PUBLIC_KEY" -pubin -in "$part" -out "${part}.enc"
  rm "$part"  # Видаляємо нешифровану частину після шифрування
done

# Крок 4: Архівуємо всі зашифровані частини в один архів
echo "Створення фінального архіву з зашифрованими частинами..."
tar -czf "${BASENAME}_encrypted_parts.tar.gz" -C "$TEMP_DIR" .

# Крок 5: Очищення тимчасових файлів
echo "Очищення тимчасових файлів..."
rm -r "$TEMP_DIR"
rm "$ARCHIVE_NAME"

echo "Шифрування завершено. Фінальний архів: ${BASENAME}_encrypted_parts.tar.gz"

cp ${BASENAME}_encrypted_parts.tar.gz ./encrypted_file/${BASENAME}_encrypted_parts.tar.gz

