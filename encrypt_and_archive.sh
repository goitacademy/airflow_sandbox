#!/bin/bash

# Перевірка аргументів
if [ $# -ne 2 ]; then
    echo "Використання: $0 <файл_або_директорія> <публічний_ключ>"
    exit 1
fi

INPUT_PATH="$1"
PUBLIC_KEY="$2"
BASE_NAME=$(basename "$INPUT_PATH")
OUTPUT_ARCHIVE="${BASE_NAME}_encrypted_parts.tar.gz"
TEMP_DIR="temp_parts"
TEMP_ARCHIVE="temp_archive.tar.gz"

# Перевірка існування вхідного файлу/директорії
if [ ! -e "$INPUT_PATH" ]; then
    echo "Помилка: шлях $INPUT_PATH не знайдено"
    exit 1
fi

# Перевірка існування публічного ключа
if [ ! -f "$PUBLIC_KEY" ]; then
    echo "Помилка: файл ключа $PUBLIC_KEY не знайдено"
    exit 1
fi

# Створення тимчасової директорії
echo "Створення тимчасової директорії..."
mkdir -p "$TEMP_DIR"

# Архівування файлу або директорії
if [ -d "$INPUT_PATH" ]; then
    echo "Архівування директорії $INPUT_PATH у $TEMP_ARCHIVE..."
else
    echo "Архівування файлу $INPUT_PATH у $TEMP_ARCHIVE..."
fi
tar -czf "$TEMP_ARCHIVE" "$INPUT_PATH"

# Перевірка створення архіву
if [ $? -ne 0 ]; then
    echo "Помилка: не вдалося створити архів"
    rm -rf "$TEMP_DIR"
    exit 1
fi

# Розбиття архіву на частини
echo "Розбиваємо архів на частини по 128 байтів..."
split -b 128 "$TEMP_ARCHIVE" "${TEMP_DIR}/part_"

# Перевірка, чи створені частини
if [ ! "$(ls -A $TEMP_DIR)" ]; then
    echo "Помилка: не вдалося створити частини файлу"
    rm -rf "$TEMP_DIR" "$TEMP_ARCHIVE"
    exit 1
fi

# Шифрування частин
echo "Шифрування частин..."
for part in "${TEMP_DIR}"/part_*; do
    if [ -f "$part" ]; then
        echo "  Шифрування $(basename "$part")..."
        openssl pkeyutl -encrypt -pubin -inkey "$PUBLIC_KEY" -in "$part" -out "${part}.enc"

        # Перевірка успішності шифрування
        if [ $? -ne 0 ]; then
            echo "Помилка при шифруванні $part"
            rm -rf "$TEMP_DIR" "$TEMP_ARCHIVE"
            exit 1
        fi

        # Видалення незашифрованої частини
        rm "$part"
    fi
done

# Створення фінального архіву
echo "Створення фінального архіву з зашифрованими частинами..."
tar -czf "$OUTPUT_ARCHIVE" -C "$TEMP_DIR" .

# Перевірка створення архіву
if [ $? -ne 0 ]; then
    echo "Помилка при створенні фінального архіву"
    rm -rf "$TEMP_DIR" "$TEMP_ARCHIVE"
    exit 1
fi

# Очищення тимчасових файлів
echo "Очищення тимчасових файлів..."
rm -rf "$TEMP_DIR" "$TEMP_ARCHIVE"

echo "✓ Шифрування завершено успішно!"
echo "✓ Фінальний архів: $OUTPUT_ARCHIVE"
echo "✓ Кількість зашифрованих частин: $(tar -tzf "$OUTPUT_ARCHIVE" | wc -l)"
