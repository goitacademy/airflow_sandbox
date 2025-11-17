#!/bin/bash

# Перевірка аргументів
if [ $# -ne 2 ]; then
    echo "Використання: $0 <файл_для_шифрування> <публічний_ключ>"
    exit 1
fi

INPUT_FILE="$1"
PUBLIC_KEY="$2"
BASE_NAME=$(basename "$INPUT_FILE")
OUTPUT_ARCHIVE="${BASE_NAME}_encrypted_parts.tar.gz"
TEMP_DIR="temp_parts"

# Перевірка існування вхідного файлу
if [ ! -f "$INPUT_FILE" ]; then
    echo "Помилка: файл $INPUT_FILE не знайдено"
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

# Архівування файлу
echo "Архівування файлу $INPUT_FILE у $INPUT_FILE.tar.gz..."
tar -czf "$INPUT_FILE.tar.gz" "$INPUT_FILE"

# Розбиття архіву на частини
echo "Розбиваємо архів на частини по 128 байтів..."
split -b 128 "$INPUT_FILE.tar.gz" "${TEMP_DIR}/part_"

# Перевірка, чи створені частини
if [ ! "$(ls -A $TEMP_DIR)" ]; then
    echo "Помилка: не вдалося створити частини файлу"
    rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"
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
            rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"
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
    rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"
    exit 1
fi

# Очищення тимчасових файлів
echo "Очищення тимчасових файлів..."
rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"

echo "✓ Шифрування завершено успішно!"
echo "✓ Фінальний архів: $OUTPUT_ARCHIVE"
echo "✓ Кількість зашифрованих частин: $(tar -tzf "$OUTPUT_ARCHIVE" | wc -l)"#!/bin/bash

# Перевірка аргументів
if [ $# -ne 2 ]; then
    echo "Використання: $0 <файл_для_шифрування> <публічний_ключ>"
    exit 1
fi

INPUT_FILE="$1"
PUBLIC_KEY="$2"
BASE_NAME=$(basename "$INPUT_FILE")
OUTPUT_ARCHIVE="${BASE_NAME}_encrypted_parts.tar.gz"
TEMP_DIR="temp_parts"

# Перевірка існування вхідного файлу
if [ ! -f "$INPUT_FILE" ]; then
    echo "Помилка: файл $INPUT_FILE не знайдено"
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

# Архівування файлу
echo "Архівування файлу $INPUT_FILE у $INPUT_FILE.tar.gz..."
tar -czf "$INPUT_FILE.tar.gz" "$INPUT_FILE"

# Розбиття архіву на частини
echo "Розбиваємо архів на частини по 128 байтів..."
split -b 128 "$INPUT_FILE.tar.gz" "${TEMP_DIR}/part_"

# Перевірка, чи створені частини
if [ ! "$(ls -A $TEMP_DIR)" ]; then
    echo "Помилка: не вдалося створити частини файлу"
    rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"
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
            rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"
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
    rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"
    exit 1
fi

# Очищення тимчасових файлів
echo "Очищення тимчасових файлів..."
rm -rf "$TEMP_DIR" "$INPUT_FILE.tar.gz"

echo "✓ Шифрування завершено успішно!"
echo "✓ Фінальний архів: $OUTPUT_ARCHIVE"
echo "✓ Кількість зашифрованих частин: $(tar -tzf "$OUTPUT_ARCHIVE" | wc -l)"
