param(
    [Parameter(Mandatory = $true)]
    [string]$InputFile,

    [Parameter(Mandatory = $true)]
    [string]$PublicKey
)

# Витягуємо ім'я файлу без шляху
$BaseName = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
$ArchiveName = "$BaseName.tar.gz"
$PartSize = 245

# Створюємо тимчасову папку
$TempDir = "temp_parts"
New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

# Крок 1: Архівування файлу з іменем архіву, що відповідає імені файлу
Write-Output "Архівування файлу $InputFile у $ArchiveName..."
& tar -czf $ArchiveName $InputFile

# Крок 2: Розбиваємо архів на частини по $PartSize байтів
Write-Output "Розбиваємо архів на частини по $PartSize байтів..."

$fs = [System.IO.File]::OpenRead($ArchiveName)
$buffer = New-Object byte[] $PartSize

# Алфавітне найменування (aa, ab, ac, ...)
$alphabet = 'abcdefghijklmnopqrstuvwxyz'
$index = 0
while (($bytesRead = $fs.Read($buffer, 0, $PartSize)) -gt 0) {
    $first = $alphabet[[Math]::Floor($index / 26) % 26]
    $second = $alphabet[$index % 26]
    $alphaIndex = "$first$second"
    $partFile = Join-Path $TempDir ("part_$alphaIndex")
    [System.IO.File]::WriteAllBytes($partFile, $buffer[0..($bytesRead-1)])
    $index++
}
$fs.Close()

# Крок 3: Шифруємо кожну частину публічним ключем
Write-Output "Шифрування частин..."

foreach ($part in Get-ChildItem -Path $TempDir -Filter "part_*") {
    $encryptedPart = "$($part.FullName).enc"
    & openssl rsautl -encrypt -inkey $PublicKey -pubin -in $part.FullName -out $encryptedPart
    Remove-Item $part.FullName  # Видаляємо нешифровану частину після шифрування
}

# Крок 4: Архівуємо всі зашифровані частини в один архів формату .tar.gz
Write-Output "Створення фінального архіву з зашифрованими частинами..."
& tar -czf "${BaseName}_encrypted_parts.tar.gz" -C $TempDir .

# Крок 5: Очищення тимчасових файлів
Write-Output "Очищення тимчасових файлів..."
Remove-Item -Recurse -Force $TempDir
Remove-Item $ArchiveName

Write-Output "Шифрування завершено. Фінальний архів: ${BaseName}_encrypted_parts.tar.gz"

