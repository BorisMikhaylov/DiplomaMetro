#!/bin/bash
# Загружает данные из приватного репозитория DiplomaData.
# Требует доступа к https://github.com/BorisMikhaylov/DiplomaData
#
# Использование:
#   bash setup_data.sh

set -e

DATA_REPO="https://github.com/BorisMikhaylov/DiplomaData.git"
DATA_DIR="data"

if [ -d "$DATA_DIR/.git" ]; then
  echo "Данные уже загружены. Обновляем..."
  git -C "$DATA_DIR" pull
else
  echo "Клонирую данные из $DATA_REPO..."
  git clone "$DATA_REPO" "$DATA_DIR"
fi

echo ""
echo "Готово. Структура data/:"
ls "$DATA_DIR"
echo ""
echo "ВАЖНО: PASS_ALL_202503242210.csv (12 ГБ) хранится отдельно."
echo "Положи его вручную в data/pass_10-160324/"
