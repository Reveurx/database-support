## Данные проекта

Эта папка содержит ссылки на исходные данные и информацию для работы с ними.

## Загрузка файлов
Ссылка 1: Основные файлы (обязательные)
**[Скачать Excel и TXT](https://cloud.mail.ru/public/qedq/5mbsZZaBK)**  
Содержит:
- `tables_specification.xlsx` - полное описание таблиц с индексами (копипаст с [сайта схемы](https://sedeschema.github.io/))
- `sql_queries.txt` - SQL-код с типами данных и комментариями автора схемы БД StackExchange для MSSQL

*[Ссылка на оригинальный репозиторий -](https://github.com/leerssej/SEDESchema/blob/master/info_schema_create_tables.sql)*
Найден, при изучении схемы БД StackExchange по ссылке в задании.

Ссылка 2: Исходные архивы (требуют распаковки)
**[Скачать архивы с данными](https://archive.org/download/stackexchange)**  
Содержит:
- `dba.stackexchange.com.7z` - первый набор данных
- `dba.meta.stackexchange.com.7z` - второй набор данных

## Настройка окружения
1. **Скачайте обе части**:
   - По первой ссылке - Excel и TXT файлы
   - По второй ссылке - ZIP-архивы

2. **Распакуйте архивы** в отдельную папку

3. **Соберите все файлы вместе**:
   ```
   /ваша_папка_проекта/
   └── data/
       ├── tables_specification.xlsx   (из Ссылки 1)
       ├── sql_queries.txt            (из Ссылки 1)
       ├── data_from_archive1/        (распаковано из archive1.zip)
       └── data_from_archive2/        (распаковано из archive2.zip)
   ```
