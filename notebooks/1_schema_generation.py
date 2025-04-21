# Импорт всех необходимых библиотек
# !pip install pandas openpyxl lxml matplotlib networkx graphviz sqlalchemy psycopg2-binary
import pandas as pd
import re
import os
import xml.etree.ElementTree as ET
from sqlalchemy import create_engine, Integer, SmallInteger, String, Text, DateTime, Date, Boolean, Float
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import create_engine
from graphviz import Digraph
import graphviz

# Парсинг Excel схемы
excel_path = r"C:\ваш\путь\к_проекту\tables_specification.xlsx" # Заменить на путь, куда загружен файл из data
xls = pd.read_excel(excel_path, sheet_name=None, header=None)

parsed_rows = []

current_table = None
parsing_fields = False
field_definitions = []
primary_keys = set()
foreign_keys_map = {}

for sheet_name, df in xls.items():
    for idx, row in df.iterrows():
        col_a = str(row[0]) if pd.notna(row[0]) else ""
        col_b = str(row[1]) if pd.notna(row[1]) else ""
        col_c = str(row[2]) if pd.notna(row[2]) else ""

        if col_a.startswith("Table "):
            if current_table and field_definitions:
                for field in field_definitions:
                    field_name = field["Field Name"]
                    parsed_rows.append({
                        "Table": current_table,
                        "Field Name": field_name,
                        "Data Type": field["Data Type"],
                        "Indexed": field["Indexed"],
                        "Primary Key": field_name in primary_keys,
                        "Foreign Key": foreign_keys_map.get(field_name)
                    })
            current_table = col_a.replace("Table ", "").strip()
            parsing_fields = False
            field_definitions = []
            primary_keys = set()
            foreign_keys_map = {}
            continue

        if "Field Name" in col_b:
            parsing_fields = True
            continue

        if col_a.strip() in {"Indexes", "Foreign Keys"}:
            parsing_fields = False
            continue

        if col_b.strip() == "Primary" and col_c.startswith("ON "):
            pk = col_c.replace("ON", "").strip()
            primary_keys.add(pk)
            continue

        if col_b.startswith("Fk_") and "ref" in col_c:
            match = re.match(r"\(\s*(\w+)\s*\)\s*ref\s+(\w+)\s*\((\w+)\)", col_c)
            if match:
                local_col, ref_table, ref_col = match.groups()
                foreign_keys_map[local_col] = f"{ref_table}({ref_col})"
            continue

        if parsing_fields and col_b and col_c:
            field_definitions.append({
                "Field Name": col_b,
                "Data Type": col_c,
                "Indexed": col_a.strip() == "*"
            })

if current_table and field_definitions:
    for field in field_definitions:
        field_name = field["Field Name"]
        parsed_rows.append({
            "Table": current_table,
            "Field Name": field_name,
            "Data Type": field["Data Type"],
            "Indexed": field["Indexed"],
            "Primary Key": field_name in primary_keys,
            "Foreign Key": foreign_keys_map.get(field_name)
        })

excel_schema_df = pd.DataFrame(parsed_rows)

# Схема XML
xml_dirs = [
    r"C:\ваш\путь\к_проекту\dba.meta.stackexchange.com", # Заменить на путь, куда загружен и распакован файл из data
    r"C:\ваш\путь\к_проекту\dba.stackexchange.com" # Заменить на путь, куда загружен и распакован файл из data
]

def parse_xml_to_df(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = [row.attrib for row in root.findall('row')]
        return pd.DataFrame(data)
    except Exception as e:
        print(f"Ошибка при обработке {file_path}: {e}")
        return pd.DataFrame()

xml_tables_data = {}
xml_schema = {}

for xml_dir in xml_dirs:
    for filename in os.listdir(xml_dir):
        if filename.endswith(".xml"):
            table_name = os.path.splitext(filename)[0]
            file_path = os.path.join(xml_dir, filename)
            df = parse_xml_to_df(file_path)
            if table_name not in xml_tables_data:
                xml_tables_data[table_name] = df
            else:
                xml_tables_data[table_name] = pd.concat([xml_tables_data[table_name], df], ignore_index=True)

for table_name, df in xml_tables_data.items():
    xml_schema[table_name] = list(df.columns)

xml_rows = []
for table, cols in xml_schema.items():
    for col in cols:
        xml_rows.append({"Table": table, "Field Name": col})
xml_schema_df = pd.DataFrame(xml_rows)

# Парсинг basw.txt
sql_path = r"C:\ваш\путь\к_проекту\sql_queries.txt" # Заменить на путь, куда загружен файл из data
with open(sql_path, encoding='utf-8') as f:
    sql_text = f.read()

# Убираем комментарии
sql_text = re.sub(r'--.*?$', '', sql_text, flags=re.MULTILINE)

# Находим все определения таблиц
table_defs = re.findall(r'CREATE TABLE\s+(\w+)\s*\((.*?)\);', sql_text, flags=re.DOTALL | re.IGNORECASE)

rows = []
for table_name, body in table_defs:
    fields = [line.strip().rstrip(',') for line in body.split('\n') if line.strip()]
    primary_keys = []

    # Обработка первичных ключей
    for line in fields:
        if line.upper().startswith("PRIMARY KEY"):
            pk_match = re.findall(r'\((.*?)\)', line)
            if pk_match:
                primary_keys = [col.strip().strip('"') for col in pk_match[0].split(',')]
            continue

        # Обработка обычных столбцов
        match = re.match(r'(\w+)\s+([\w\(\)]+)(?:\s+(NOT NULL|NULL))?', line, flags=re.IGNORECASE)
        if match:
            col_name, col_type, nullability = match.groups()
            rows.append({
                "Table": table_name,
                "Field Name": col_name,
                "Data Type": col_type,
                "Nullable": nullability != "NOT NULL",
                "Primary Key": col_name in primary_keys,
                "Foreign Key": None,
                "Indexed": False
            })

# Обработка внешних ключей
foreign_keys = re.findall(
    r'ALTER TABLE (\w+)\s+ADD CONSTRAINT \w+\s+FOREIGN KEY \(\s*(\w+)\s*\)\s+REFERENCES (\w+)\s*\(\s*(\w+)\s*\)',
    sql_text, flags=re.IGNORECASE)

for table, field, ref_table, ref_field in foreign_keys:
    for row in rows:
        if row["Table"] == table and row["Field Name"] == field:
            row["Foreign Key"] = f"{ref_table}({ref_field})"

# Обработка индексов
index_defs = re.findall(r'CREATE INDEX ON (\w+)\s*\(\s*(\w+)\s*\)', sql_text, flags=re.IGNORECASE)
for table, field in index_defs:
    for row in rows:
        if row["Table"] == table and row["Field Name"] == field:
            row["Indexed"] = True

# Создание DataFrame
ddl_df = pd.DataFrame(rows)

# Маппинг типов данных из MSSQL в PostgreSQL
def convert_mssql_to_pg(mssql_type: str) -> str:
    mssql_type = mssql_type.strip().lower()
    match = re.match(r'^([a-zA-Z]+)(\([0-9, ]+\))?', mssql_type)
    if not match:
        return 'text'  # запасной вариант

    base_type, size = match.groups()
    size = size or ''

    base_mapping = {
        'int': 'integer',
        'tinyint': 'smallint',
        'smallint': 'smallint',
        'bit': 'boolean',
        'uniqueidentifier': 'uuid',
        'nvarchar': 'varchar',
        'varchar': 'varchar',
        'datetime': 'timestamp',
        'smalldatetime': 'timestamp',
        'date': 'date'
    }

    pg_type = base_mapping.get(base_type, 'text')
    if pg_type in {'varchar', 'char', 'numeric'} and size:
        return f'{pg_type}{size}'
    return pg_type

ddl_df["Data Type"] = ddl_df["Data Type"].apply(convert_mssql_to_pg)

# Сопоставление и создание финальной схемы
excel_schema_df["Table_lc"] = excel_schema_df["Table"].str.lower()
ddl_df["Table_lc"] = ddl_df["Table"].str.lower()
xml_schema_df["Table_lc"] = xml_schema_df["Table"].str.lower()

common_tables = set(excel_schema_df["Table_lc"]).intersection(set(xml_schema_df["Table_lc"]))

ordered_schema = []
for table in sorted(common_tables):
    excel_sub = excel_schema_df[excel_schema_df["Table_lc"] == table]
    xml_sub = xml_schema_df[xml_schema_df["Table_lc"] == table]
    ddl_sub = ddl_df[ddl_df["Table_lc"] == table]

    xml_fields = list(xml_sub["Field Name"])
    excel_fields = list(excel_sub["Field Name"])
    ddl_fields = list(ddl_sub["Field Name"])

    for col in xml_fields:
        base_row = excel_sub[excel_sub["Field Name"] == col]
        if not base_row.empty:
            row = base_row.iloc[0].to_dict()
        else:
            row = {
                "Table": excel_sub["Table"].iloc[0],
                "Field Name": col,
                "Data Type": "varchar" if "id" not in col.lower() else "integer",
                "Indexed": False,
                "Primary Key": False,
                "Foreign Key": None,
                "Nullable": True
            }

        ddl_row = ddl_sub[ddl_sub["Field Name"] == col]
        if not ddl_row.empty:
            row["Data Type"] = ddl_row["Data Type"].iloc[0]
            row["Nullable"] = ddl_row["Nullable"].iloc[0]

        ordered_schema.append(row)

# Создание финальной схемы
final_schema_df = pd.DataFrame(ordered_schema)
final_schema_df = final_schema_df[["Table", "Field Name", "Data Type", "Indexed", "Primary Key", "Foreign Key", "Nullable"]]

# # Создание визуализации:

# df = final_schema_df.copy()

# # Группируем поля по таблицам
# tables = df['Table'].unique()

# dot = Digraph(comment='Full Database Schema', format='png')
# dot.attr(rankdir='LR')  # Горизонтальная схема
# dot.attr('node', shape='plaintext')  # Таблицы как таблички

# # Создаем таблички с полями
# for table in tables:
#     table_df = df[df['Table'] == table]
#     fields = ""
#     for _, row in table_df.iterrows():
#         line = f"{row['Field Name']} : {row['Data Type']}"
#         if row['Primary Key']:
#             line += " [PK]"
#         if row['Foreign Key'] != "None":
#             line += f" [FK → {row['Foreign Key']}]"
#         if row['Indexed']:
#             line += " [IDX]"
#         fields += f"<TR><TD ALIGN='LEFT'>{line}</TD></TR>"

#     table_html = f"""<
#     <TABLE BORDER='1' CELLBORDER='0' CELLSPACING='0'>
#         <TR><TD BGCOLOR='lightgray'><B>{table}</B></TD></TR>
#         {fields}
#     </TABLE>
#     >"""

#     dot.node(table, table_html)

# # Добавим связи между таблицами на основе Foreign Keys
# df_fk = df[df['Foreign Key'].notna() & (df['Foreign Key'] != 'None')]

# for _, row in df_fk.iterrows():
#     from_table = row['Table']
#     fk = row['Foreign Key']

#     try:
#         to_table, _ = fk.strip(")").split("(")
#         to_table = to_table.strip()
#         dot.edge(from_table, to_table, label=row['Field Name'])
#     except Exception as e:
#         print(f"Ошибка в Foreign Key '{fk}': {e}")

# # Сохраняем как PNG и открываем
# dot.render('full_database_schema', cleanup=True)
# dot.view()

# Сначала создаём SQL для создания таблиц
create_table_statements = []
for table, group in final_schema_df.groupby("Table"):
    create_statement = f"CREATE TABLE {table} (\n"
    
    fields = []

    # Добавляем служебные поля вручную для Posts и Users
    if table in ['Posts', 'Users']:
        fields.append("  OriginalId INTEGER NOT NULL")  # ID из XML
        fields.append("  ArchiveVersion SMALLINT NOT NULL")  # Версия архива

    for _, row in group.iterrows():
        if row["Field Name"] in ['OriginalId', 'ArchiveVersion']:
            continue  # чтобы не дублировались

        # Id -> SERIAL PRIMARY KEY
        if row["Field Name"] == 'Id':
            field_def = f"  {row['Field Name']} SERIAL PRIMARY KEY"
        # IsRequired / IsModeratorOnly -> boolean
        elif row["Field Name"] in ['IsRequired', 'IsModeratorOnly']:
            field_def = f"  {row['Field Name']} boolean"
            if not row["Nullable"]:
                field_def += " NOT NULL"
        else:
            field_def = f"  {row['Field Name']} {row['Data Type']}"
            if not row["Nullable"]:
                field_def += " NOT NULL"

        fields.append(field_def)
    
    create_statement += ",\n".join(fields) + "\n);"
    create_table_statements.append(create_statement)

# Получаем список допустимых таблиц из xml_schema_df
valid_fk_tables = set(xml_schema_df["Table"].str.lower())

# Блок обновления ID (добавлен между созданием таблиц и FK)
update_statements = [
    "\n-- Обновляем AcceptedAnswerId",
    "UPDATE Posts p",
    "SET AcceptedAnswerId = sub.id",
    "FROM (",
    "    SELECT OriginalId, ArchiveVersion, Id",
    "    FROM Posts",
    ") sub",
    "WHERE p.AcceptedAnswerId = sub.OriginalId",
    "  AND p.ArchiveVersion = sub.ArchiveVersion;",
    "",
    "-- Обновляем ParentId",
    "UPDATE Posts p",
    "SET ParentId = sub.id",
    "FROM (",
    "    SELECT OriginalId, ArchiveVersion, Id",
    "    FROM Posts",
    ") sub",
    "WHERE p.ParentId = sub.OriginalId",
    "  AND p.ArchiveVersion = sub.ArchiveVersion;\n"
]

# Далее добавляем внешние ключи только если таблица назначения есть в xml_schema_df
alter_table_statements = []
for table, group in final_schema_df.groupby("Table"):
    for _, row in group.iterrows():
        if row["Foreign Key"]:
            foreign_table = row["Foreign Key"].split("(")[0].strip().lower()
            if foreign_table in valid_fk_tables:
                alter_statement = f"ALTER TABLE {table} ADD CONSTRAINT fk_{table}_{row['Field Name']} FOREIGN KEY ({row['Field Name']}) REFERENCES {row['Foreign Key']};"
                alter_table_statements.append(alter_statement)

# Добавляем индексы на основе столбца "Indexed", исключая primary key
index_statements = []
for table, group in final_schema_df.groupby("Table"):
    for _, row in group.iterrows():
        if row.get("Indexed", False) and not row.get("Primary Key", False):
            index_statement = f"CREATE INDEX ON {table}({row['Field Name']});"
            index_statements.append(index_statement)

# Объединяем все SQL-выражения в правильном порядке
sql_statements = (
    create_table_statements + 
    ["\n".join(update_statements)] +  # Добавляем блок обновлений
    alter_table_statements + 
    index_statements
)

# Финальный SQL
final_sql = "\n".join(sql_statements)

# Сохраняем SQL в файл
# output_path = "C:\ваш\путь\к_проекту\generated_schema.sql"

# with open(output_path, "w", encoding="utf-8") as f:
#     f.write(final_sql)

# print(f"\nSQL-скрипт сохранён в файл:\n{output_path}")
