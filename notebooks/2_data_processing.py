# Функция для парсинга XML
def parse_xml_to_df(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        data = []
        for row in root.findall('row'):
            data.append(row.attrib)
        df = pd.DataFrame(data)
        return df
    except Exception as e:
        print(f"Ошибка при обработке {file_path}: {e}")
        return pd.DataFrame()

# Функция для подготовки DataFrame
def prepare_df_auto(df, archive_version):
    df = df.copy()
    df.columns = [col.strip() for col in df.columns]

    if "Id" in df.columns:
        df.rename(columns={"Id": "OriginalId"}, inplace=True)
    df["OriginalId"] = df["OriginalId"].astype(int)

    date_columns = [col for col in df.columns if "Date" in col]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')

    df["ArchiveVersion"] = archive_version
    return df

# Настройки подключения к БД                                                                     -- Вставить свои параметры
DB_CONFIG = {
    'user': 'postgres',
    'password': '___',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres'
}
engine = create_engine(f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["database"]}')

# Словарь для преобразования типов
SQLALCHEMY_TYPE_MAP = {
    'integer': Integer(),
    'smallint': SmallInteger(),
    'character varying': String(),
    'text': Text(),
    'timestamp without time zone': DateTime(),
    'int': Integer(),
    'nvarchar': String(),
    'datetime': DateTime(),
    'tinyint': SmallInteger(),
    'bit': Boolean(),
    'date': Date(),
    'uniqueidentifier': UUID(),
    'varchar(32)': String(32)
}

def get_table_schema(table_name):
    """Получить схему таблицы из БД"""
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '{table_name.lower()}';
    """
    return pd.read_sql(query, engine)

def prepare_data_types(df, table_name):
    """Приведение типов данных в соответствии со схемой таблицы"""
    df_schema = get_table_schema(table_name)
    dtype_dict = {row['column_name']: row['data_type'] for _, row in df_schema.iterrows()}

    for col, db_type in dtype_dict.items():
        if col in df.columns:
            if db_type in ['integer', 'smallint', 'int']:
                df[col] = df[col].astype('Int64')
            elif db_type in ['timestamp without time zone', 'datetime']:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            elif db_type == 'bit':
                df[col] = df[col].astype(bool)
            elif db_type in ['text', 'character varying', 'nvarchar', 'varchar(32)']:
                df[col] = df[col].astype(str).where(df[col].notna(), None)
            elif db_type == 'date':
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
            elif db_type == 'uniqueidentifier':
                df[col] = df[col].astype(str)
    return df

def get_sqlalchemy_dtype_dict(table_name):
    """Получить словарь типов SQLAlchemy для таблицы"""
    df_schema = get_table_schema(table_name)
    return {
        col: SQLALCHEMY_TYPE_MAP[db_type]
        for col, db_type in zip(df_schema['column_name'], df_schema['data_type'])
        if db_type in SQLALCHEMY_TYPE_MAP
    }

def load_data_to_db(df, table_name):
    """Загрузка данных в таблицу БД"""
    sqlalchemy_dtype_dict = get_sqlalchemy_dtype_dict(table_name)
    df.to_sql(
        name=table_name.lower(),
        con=engine,
        if_exists='append',
        index=False,
        dtype=sqlalchemy_dtype_dict
    )

# Основные директории
BASE_DIR = r"C:\ваш\путь\к_проекту"
ARCHIVE_DIRS = {
    'meta': os.path.join(BASE_DIR, "dba.meta.stackexchange.com"),
    'main': os.path.join(BASE_DIR, "dba.stackexchange.com")
}

def process_table(table_name):
    """Обработка таблицы с точным сохранением оригинальной логики"""
    print(f"\nНачата обработка таблицы {table_name}...")

    # Загрузка данных из XML
    file1 = os.path.join(ARCHIVE_DIRS['meta'], f"{table_name}.xml")
    file2 = os.path.join(ARCHIVE_DIRS['main'], f"{table_name}.xml")

    df1 = parse_xml_to_df(file1)
    df2 = parse_xml_to_df(file2)

    # Подготовка данных
    df1_prepared = prepare_df_auto(df1, 1)
    df2_prepared = prepare_df_auto(df2, 2)
    df_combined = pd.concat([df1_prepared, df2_prepared], ignore_index=True)

    # Обработка для конкретных таблиц
    if table_name.lower() == 'users':
        # Для Users просто преобразуем типы
        df_combined.columns = [col.lower() for col in df_combined.columns]

    elif table_name.lower() == 'badges':
        # Обработка Badges
        df_users_db = pd.read_sql("SELECT id, originalid, archiveversion FROM users", engine)
        df_users_db.columns = [col.lower() for col in df_users_db.columns]

        df_combined['UserId'] = df_combined['UserId'].astype('Int64')
        df_combined.columns = [col.lower() for col in df_combined.columns]

        df_combined = df_combined.merge(
            df_users_db,
            left_on=['userid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['userid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['originalid_x', 'archiveversion', 'id', 'originalid_y'], inplace=True, errors='ignore')

    elif table_name.lower() == 'posts':
        # Обработка Posts
        df_users_db = pd.read_sql("SELECT id, originalid, archiveversion FROM users", engine)
        df_users_db.columns = [col.lower() for col in df_users_db.columns]

        for col in df_combined.columns:
            if 'Id' in col and df_combined[col].dtype == 'object':
                df_combined[col] = df_combined[col].astype('Int64')

        df_combined.columns = [col.lower() for col in df_combined.columns]

        # Обработка owneruserid
        df_combined = df_combined.merge(
            df_users_db,
            left_on=['owneruserid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['owneruserid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['id', 'originalid_y'], inplace=True)
        df_combined.rename(columns={'originalid_x': 'originalid'}, inplace=True)

        # Обработка lasteditoruserid
        df_combined = df_combined.merge(
            df_users_db,
            left_on=['lasteditoruserid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['lasteditoruserid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['id', 'originalid_y'], inplace=True)
        df_combined.rename(columns={'originalid_x': 'originalid'}, inplace=True)

        # Исправление строки WHERE AcceptedAnswerId = 338217 и не имеет внешнего Id
        if 'acceptedanswerid' in df_combined.columns:
            df_combined.loc[df_combined['acceptedanswerid'] == 338217, 'acceptedanswerid'] = None

    elif table_name.lower() == 'tags':
        # Обработка Tags
        df_posts_db = pd.read_sql("SELECT id, originalid, archiveversion FROM posts", engine)
        df_posts_db.columns = [col.lower() for col in df_posts_db.columns]

        for col in df_combined.columns:
            if 'Id' in col and df_combined[col].dtype == 'object':
                df_combined[col] = df_combined[col].astype('Int64')

        df_combined.columns = [col.lower() for col in df_combined.columns]

        # Обработка excerptpostid
        df_combined = df_combined.merge(
            df_posts_db,
            left_on=['excerptpostid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['excerptpostid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['id', 'originalid_y'], inplace=True)
        df_combined.rename(columns={'originalid_x': 'originalid'}, inplace=True)

        # Обработка wikipostid
        df_combined = df_combined.merge(
            df_posts_db,
            left_on=['wikipostid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['wikipostid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['originalid_x', 'archiveversion', 'id', 'originalid_y'], inplace=True)

    elif table_name.lower() == 'postlinks':
        # Обработка PostLinks
        df_posts_db = pd.read_sql("SELECT id, originalid, archiveversion FROM posts", engine)
        df_posts_db.columns = [col.lower() for col in df_posts_db.columns]

        for col in df_combined.columns:
            if 'Id' in col and df_combined[col].dtype == 'object':
                df_combined[col] = df_combined[col].astype('Int64')

        df_combined.columns = [col.lower() for col in df_combined.columns]

        # Обработка postid
        df_combined = df_combined.merge(
            df_posts_db,
            left_on=['postid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['postid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['id', 'originalid_y'], inplace=True)
        df_combined.rename(columns={'originalid_x': 'originalid'}, inplace=True)

        # Обработка relatedpostid
        df_combined = df_combined.merge(
            df_posts_db,
            left_on=['relatedpostid', 'archiveversion'],
            right_on=['originalid', 'archiveversion'],
            how='left'
        )
        df_combined['relatedpostid'] = df_combined['id'].astype('Int64')
        df_combined.drop(columns=['originalid_x', 'archiveversion', 'id', 'originalid_y'], inplace=True)

    elif table_name.lower() in ['posthistory', 'comments', 'votes']:
        # Общая логика для PostHistory, Comments и Votes
        df_users_db = pd.read_sql("SELECT id, originalid, archiveversion FROM users", engine)
        df_users_db.columns = [col.lower() for col in df_users_db.columns]

        df_posts_db = pd.read_sql("SELECT id, originalid, archiveversion FROM posts", engine)
        df_posts_db.columns = [col.lower() for col in df_posts_db.columns]

        for col in df_combined.columns:
            if 'Id' in col and df_combined[col].dtype == 'object':
                df_combined[col] = df_combined[col].astype('Int64')

        df_combined.columns = [col.lower() for col in df_combined.columns]

        # Обработка userid
        if 'userid' in df_combined.columns:
            df_combined = df_combined.merge(
                df_users_db,
                left_on=['userid', 'archiveversion'],
                right_on=['originalid', 'archiveversion'],
                how='left'
            )
            df_combined['userid'] = df_combined['id'].astype('Int64')
            df_combined.drop(columns=['id', 'originalid_y'], inplace=True)
            df_combined.rename(columns={'originalid_x': 'originalid'}, inplace=True)

        # Обработка postid
        if 'postid' in df_combined.columns:
            df_combined = df_combined.merge(
                df_posts_db,
                left_on=['postid', 'archiveversion'],
                right_on=['originalid', 'archiveversion'],
                how='left'
            )
            df_combined['postid'] = df_combined['id'].astype('Int64')
            df_combined.drop(columns=['originalid_x', 'archiveversion', 'id', 'originalid_y'], inplace=True)

    # Приведение типов данных
    df_combined = prepare_data_types(df_combined, table_name)

    # Загрузка в БД
    load_data_to_db(df_combined, table_name)
    print(f"Таблица {table_name} успешно обработана и загружена в БД")

    return df_combined

# Запуск обработки всех таблиц
if __name__ == "__main__":
    # Порядок обработки важен из-за зависимостей между таблицами
    tables_to_process = [
        'Users',    # Должна быть первой, так как другие таблицы ссылаются на Users
        'Badges',   # Зависит от Users
        'Posts',    # Зависит от Users
        'Tags',     # Зависит от Posts
        'PostLinks', # Зависит от Posts
        'PostHistory', # Зависит от Users и Posts
        'Comments',  # Зависит от Users и Posts
        'Votes'     # Зависит от Users и Posts
    ]

    for table in tables_to_process:
        try:
            process_table(table)
        except Exception as e:
            print(f"Ошибка при обработке таблицы {table}: {e}")

    print("\nВсе таблицы успешно обработаны и загружены в БД")
