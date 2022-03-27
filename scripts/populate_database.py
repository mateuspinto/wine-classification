import json
import time

from sqlalchemy import ForeignKey, create_engine, MetaData, Table, Column, INT, VARCHAR, DATE, REAL, SMALLINT
from sqlalchemy_utils import database_exists, create_database, drop_database
import psycopg2


def load_csv_in_database(DB_NAME: str, DB_USER: str, DB_PASSWORD: str, DB_IP: str, DB_PORT: int, TABLE_NAMES) -> None:
    START_TIME = time.time()

    with psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_IP, port=DB_PORT) as DB_CON, DB_CON.cursor() as CUR:
        for TABLE_NAME in TABLE_NAMES:
            with open(f'data/enriched/{TABLE_NAME}.csv', 'r', newline='', encoding='utf-8') as FILE:
                CUR.copy_from(FILE, TABLE_NAME, sep=',')
        CUR.execute("ANALYZE")
        DB_CON.commit()

    print(f'Loaded CSVs into {DB_NAME} database in {round(time.time() - START_TIME, 2)} seconds')


def create_df_schema(DB_NAME: str, DB_USER: str, DB_PASSWORD: str, DB_IP: str, DB_PORT: int) -> None:
    START_TIME = time.time()

    DB_ENGINE = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_IP}:{DB_PORT}/{DB_NAME}', future=True)

    if database_exists(DB_ENGINE.url):
        drop_database(DB_ENGINE.url)

    create_database(DB_ENGINE.url)

    DB_METADATA = MetaData(DB_ENGINE)

    TB_WINE = Table('wines', DB_METADATA,
                    Column('id', INT, primary_key=True),
                    Column('name', VARCHAR),
                    Column('is_red', SMALLINT),
                    Column('fixed_acidity', REAL),
                    Column('volatile_acidity', REAL),
                    Column('citric_acid', REAL),
                    Column('residual_sugar', REAL),
                    Column('chlorides', REAL),
                    Column('free_sulfur_dioxide', REAL),
                    Column('total_sulfur_dioxide', REAL),
                    Column('density', REAL),
                    Column('ph', REAL),
                    Column('sulphates', REAL),
                    Column('alcohol', REAL),
                    Column('quality', SMALLINT),
                    Column('price', REAL)
                    )

    TB_USER = Table('users', DB_METADATA,
                    Column('id', INT, primary_key=True),
                    Column('name', VARCHAR),
                    Column('email', VARCHAR),
                    Column('birthday', DATE),
                    Column('gender', SMALLINT),
                    )

    TB_PURCHASE = Table('purchases', DB_METADATA,
                        Column('id', INT, primary_key=True),
                        Column('date', DATE),
                        Column('rating', REAL),
                        Column('user_id', INT, ForeignKey('users.id')),
                        Column('wine_id', INT, ForeignKey('wines.id'))
                        )

    TABLES = [TB_WINE, TB_USER, TB_PURCHASE]

    for TABLE in TABLES:
        TABLE.create()

    DB_ENGINE.dispose()
    print(f'Created {DB_NAME} schema in {round(time.time() - START_TIME, 2)} seconds')


if __name__ == '__main__':
    with open('constants.json') as CONSTANTS_FILE:
        CONSTANTS = json.load(CONSTANTS_FILE)

        create_df_schema(CONSTANTS['DB_NAME'], CONSTANTS['DB_USER'], CONSTANTS['DB_PASSWORD'], CONSTANTS['DB_IP'], CONSTANTS['DB_PORT'])
        load_csv_in_database(CONSTANTS['DB_NAME'], CONSTANTS['DB_USER'], CONSTANTS['DB_PASSWORD'], CONSTANTS['DB_IP'], CONSTANTS['DB_PORT'], ['wines', 'users', 'purchases'])
