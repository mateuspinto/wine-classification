import random
import csv

from faker import Faker
import pandas as pd

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
FAKER = Faker(seed=RANDOM_SEED)

########################

RED = pd.read_csv('data/raw/winequality-red.csv', sep=';')
WHITE = pd.read_csv('data/raw/winequality-red.csv', sep=';')
WINE_NAMES = pd.read_csv('data/raw/wine_names.csv')['wine_name'].to_list()


def format_column(x): return x.lower().replace(' ', '_')


RED.rename(columns={RAW_COLUMN_NAME: format_column(RAW_COLUMN_NAME) for RAW_COLUMN_NAME in RED.columns.to_list()}, inplace=True)
WHITE.rename(columns={RAW_COLUMN_NAME: format_column(RAW_COLUMN_NAME) for RAW_COLUMN_NAME in WHITE.columns.to_list()}, inplace=True)

RED['is_red'] = 1
WHITE['is_red'] = 0

RED['name'] = RED.apply(lambda x: 'Red ' + random.choice(WINE_NAMES), axis=1)
WHITE['name'] = WHITE.apply(lambda x: 'White ' + random.choice(WINE_NAMES), axis=1)

RED['price'] = RED.apply(lambda x: round(x['quality'] * random.uniform(112.5, 137.5), 2), axis=1)
WHITE['price'] = WHITE.apply(lambda x: round(x['quality'] * random.uniform(67.5, 82.5), 2), axis=1)

WINE_DATA = pd.concat([RED, WHITE])[['name', 'is_red', 'fixed_acidity', 'volatile_acidity', 'citric_acid', 'residual_sugar', 'chlorides', 'free_sulfur_dioxide', 'total_sulfur_dioxide', 'density', 'ph', 'sulphates', 'alcohol', 'quality', 'price']].sample(frac=1, random_state=RANDOM_SEED, ignore_index=True)
WINE_COUNT = len(WINE_DATA)
WINE_DATA.to_csv('data/enriched/wine.csv', header=None)

########################

USER_COUNT = 1000
with open('data/enriched/user.csv', 'w', newline='', encoding='utf-8') as FILE:
    CSV = csv.writer(FILE)

    for I in range(USER_COUNT):
        IS_MALE = random.randint(0, 1)
        CSV.writerow([I, FAKER.name_male() if IS_MALE else FAKER.name_female(), FAKER.email(), FAKER.date_of_birth(), IS_MALE])

########################

PURCHASE_COUNT = 10000
with open('data/enriched/purchase.csv', 'w', newline='', encoding='utf-8') as FILE:
    CSV = csv.writer(FILE)

    for I in range(PURCHASE_COUNT):
        CSV.writerow([I, FAKER.date_this_year(), round(random.random(), 4), random.randint(0, USER_COUNT - 1), random.randint(0, WINE_COUNT - 1)])
