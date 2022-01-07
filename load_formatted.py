#!/usr/bin/env python
# coding: utf-8

# Import libraries and packages
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import pickle

# Set connection with postgres database
host = 'postgresfib.fib.upc.edu'
dbname = 'ADSDBjordi.cluet'
user = 'jordi.cluet'
pwd = 'DB151199'
port = 6433
sslmode = 'require'

conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, user, pwd))
cursor = conn.cursor()


#################### Create formatted and trusted zone schemas if they do not exist ####################

# Create formatted_zone schema if it does not exist
create_formatted_zone = """CREATE SCHEMA IF NOT EXISTS formatted_zone;"""
cursor.execute(create_formatted_zone)
conn.commit()

# Create trusted_zone schema if it does not exist
create_trusted_zone = """CREATE SCHEMA IF NOT EXISTS trusted_zone;"""
cursor.execute(create_trusted_zone)
conn.commit()


################################## Function to insert rows into table ##################################

def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("The dataframe was correctly inserted")
    cursor.close()


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''''''''''''''''''''''''''''''''' DATA PREPARATION '''''''''''''''''''''''''''''''''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

################################ Load housing table into formatted zone ################################

# Read dataframe from CSV file
df = pd.read_csv('data/zenodo_fotocasa_2020_21-12-06_formatted.csv')

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS formatted_zone.zenodo_fotocasa_2020_21_12_06 (
    ID INTEGER PRIMARY KEY,
    ADDRESS VARCHAR(80),
    BATHROOMS INTEGER,
    BUILDING_SUBTYPE VARCHAR(30),
    BUILDING_TYPE VARCHAR(4),
    CONSERVATION_STATE INTEGER,
    EXTRACTION_DATE DATE,
    DISCOUNT INTEGER,
    FLOOR_ELEVATOR INTEGER,
    IS_NEW_CONSTRUCTION BOOLEAN,
    LINK VARCHAR(255),
    PRICE FLOAT,
    REAL_ESTATE VARCHAR(55),
    REAL_ESTATE_ID VARCHAR(15),
    ROOMS INTEGER,
    SQ_METERS FLOAT,
    NEIGHBOURHOOD VARCHAR(45),
    NEIGHBOURHOOD_MEAN_PRICE FLOAT
);"""
cursor.execute(sqlCreateTable)
conn.commit()

execute_values(conn, df, 'formatted_zone.zenodo_fotocasa_2020_21_12_06')


##################################### Format housing table for trusted zone #####################################

#  Select whole table as dataframe
sql = "SELECT * from formatted_zone.zenodo_fotocasa_2020_21_12_06;"
df = pd.read_sql_query(sql, conn)

# Remove useless columns
df = df.drop(['extraction_date', 'link'], axis = 1)  # useless columns

# Correct some data types
df['id'] = df['id'].astype("object")
df['address'] = df['address'].astype("string")
df['building_subtype'] = df['building_subtype'].astype("category")
df['building_type'] = df['building_type'].astype("category")
df['conservation_state'] = df['conservation_state'].astype("category")
df['floor_elevator'] = df['floor_elevator'].astype("bool")
df['real_estate'] = df['real_estate'].astype("category")
df['real_estate_id'] = df['real_estate_id'].astype("object")
df['neighbourhood'] = df['neighbourhood'].astype("category")

# Remove duplicates
df = df[-df.iloc[:, 1:].duplicated()]
df = df.reset_index(drop=True)

# Check levels of categorical variables

# building_type
df = df.drop(['building_type'], axis = 1)

# conservation_state
df['conservation_state'] = df['conservation_state'].replace({
    0: 'New construction', 
    1: 'Nearly new', 
    2: 'Very good', 
    3: 'Good', 
    4: 'To renovate', 
    8: 'Renovated'
  })
df['conservation_state'] = df['conservation_state'].astype("category")

# is_new_construction
df = df.drop(['is_new_construction'], axis = 1)

# Define function to get outliers
def get_outliers(var, factor):
    Q1 = df[var].quantile(0.25)
    Q3 = df[var].quantile(0.75)
    IQR = Q3 - Q1
    outliers = (df[var] < Q1-factor*IQR) | (df[var] > Q3+factor*IQR)
    return outliers

# Analysis, missing values and outliers of numerical variables

# bathrooms
outliers = get_outliers('bathrooms', 3)
df = df[-outliers]

# discount
outliers = get_outliers('discount', 5)
df = df[-outliers]

# price
extreme_outliers = df['price'] > 15000
df = df[-extreme_outliers]
outliers = get_outliers('price', 5)
df = df[-outliers]

# rooms
outliers = get_outliers('rooms', 3)
df = df[-outliers]

# sq_meters
outliers = get_outliers('sq_meters', 5)
df = df[-outliers]

df = df.reset_index(drop=True)
little = df['sq_meters'] <= 15
df.loc[little, 'sq_meters'] = np.nan

# neighbourhood_mean_price
aux = df.iloc[:,-2:].drop_duplicates().dropna()
aux = aux.sort_values(by=['neighbourhood_mean_price'], ascending=False)
aux = aux.reset_index(drop=True)

#  Missing values
df = df.reset_index(drop=True)

# Count missing values
df = df.replace('NaN', np.nan, regex=True)

# Manually correct 2 missings in neighbourhood and neighbourhood_mean_price
df.loc[df['id'] == 968, 'neighbourhood'] = 'sant andreu'
df.loc[df['id'] == 9380, 'neighbourhood'] = 'la marina de port'

df.loc[df['id'] == 968, 'neighbourhood_mean_price'] = df[df['neighbourhood'] == 'sant andreu']['neighbourhood_mean_price'].mean()
df.loc[df['id'] == 9380, 'neighbourhood_mean_price'] = df[df['neighbourhood'] == 'la marina de port']['neighbourhood_mean_price'].mean()

# Remove 4 rows with missing price (since it is the target)
df = df[-df['price'].isna()]

# Impute missings in sq_meters using KNN method
df = df.reset_index(drop=True)

from sklearn.impute import KNNImputer
imputer = KNNImputer(n_neighbors=5, weights="uniform")

newData = df.select_dtypes('number').iloc[:,1:]
newData = pd.DataFrame(imputer.fit_transform(newData), columns=newData.columns)

df['sq_meters'] = newData['sq_meters'].copy()

# Remove outliers on price_per_sqm
df['price_per_sqm'] = df['price'] / df['sq_meters']
extreme_outliers = df['price_per_sqm'] > 60

df = df[-extreme_outliers]
df = df.reset_index(drop=True)


################################ Load barris-districtes table into formatted zone ################################

# Read dataframe from CSV file
df = pd.read_csv('data/ajunt_barris_2017_21-12-06.csv')

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS formatted_zone.AJUNT_BARRIS_2017_21_12_06 (
    CODI_DISTRICTE INTEGER,
    NOM_DISTRICTE VARCHAR(50),
    CODI_BARRI INTEGER,
    NOM_BARRI VARCHAR(50));"""
cursor.execute(sqlCreateTable)
conn.commit()

# Insert rows into table
execute_values(conn, df, 'formatted_zone.AJUNT_BARRIS_2017_21_12_06')


################################ Load barris-districtes table into trusted zone ################################
# ------------------------------------ As no quality changes are needed ----------------------------------------

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS trusted_zone.AJUNT_BARRIS_2017_21_12_06 (
    CODI_DISTRICTE INTEGER,
    NOM_DISTRICTE VARCHAR(50),
    CODI_BARRI INTEGER,
    NOM_BARRI VARCHAR(50));"""
cursor.execute(sqlCreateTable)
conn.commit()

# Insert rows into table
execute_values(conn, df, 'trusted_zone.AJUNT_BARRIS_2017_21_12_06')


################################ Load crime table into formatted zone ################################

# Read dataframe from CSV file
df = pd.read_excel('data/ajunt_crime_2020_21-12-06_final.xlsx')

# Rename columns
df.columns = ['Districte', 'Furt', 'Estafes', 'Danys', 'Rob_viol_intim', 'Rob_en_vehicle', 'Rob_força', 'Lesions', 'Aprop_indeg', 'Amenaces', 'Rob_de_vehicle', 'Ocupacions', 'Salut_pub', 'Abusos_sex', 'Entrada_domicili', 'Agressio_sex', 'Conviv_veinal', 'Vigilancia_poli', 'Molesties_espai_pub', 'Contra_prop_priv', 'Incendis', 'Estupefaents', 'Agressions', 'Proves_alcohol','Proves_droga']

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS formatted_zone.ajunt_crime_2020_21_12_06 (
    DISTRICTE VARCHAR(50),
    FURT INTEGER,
    ESTAFES INTEGER,
    DANYS INTEGER,
    ROB_VIOL_INTIM INTEGER,
    ROB_EN_VEHICLE INTEGER,
    ROB_FORÇA INTEGER,
    LESIONS INTEGER,
    APROP_INDEG INTEGER,
    AMENACES INTEGER,
    ROB_DE_VEHICLE INTEGER,
    OCUPACIONS INTEGER,
    SALUT_PUB INTEGER,
    ABUSOS_SEX INTEGER,
    ENTRADA_DOMICILI INTEGER,
    AGRESSIO_SEX INTEGER,
    CONVIV_VEINAL INTEGER,
    VIGILANCIA_POLI INTEGER,
    MOLESTIES_ESPAI_PUB INTEGER,
    CONTRA_PROP_PRIV INTEGER,
    INCENDIS INTEGER,
    ESTUPEFAENTS INTEGER,
    AGRESSIONS INTEGER,
    PROVES_ALCOHOL INTEGER,
    PROVES_DROGA INTEGER
);"""
cursor.execute(sqlCreateTable)
conn.commit()

# Insert rows into table
execute_values(conn, df, 'formatted_zone.ajunt_crime_2020_21_12_06')


################################ Load crime table into trusted zone ################################
# ------------------------------- As no quality changes are needed ---------------------------------

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS trusted_zone.ajunt_crime_2020_21_12_06 (
    DISTRICTE VARCHAR(50),
    FURT INTEGER,
    ESTAFES INTEGER,
    DANYS INTEGER,
    ROB_VIOL_INTIM INTEGER,
    ROB_EN_VEHICLE INTEGER,
    ROB_FORÇA INTEGER,
    LESIONS INTEGER,
    APROP_INDEG INTEGER,
    AMENACES INTEGER,
    ROB_DE_VEHICLE INTEGER,
    OCUPACIONS INTEGER,
    SALUT_PUB INTEGER,
    ABUSOS_SEX INTEGER,
    ENTRADA_DOMICILI INTEGER,
    AGRESSIO_SEX INTEGER,
    CONVIV_VEINAL INTEGER,
    VIGILANCIA_POLI INTEGER,
    MOLESTIES_ESPAI_PUB INTEGER,
    CONTRA_PROP_PRIV INTEGER,
    INCENDIS INTEGER,
    ESTUPEFAENTS INTEGER,
    AGRESSIONS INTEGER,
    PROVES_ALCOHOL INTEGER,
    PROVES_DROGA INTEGER
);"""
cursor.execute(sqlCreateTable)
conn.commit()

# Insert rows into table
execute_values(conn, df, 'trusted_zone.ajunt_crime_2020_21_12_06')


################### Load district population and surface table into formatted zone ###################

# Read dataframe from CSV file
df = pd.read_excel('data/ajunt_districtes_2021_21-12-24.xlsx')

# Rename columns
df.columns = ['Districte', 'Superficie', 'Poblacio']

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS formatted_zone.ajunt_districtes_2021_21_12_24 (
    DISTRICTE VARCHAR(50),
    SUPERFICIE FLOAT,
    POBLACIO INTEGER
);"""
cursor.execute(sqlCreateTable)
conn.commit()

# Insert rows into table
execute_values(conn, df, 'formatted_zone.ajunt_districtes_2021_21_12_24')


################### Load district population and surface table into trusted zone ###################
# --------------------------------- As no quality changes are needed -----------------------------------

# Create new table in PostgreSQL database
sqlCreateTable = """CREATE TABLE IF NOT EXISTS trusted_zone.ajunt_districtes_2021_21_12_24 (
    DISTRICTE VARCHAR(50), 
    SUPERFICIE FLOAT,
    POBLACIO INTEGER
);"""
cursor.execute(sqlCreateTable)
conn.commit()

execute_values(conn, df, 'trusted_zone.ajunt_districtes_2021_21_12_24')


'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
'''''''''''''''''''''''''''''''''' MODELLING ''''''''''''''''''''''''''''''''''''''''''
'''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

# Select integrated table from exploitation zone
sql = "SELECT * from exploitation_zone.housing_view;"
df = pd.read_sql_query(sql, conn)

# Feature engineering

# Remove some variables that are not useful for our modelling
df.drop(['id', 'price_per_sqm', 'discount'], axis=1, inplace=True)

#  One-hot enconding of some variables
ohe_bs = pd.get_dummies(df.building_subtype, prefix='bs')
ohe_cs = pd.get_dummies(df.conservation_state, prefix='cs')
ohe_d = pd.get_dummies(df.districte, prefix='d')
ohe_n = pd.get_dummies(df.neighbourhood, prefix='n')
df = pd.concat([df, ohe_bs, ohe_cs, ohe_d, ohe_n], axis=1)
df.drop(['building_subtype', 'conservation_state', 'districte', 'neighbourhood'], axis=1, inplace=True)

# Dataframe with crime variables, with district, with neighbourhood (i.e. with everything)
df_neigh = df

# Divide between train and validation sets
from sklearn.model_selection import train_test_split
X = df_neigh.drop('price', axis=1)
y = df_neigh['price']
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

# Linear regression model
from sklearn.linear_model import LinearRegression
reg = LinearRegression().fit(X_train, y_train)

# Save model in pickle file
pkl_filename = "./model.pkl"
with open(pkl_filename, 'wb') as file:
    pickle.dump(reg, file)
