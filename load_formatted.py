#!/usr/bin/env python
# coding: utf-8

# Import libraries and packages
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np

# Set connection with postgres database
host = 'postgresfib.fib.upc.edu'
dbname = 'ADSDBjordi.cluet'
user = 'jordi.cluet'
pwd = 'DB151199'
port = 6433
sslmode = 'require'

conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, user, pwd))
cursor = conn.cursor()


################################ Load housing table into formatted zone ################################

# Read dataframe from CSV file
df = pd.read_csv('data/zenodo_fotocasa_2020_21-12-06_formatted.csv')

# Create formatted_zone schema if it does not exist
create_formatted_zone = """CREATE SCHEMA IF NOT EXISTS formatted_zone;"""
cursor.execute(create_formatted_zone)
conn.commit()

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

# Insert rows into table
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

execute_values(conn, df, 'formatted_zone.zenodo_fotocasa_2020_21_12_06')


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
# As no quality changes are needed.

# Create trusted_zone schema if it does not exist
create_trusted_zone = """CREATE SCHEMA IF NOT EXISTS trusted_zone;"""
cursor.execute(create_trusted_zone)
conn.commit()

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

# Create formatted_zone schema if it does not exist
create_formatted_zone = """CREATE SCHEMA IF NOT EXISTS formatted_zone;"""
cursor.execute(create_formatted_zone)
conn.commit()

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


################################ Load district population and surface table into formatted zone ################################

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
