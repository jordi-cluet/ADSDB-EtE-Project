#!/usr/bin/env python
# coding: utf-8

# Import libraries and packages
import pickle
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from difflib import SequenceMatcher

# Load final model from pickle file
pkl_filename = 'final_model.pkl'
with open(pkl_filename, 'rb') as file:
    [pickle_model, feature_order] = pickle.load(file)

# Ask user for input
print("Hi, please introduce the characteristics of the flat whose price you want to predict.")

# bathrooms
bathrooms = int(input("Number of bathrooms [0,20]: "))
assert bathrooms in range(0,21), "Number of bathrooms is not valid."

# building_subtype
building_subtype = input("Building subtype (write 'list' for available options): ")
building_subtypes = ['Flat', 'Apartment', 'Attic', 'Duplex', 'Loft', 'Study', 'House_Chalet', 'GroundFloorWithGarden', 'SemidetachedHouse', 'SemiDetached']
if building_subtype == 'list':
    print(building_subtypes)
    building_subtype = input("Building subtype: ")
assert building_subtype in building_subtypes, "Building subtype is not valid."

# conservation_state
conservation_state = input("Conservation state (write 'list' for available options): ")
conservation_states = ['New construction', 'Nearly new', 'Very good', 'Good', 'Renovated', 'To renovate']
if conservation_state == 'list':
    print(conservation_states)
    conservation_state = input("Conservation state: ")
assert conservation_state in conservation_states, "Conservation state is not valid."
    
# floor_elevator
floor_elevator = bool(input("Elevator (True/False): "))
assert floor_elevator in [True, False], "Elevator is not valid."

# rooms
rooms = int(input("Number of rooms [0,20]: "))
assert rooms in range(0,21), "Number of rooms is not valid."
    
# sq_meters
sq_meters = int(input("Squared meters [15,1000]: "))
assert sq_meters in range(0,10001), "Squared meters are not valid."
    
# neighbourhood
neighbourhood = input("Neighbourhood (write 'list' for available options): ")
neighbourhoods = ['el Raval', 'el Barri Gòtic', 'la Barceloneta', 'Sant Pere, Santa Caterina i la Ribera', 'el Fort Pienc', 'la Sagrada Família', "la Dreta de l'Eixample", "l'Antiga Esquerra de l'Eixample", "la Nova Esquerra de l'Eixample", 'Sant Antoni', 'el Poble Sec', 'la Marina del Prat Vermell', 'la Marina de Port', 'la Font de la Guatlla', 'Hostafrancs', 'la Bordeta', 'Sants - Badal', 'Sants', 'les Corts', 'la Maternitat i Sant Ramon', 'Pedralbes', 'Vallvidrera, el Tibidabo i les Planes', 'Sarrià', 'les Tres Torres', 'Sant Gervasi - la Bonanova', 'Sant Gervasi - Galvany', 'el Putxet i el Farró', 'Vallcarca i els Penitents', 'el Coll', 'la Salut', 'la Vila de Gràcia', "el Camp d'en Grassot i Gràcia Nova", 'el Baix Guinardó', 'Can Baró', 'el Guinardó', "la Font d'en Fargues", 'el Carmel', 'la Teixonera', 'Sant Genís dels Agudells', 'Montbau', "la Vall d'Hebron", 'la Clota', 'Horta', 'Vilapicina i la Torre Llobeta', 'Porta', 'el Turó de la Peira', 'Can Peguera', 'la Guineueta', 'Canyelles', 'les Roquetes', 'Verdun', 'la Prosperitat', 'la Trinitat Nova', 'Torre Baró', 'Ciutat Meridiana', 'Vallbona', 'la Trinitat Vella', 'Baró de Viver', 'el Bon Pastor', 'Sant Andreu', 'la Sagrera', 'el Congrés i els Indians', 'Navas', "el Camp de l'Arpa del Clot", 'el Clot', 'el Parc i la Llacuna del Poblenou', 'la Vila Olímpica del Poblenou', 'el Poblenou', 'Diagonal Mar i el Front Marítim del Poblenou', 'el Besòs i el Maresme', 'Provençals del Poblenou', 'Sant Martí de Provençals', 'la Verneda i la Pau']
if neighbourhood == 'list':
    print(neighbourhoods)
    neighbourhood = input("Neighbourhood: ")
assert neighbourhood in neighbourhoods, "Neighbourhood is not valid."


# Create new dataframe with single row to predict
d = {'bathrooms': [bathrooms],
    'building_subtype': [building_subtype],
    'conservation_state': [conservation_state],
    'floor_elevator': [floor_elevator],
    'rooms': [rooms],
    'sq_meters': [sq_meters],
    'neighbourhood': [neighbourhood]}
df = pd.DataFrame(data=d)

# Load barris_view from exploitation zone
host = 'postgresfib.fib.upc.edu'
dbname = 'ADSDBjordi.cluet'
user = 'jordi.cluet'
pwd = 'DB151199'
port = 6433
sslmode = 'require'

conn = psycopg2.connect("host='{}' port={} dbname='{}' user={} password={}".format(host, port, dbname, user, pwd))
cursor = conn.cursor()

sql = "SELECT * from exploitation_zone.barris_view;"
barris_view = pd.read_sql_query(sql, conn)


# Prediction

# Augmentate data
dfm = pd.merge(df, barris_view, on='neighbourhood')

# Add categories that are not in the row to be predicted
dfm.building_subtype = dfm.building_subtype.astype('category')
dfm.building_subtype = dfm.building_subtype.cat.add_categories(list(set(building_subtypes) - set([dfm.building_subtype[0]])))

dfm.neighbourhood = dfm.neighbourhood.astype('category')
dfm.neighbourhood = dfm.neighbourhood.cat.add_categories(list(set(neighbourhoods) - set([dfm.neighbourhood[0]])))

dfm.conservation_state = dfm.conservation_state.astype('category')
dfm.conservation_state = dfm.conservation_state.cat.add_categories(list(set(conservation_states) - set([dfm.conservation_state[0]])))

districtes = ['Ciutat Vella', 'Eixample', 'Sants-Montjuïc', 'Les Corts', 'Sarrià-Sant Gervasi', 'Gràcia', 'Horta-Guinardó', 'Nou Barris', 'Sant Andreu', 'Sant Martí']
dfm.districte = dfm.districte.astype('category')
dfm.districte = dfm.districte.cat.add_categories(list(set(districtes) - set([dfm.districte[0]])))


# One-hot encoding
ohe_bs = pd.get_dummies(dfm.building_subtype, prefix='bs')
ohe_cs = pd.get_dummies(dfm.conservation_state, prefix='cs')
ohe_d = pd.get_dummies(dfm.districte, prefix='d')
ohe_n = pd.get_dummies(dfm.neighbourhood, prefix='n')
dfmoh = pd.concat([dfm, ohe_bs, ohe_cs, ohe_d, ohe_n], axis=1)
dfmoh.drop(['building_subtype', 'conservation_state', 'districte', 'neighbourhood'], axis=1, inplace=True)


# Execute prediction
assert set(feature_order) == set(list(dfmoh.columns)), "The number of features does not coincide"
dfmoh = dfmoh[feature_order]
print(f"Predicted price is {round(pickle_model.predict(dfmoh)[0], 2)}€.")

