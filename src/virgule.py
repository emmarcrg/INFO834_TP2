import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import matplotlib as plt

def recup_data (filename) :
    # Récupération des données des fichiers csv 
    df = pd.read_csv(filename)
    data = pd.DataFrame(df)
    return data
    
villes = recup_data("./data/villes_virgule.csv")
academies = recup_data("./data/academies_virgule.csv")


# 1. Convertir les données d'un format à un autre
def df_to_table(data):
    # écriture d'un dataframe à une table
    if isinstance(data, pd.DataFrame) :
        tb = pa.Table.from_pandas(data)
    return tb
    
def table_to_df(data):
    # lecture d'une table à un dataframe
    if isinstance(data, pa.Table) :
        df = data.to_pandas()
    return df

def table_to_parquet(data, parquet_filename):
    # écriture d'un fichier parquet
    if isinstance(data, pa.Table) :
        pq.write_table(data, parquet_filename)    
    # pas de return puisque c'est un objet parquet
    
def parquet_to_table(parquet_filename):
    # lecture d'un fichier parquet 
    table = pq.read_table(parquet_filename)
    return table

villes_tb = df_to_table(villes)
academies_tb = df_to_table(academies)

def get_schema (data):
    # Récupérer le schéma des tables
    if isinstance(data, pa.Table) :
        return data.schema
    
def get_colonne (data, nom_colonne):
    # Retourner les données des colonnes; il est supposer que la colonne fait bien partie de la table choisie 
    if isinstance(data, pa.Table) :
        return data.column(nom_colonne)
    '''
    En utilisant select, nous aurions eu : table.select(0) avec 0 étant l'indice de la colonne que nous souhaitions sélectionnée
    '''

def stats(data, nom_colonne):
    # Effectuer count, count_distinct, sum, min, max d'un certain tableau
    col = get_colonne(data, nom_colonne)
    list = col.to_pylist()
        
    count = pc.count(list)
    countd = pc.count_distinct(list)
    sum = pc.sum(list)
    min = pc.min(list)
    max = pc.max(list)
    return count, countd, sum, min, max
    
    
################## TEST ################
'''print("Dataframe des villes -----------------------------------")
print(villes)
print("Dataframe des académies -----------------------------------")
print(academies)'''

'''print("Test des lectures et écritures sur les villes ------------------------")
print("Passage en table : ")
data = df_to_table(villes)
print(data)
print("Passage en dataframe (depuis une table) : ")
data_return = table_to_df(data)
print(data_return)
print("Passage en parquet et vérification")
data_further = table_to_parquet(data, "villes.parquet")
data_further = parquet_to_table("villes.parquet")
print(data_further)'''

'''print("Test des lectures et écritures sur les académies ------------------------")
print("Passage en table : ")
data = df_to_table(academies)
print(data)
print("Passage en dataframe (depuis une table) : ")
data_return = table_to_df(data)
print(data_return)
print("Passage en parquet et vérification")
data_further = table_to_parquet(data, "academies.parquet")
data_further = parquet_to_table("academies.parquet")
print(data_further)'''

'''print("Test des schémas des tables ------------------------")
print("Schéma pour les données des villes : ")
print(get_schema(villes_tb))
print("Schéma pour les données des académies")
print(get_schema(academies_tb))'''

'''print("Test de la récupération de colonne ------------------------")
print("Récupération de la colonne dep de ville : ")
print(get_colonne(villes_tb, "dep"))
print("Récupération de la colonne vacances d'academies : ")
print(get_colonne(academies_tb, "vacances"))'''

'''print("Test des stats ------------------------")
count, countd, sum, min, max = stats(villes_tb, "nb_hab_2010")
print(f"Statistiques pour villes avec la colonne nb_hab_2010 : count = {count}; countd = {countd}; sum = {sum}; min = {min}; max = {max}")
count, countd, sum, min, max = stats(villes_tb, "nb_hab_2012")
print(f"Statistiques pour villes avec la colonne nb_hab_2012 : count = {count}; countd = {countd}; sum = {sum}; min = {min}; max = {max}")'''