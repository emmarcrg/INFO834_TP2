import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
import matplotlib.pyplot as plt

def recup_data (filename) :
    # Récupération des données des fichiers csv 
    df = pd.read_csv(filename)
    data = pd.DataFrame(df)
    return data
    
villes = recup_data("./data/villes_virgule.csv")
academies = recup_data("./data/academies_virgule.csv")

# 1. Convertir les données d'un format à un autre
# Dans la majorité des cas ici, une vérification du types des objets mis en paramètre a été rajoutée.
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

# 2. Le schéma 
def get_schema (data):
    # Récupérer le schéma des tables
    if isinstance(data, pa.Table) :
        # retourne le schéma d'une table
        return data.schema

# 3. Récupérer une colonne
def get_colonne (data, nom_colonne):
    # Retourner les données des colonnes; il est supposer que la colonne fait bien partie de la table choisie 
    if isinstance(data, pa.Table) :
        # retourne l'ensemble des valeurs présente dans une colonne de ce nom
        return data.column(nom_colonne)
    '''
    J'ai choisit de passer par le nom car je trouvais celui plus intuitif que en sélectionnant l'indice.
    En utilisant select, nous aurions eu : table.select(0) avec 0 étant l'indice de la colonne que nous souhaitions sélectionnée
    '''

# 4. Obtenir les statistiques
def stats(data, nom_colonne):
    # Effectuer count, count_distinct, sum, min, max d'un certain tableau
    col = get_colonne(data, nom_colonne)
    list = col.to_pylist()
    
    # Ensemble des statistiques demandées => dans la librairie compute de pyarrow
    count = pc.count(list)
    countd = pc.count_distinct(list)
    sum = pc.sum(list)
    min = pc.min(list)
    max = pc.max(list)
    return count, countd, sum, min, max

#5. Informations sur les villes et départements
def infos_villes (nom_ville):
    # Récupération des informations pour une ville données
    # Passer par un dataframe ain d'avoir les informations en fonction des lignes : semble plus simple dans ce cas ci
    df = table_to_df(villes_tb)
    infos = df.loc[df['nom'] == nom_ville]
    return infos

def infos_dep (data, num_dep):
    #Récupération des données informations pour un département 
    
    # Trie des départements par ordre croissants 
    sorted_indices = pc.sort_indices(data['dep'])
    data.take(sorted_indices)
    
    # On sélectionne le département 
    # Autre manière de sélectionner un élément, cette fois ci en passant par la table
    condition = pc.equal(data['dep'], num_dep)
    data = data.filter(condition)
    
    # Trie des départements par ordre alphabétique 
    sorted_indices = pc.sort_indices(data['nom'])
    data.take(sorted_indices)
    return data

# 6. Agrégats
def agregats (num_dep):
    # Récupération des moyennes
    # Moyenne pour un département donner en paramètre 
    # J'ai décidé d'utiliser l'attribut agregate dans lequel je lui donne la fonction effectuée
    villes_triee = infos_dep(villes_tb, num_dep)
    mean_2012 = pa.TableGroupBy(villes_triee,"dep").aggregate([("nb_hab_2012", "mean")])

    # Moyenne du nombre d'habitant en fonction du département 
    # j'ai décidé de prendre le nombre d'habitant par département en 2012 de sorte à avoir des valeurs cohérentes
    mean_hab_dep = pa.TableGroupBy(villes_tb,"dep").aggregate([("nb_hab_2012", "mean")])
    return mean_2012, mean_hab_dep  

#7. Jointures 
def jointures():
    # Affichage des données suivantes : 
    # Zones de vacances des villes, 
    #Jointure de villes_tb et academies_tb par rapport à la colonne dep
    combined_1 = villes_tb.join(academies_tb, 'dep')
    #Suppression des colonnes dont je n'ai pas besoin
    combined_1 = combined_1.drop([
                        'cp', 'nb_hab_2010','nb_hab_1999',
                        'nb_hab_2012', 'dens', 'surf', 'long',  'lat',
                        'alt_min', 'alt_max', 'wikipedia', 'departement',
                        'region'])
    
    # Le reste des tableaux est donner en fonction de la première jointure et d'une sélection approfondie
    # Les villes de la zone de  vacances A 
    temp = combined_1
    condition = pc.equal(temp['vacances'], "Zone A")
    combined_2 = temp.filter(condition)
    
    # les départements des zones de vacances A et B 
    temp = combined_1
    condition1 = pc.equal(temp['vacances'], "Zone A")
    condition2 = pc.equal(temp['vacances'], "Zone B")
    combined_condition = pc.or_(condition1, condition2)

    combined_3 = temp.filter(combined_condition)
    
    # le nombre de villes par académie 
    temp = combined_1
    combined_4 = pa.TableGroupBy(temp,"academie").aggregate([("nom", "count")])
    
    return combined_1, combined_2, combined_3, combined_4

c1, c2, c3, c4 = jointures()

# 9. Histogramme
def histogramme() : 
    sort = pc.sort_indices(c4['nom_count'])
    c4.take(sort)
    data = table_to_df(c4)
    
    # Supprimer les valeurs nulles
    data = data.dropna()

    # Extraire les colonnes
    x = data['academie']
    y = data['nom_count']    
    
    plt.bar(x, y, edgecolor='black')

    # Ajouter un titre et des étiquettes
    plt.title("Histogramme du nombre de villes en fonction de l'académie")
    plt.xlabel('Académies')
    plt.ylabel('Nombre de villes')

    # Afficher l'histogramme
    plt.show()


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


'''print("Test des récupérations des informations ------------------------")
infos_annecy = infos_villes("Annecy")
print(f"Information concernant la ville d'Annecy : {infos_annecy}")
info_74 = infos_dep(villes_tb, "74")
print(f"Information concernant le département de la Haute Savoie : {info_74}")'''

'''print("Test des agrégats ------------------------")
mean_2012, mean_hab = agregats("74")
print(f"Moyenne des habitants en 2012 pour la Haute Savoie = {mean_2012}, moyenne des habitants par départements = {mean_hab}")'''

'''print("Test des jointures ------------------------")
print("Résultats des zones de vacances de chacunes des villes : ")
print(c1)
print("Résultats des villes de la zone A : ")
print(c2)
print("Résultats des départements de la zone A & B : ")
print(c3)
print("Résultats du nombre de villes par académie : ")
print(c4)'''

#histogramme()