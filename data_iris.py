import os
import psycopg2
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Point
from pyexcel_ods import get_data

from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Alignment

import importlib
import utils
importlib.reload(utils)
from utils import *

prefixe = 'iris'

# Vérification de l'existence des fichiers
data_exists, sheets_exist, wb = construct_or_import(prefixe)

if data_exists and sheets_exist:
        table_iris = wb.parse(f'table_{prefixe}')
        données_iris = wb.parse(f'données_{prefixe}')
        carte_iris = gpd.read_file(f"{CHEMIN}/carte_{prefixe}.geojson")
        print(f"Les tables table_{prefixe}, données_{prefixe}, carte_{prefixe} ont été importées avec succès.")

else:
    import data_gen
    
    bnb2022, bnb2023, décret = data_gen.tables()

    # Partie 1: AIRBNB ----
    # Lecture des données GeoJSON pour les iris
    iris = gpd.read_file("données/couches/georef-herault-iris.geojson")

    # Convertir bnb2022 en GeoDataFrame et effectuer la jointure spatiale avec iris
    bnb2022['geometry'] = bnb2022.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
    bnb2022_gdf = gpd.GeoDataFrame(bnb2022, geometry='geometry', crs="EPSG:4326")
    bnb2022_gdf = gpd.sjoin(bnb2022_gdf, iris, how="left", predicate="intersects")

    # Sélectionner les colonnes d'intérêt
    bnb2022_gdf = bnb2022_gdf[['code_officiel_iris', 'code_officiel_commune', 'Code EPCI', 
                            'Nb jours réservés', 'Revenu annuel €', 'Nb total jours de disponibilité']]

    # Agréger les données
    bnb2022_agg = bnb2022_gdf.groupby(['code_officiel_iris', 'code_officiel_commune']).agg(
        nombre_reserv_120=('Nb jours réservés', lambda x: (x >= 120).sum()),
        nombre_dispo_120=('Nb total jours de disponibilité', lambda x: (x >= 120).sum()),
        nombre_total=('Nb jours réservés', 'count'),
        jours_reserves=('Nb jours réservés', 'sum'),
        revenu=('Revenu annuel €', 'sum'),
        jours_reserves_120=('Nb jours réservés', lambda x: (bnb2022_gdf['Nb total jours de disponibilité'] >= 120).sum()),
        revenu_120=('Revenu annuel €', lambda x: (bnb2022_gdf['Nb total jours de disponibilité'] >= 120).sum())
    ).reset_index()

    # Convertir en DataFrame classique
    bnb2022_agg = pd.DataFrame(bnb2022_agg)

    # Convertir les colonnes Latitude et Longitude de bnb2023 en valeurs numériques
    bnb2023['Latitude'] = pd.to_numeric(bnb2023['Latitude'], errors='coerce')
    bnb2023['Longitude'] = pd.to_numeric(bnb2023['Longitude'], errors='coerce')

    # Filtrer les observations avec des valeurs non manquantes
    bnb2023 = bnb2023.dropna(subset=['Latitude', 'Longitude'])

    # Convertir bnb2023 en GeoDataFrame et effectuer la jointure spatiale avec iris
    bnb2023['geometry'] = bnb2023.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)
    bnb2023_gdf = gpd.GeoDataFrame(bnb2023, geometry='geometry', crs="EPSG:4326")
    bnb2023_gdf = gpd.sjoin(bnb2023_gdf, iris, how="left", predicate="intersects")

    # Sélectionner les colonnes d'intérêt
    bnb2023_gdf = bnb2023_gdf[['code_officiel_iris', 'code_officiel_commune', 
                            'Nb jours réservés', 'Revenu annuel €', 'Nb total jours de disponibilité']]

    # Agréger les données
    bnb2023_agg = bnb2023_gdf.groupby(['code_officiel_iris', 'code_officiel_commune']).agg(
        nombre_reserv_120=('Nb jours réservés', lambda x: (x >= 120).sum()),
        nombre_dispo_120=('Nb total jours de disponibilité', lambda x: (x >= 120).sum()),
        nombre_total=('Nb jours réservés', 'count'),
        jours_reserves=('Nb jours réservés', 'sum'),
        revenu=('Revenu annuel €', 'sum'),
        jours_reserves_120=('Nb jours réservés', lambda x: (bnb2023_gdf['Nb total jours de disponibilité'] >= 120).sum()),
        revenu_120=('Revenu annuel €', lambda x: (bnb2023_gdf['Nb total jours de disponibilité'] >= 120).sum())
    ).reset_index()

    # Convertir en DataFrame classique
    bnb2023_agg = pd.DataFrame(bnb2023_agg)

    # Fusionner les tables pour comparer les années 2022 et 2023
    table_iris = pd.merge(bnb2022_agg, bnb2023_agg, 
                        on=['code_officiel_iris', 'code_officiel_commune'], 
                        how='outer', suffixes=('_2022', '_2023'))

    # Calculer les évolutions et les écarts
    table_iris = table_iris.assign(
        evol_bnb_120=(table_iris['nombre_dispo_120_2023'] - table_iris['nombre_dispo_120_2022']) / table_iris['nombre_dispo_120_2022'],
        ecart_bnb_120=table_iris['nombre_dispo_120_2023'] - table_iris['nombre_dispo_120_2022'],
        ecart_bnb_tot=table_iris['nombre_total_2023'] - table_iris['nombre_total_2022'],
        evol_bnb_tot=(table_iris['nombre_total_2023'] - table_iris['nombre_total_2022']) / table_iris['nombre_total_2022'],
        ecart_jours=table_iris['jours_reserves_2023'] - table_iris['jours_reserves_2022'],
        ecart_revenu=table_iris['revenu_2023'] - table_iris['revenu_2022'],
        ecart_revenu_jours=(table_iris['revenu_2023'] / table_iris['jours_reserves_2023']) - (table_iris['revenu_2022'] / table_iris['jours_reserves_2022'])
    )

    # Partie 2: FILOSOFI ----
    # Lecture des données FiLoSoFi IRIS 2020
    # filosofi_iris_2020 = get_data("données/INSEE/FiLoSoFi/filosofi_iris_2020.ods")
    # filosofi_iris_2020 = pd.DataFrame(filosofi_iris_2020[0][1:], columns=filosofi_iris_2020[0][0])
    filosofi_iris_2020 = pd.read_excel("données/INSEE/FiLoSoFi/filosofi_iris_2020.xlsx")

    # Merge des données IRIS avec FiLoSoFi IRIS 2020
    filosofi_iris_2020 = pd.merge(iris[['code_officiel_iris', 'code_officiel_commune']], 
                                filosofi_iris_2020, left_on="code_officiel_iris", right_on="CODGEO")

    # Convertir en DataFrame classique et retirer la colonne 'geometry' si présente
    filosofi_iris_2020 = filosofi_iris_2020.drop(columns=['geometry'], errors='ignore')

    # Lecture des données FiLoSoFi Commune 2020
    # filosofi_com_2020 = get_data("données/INSEE/FiLoSoFi/filosofi_com_2020.ods")
    # filosofi_com_2020 = pd.DataFrame(filosofi_com_2020[0][1:], columns=filosofi_com_2020[0][0])
    filosofi_com_2020 = pd.read_excel("données/INSEE/FiLoSoFi/filosofi_com_2020.xlsx")

    # Sélection des IRIS de type "commune"
    code_iris_egal_com = iris[iris['type'] == "commune"]

    # Convertir en DataFrame classique et retirer la colonne 'geometry' si présente
    code_iris_egal_com = code_iris_egal_com.drop(columns=['geometry'], errors='ignore')

    # Sélectionner les colonnes d'intérêt
    code_iris_egal_com = code_iris_egal_com[['code_officiel_iris', 'code_officiel_commune']]

    # Merge des données FiLoSoFi Commune avec les IRIS de type "commune"
    filosofi_iris_egal_com_2020 = pd.merge(code_iris_egal_com, 
                                        filosofi_com_2020, 
                                        left_on="code_officiel_commune", 
                                        right_on="CODGEO")

    # Concaténer les données FiLoSoFi IRIS et Commune pour les IRIS de type "commune"
    filosofi_iris_2020 = pd.concat([filosofi_iris_egal_com_2020, filosofi_iris_2020], ignore_index=True)

    # Vérification des doublons d'IRIS
    # unique_count = len(filosofi_iris_2020['code_officiel_iris'].unique())

    # Fusion des tables pour inclure les données FiLoSoFi dans table_iris
    table_iris = pd.merge(table_iris, filosofi_iris_2020, on=["code_officiel_iris","code_officiel_commune"], how="outer")

    # Partie 3: INSEE LOGEMENT ----
    # Lecture des données INSEE Logement 2020
    ic_logement_2020 = pd.read_excel("données/INSEE/base-ic-logement-2020 - Copie.xlsx", sheet_name=0)
    ic_logement_2020 = ic_logement_2020[['IRIS', 'COM', 'P20_LOG', 'P20_RP', 'P20_RSECOCC', 'P20_LOGVAC', 'P20_RP_LOC', 'P20_RP_LOCHLMV']]

    # Lecture des données INSEE Logement 2014
    ic_logement_2014 = pd.read_excel("données/INSEE/base-ic-logement-2014 - Copie.xls", sheet_name=0)
    ic_logement_2014 = ic_logement_2014[['IRIS', 'COM', 'P14_LOG', 'P14_RP', 'P14_RSECOCC', 'P14_LOGVAC', 'P14_RP_LOC', 'P14_RP_LOCHLMV']]

    # Fusion des données INSEE Logement 2020 et 2014
    ic_insee = pd.merge(ic_logement_2020, ic_logement_2014, on=['IRIS', 'COM'])
    ic_insee['IRIS'] = pd.to_numeric(ic_insee['IRIS'], errors='coerce')

    # Fusion des données INSEE avec table_iris
    table_iris = pd.merge(table_iris, ic_insee, left_on='code_officiel_iris', right_on='IRIS')

    # Calcul des variables supplémentaires
    table_iris['point_RP'] = (table_iris['P20_RP'] / table_iris['P20_LOG']) - (table_iris['P14_RP'] / table_iris['P14_LOG'])
    table_iris['deriv_RP'] = (table_iris['P20_RP'] - table_iris['P14_RP']) / table_iris['P14_RP']
    table_iris['croissance_relative_RP_log'] = ((table_iris['P20_RP'] - table_iris['P14_RP']) / table_iris['P14_RP']) - ((table_iris['P20_LOG'] - table_iris['P14_LOG']) / table_iris['P14_LOG'])
    table_iris['gain_plpr'] = (table_iris['P20_RP_LOC'] - table_iris['P20_RP_LOCHLMV']) - (table_iris['P14_RP_LOC'] - table_iris['P14_RP_LOCHLMV'])
    table_iris['taux_d_implantation'] = table_iris['nombre_dispo_120_2023'] / (table_iris['P20_LOG'] - table_iris['P20_RP'])
    table_iris['taux_d_implantation_parc_tot'] = table_iris['nombre_total_2023'] / table_iris['P20_LOG']
    table_iris['taux_d_implantation_parc_tot_120'] = table_iris['nombre_dispo_120_2023'] / table_iris['P20_LOG']
    table_iris['potentiel_reconquete'] = table_iris['nombre_dispo_120_2023']
    table_iris['taux_reconquete'] = table_iris['nombre_dispo_120_2023'] / (table_iris['P20_RP_LOC'] - table_iris['P14_RP_LOC'])
    table_iris['deriv_plpr'] = ((table_iris['P20_RP_LOC'] - table_iris['P20_RP_LOCHLMV']) - (table_iris['P14_RP_LOC'] - table_iris['P14_RP_LOCHLMV'])) / (table_iris['P14_RP_LOC'] - table_iris['P14_RP_LOCHLMV'])
    table_iris['taux_bnb_parc_total'] = table_iris['nombre_dispo_120_2023'] / table_iris['P20_LOG']
    table_iris['part_rs'] = table_iris['P20_RSECOCC'] / table_iris['P20_LOG']
    table_iris['part_vac'] = table_iris['P20_LOGVAC'] / table_iris['P20_LOG']
    table_iris['deriv_rs'] = (table_iris['P20_RSECOCC'] - table_iris['P14_RSECOCC']) / table_iris['P14_RSECOCC']
    table_iris['deriv_vac'] = (table_iris['P20_LOGVAC'] - table_iris['P14_LOGVAC']) / table_iris['P14_LOGVAC']
    table_iris['bnb_120_sur_log_tot'] = table_iris['nombre_dispo_120_2023'] / table_iris['P20_LOG']


    # Partie 4: DV3F ----
    def try_except(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print("Unable to connect to Database.")
                return None
        return wrapper

    @try_except
    def connect_to_db():
        conn = psycopg2.connect(
            dbname="dv3f_2024",
            user="hab",
            password="34!hab99",
            host="s934",
            port="5441"
        )
        # print("Database Connected!")
        return conn

    conn = connect_to_db()

    queries = {
        "2022": """
        select idmutation, anneemut, libtypbien, codtypbien, valeurfonc, sbati, l_idpar from dvf.mutation where (codtypbien = '1112' or codtypbien = '1113' or codtypbien = '12121' or codtypbien = '12122' or codtypbien = '12125' or codtypbien = '12123' or codtypbien = '12124' or codtypbien = '12131' or codtypbien = '12132' or codtypbien = '12135' or codtypbien = '12133' or codtypbien = '12134') and (anneemut = 2020 or anneemut = 2021 or anneemut = 2022)
        """,
        "2020": """
        select idmutation, anneemut, libtypbien, codtypbien, valeurfonc, sbati, l_idpar from dvf.mutation where (codtypbien = '1112' or codtypbien = '1113' or codtypbien = '12121' or codtypbien = '12122' or codtypbien = '12125' or codtypbien = '12123' or codtypbien = '12124' or codtypbien = '12131' or codtypbien = '12132' or codtypbien = '12135' or codtypbien = '12133' or codtypbien = '12134') and (anneemut = 2019 or anneemut = 2020 or anneemut = 2021)
        """,
        "2014": """
        select idmutation, anneemut, libtypbien, codtypbien, valeurfonc, sbati, l_idpar from dvf.mutation where (codtypbien = '1112' or codtypbien = '1113' or codtypbien = '12121' or codtypbien = '12122' or codtypbien = '12125' or codtypbien = '12123' or codtypbien = '12124' or codtypbien = '12131' or codtypbien = '12132' or codtypbien = '12135' or codtypbien = '12133' or codtypbien = '12134') and (anneemut = 2013 or anneemut = 2014 or anneemut = 2015)
        """
    }

    def process_data(query, year):
        df = pd.read_sql_query(query, conn)
        df['id_par'] = df['l_idpar'].apply(lambda x: x[0]) # Prends la première parcelle. A voir si on peut faire mieux.
        # df['id_par'] = df['l_idpar'].str.extract(r'{([^,]+)}', expand=False).str.strip('{}')
        df = df.merge(corres_parcelle_iris, on="id_par", how='left')

        # e = df[['inseeiris', 'codtypbien', 'valeurfonc']].groupby(['inseeiris', 'codtypbien']).agg({'compte': 'count', 'médiane': 'median'})
        e = df.groupby(['inseeiris', 'codtypbien']).agg(
                compte=('valeurfonc', 'count'),
                médiane=('valeurfonc', 'median')
            ).reset_index()
        d = df.groupby('codtypbien')['valeurfonc'].median().reset_index().rename(columns={'valeurfonc': 'med_typbien'})
        data = e.merge(d, on="codtypbien", how='left')

        total_compte = e.groupby('inseeiris')['compte'].sum().reset_index().rename(columns={'compte': 'total_compte'})
        data = data.merge(total_compte, on="inseeiris", how='left')
        data['indicateur'] = (data['compte'] / data['total_compte']) * (data['médiane'] / data['med_typbien'])
        result = data[['inseeiris', 'indicateur']].groupby('inseeiris').sum().reset_index()

        return result.rename(columns={'indicateur': f'indicateur_marche_{year}'})

    # corres_parcelle_iris = pd.read_csv('R:/Hierarchique/DGA_AT/PPH-D_Hab_Am/Observatoire_habitat/3_AUTRES/ALTERNANT/ENZO/Avancement/Airbnb/données/dv3f/coresspondance.csv', sep=';')
    download = r"C:\Users\ejaguin\Downloads"
    corres_parcelle_iris = pd.read_csv(f"{download}\correspondance.csv", sep=';', header=0, dtype=str)

    dv3f_detail = [process_data(queries[year], year) for year in queries.keys()]

    dv3f_detail_temp = dv3f_detail[0].merge(dv3f_detail[1], on="inseeiris", how='outer', suffixes=("_2014", "_2020"))
    dv3f_detail_temp2 = dv3f_detail_temp.merge(dv3f_detail[2], on="inseeiris", how='outer', suffixes=("", "_2022"))

    dv3f_detail_temp2["evol_indicateur_marche"] = dv3f_detail_temp2["indicateur_marche_2020"] - dv3f_detail_temp2["indicateur_marche_2014"]
    dv3f_detail_temp2 = dv3f_detail_temp2[["inseeiris", "indicateur_marche_2022", "evol_indicateur_marche"]]
    table_iris['code_officiel_iris'] = table_iris['code_officiel_iris'].astype(float).astype(int).astype(str)
    dv3f_detail_temp2['inseeiris'] = dv3f_detail_temp2['inseeiris'].astype(str)
    table_iris = table_iris.merge(dv3f_detail_temp2, left_on="code_officiel_iris", right_on="inseeiris", how='left')

    table_iris = table_iris.fillna({"nombre_dispo_120_2023": 0})

    table_iris["evol_bnb_1202"] = np.where(table_iris["nombre_dispo_120_2022"] == 0,
                                            0.01, # ou une autre valeur significative
                                            (table_iris["nombre_dispo_120_2023"] - table_iris["nombre_dispo_120_2022"]) / table_iris["nombre_dispo_120_2022"])


    # Partie 5: Décret ----
    table_iris = pd.merge(table_iris, décret, how='left', left_on="code_officiel_commune", right_on="INSEE")
    table_iris['décret'].fillna(0, inplace=True)
    table_iris['décret'] = table_iris['décret'] #.astype('category')
    # table_iris.columns.name = "INSEE"

    # Partie 6: Nombre de batîment touristique / logement ----
    tourisme2020 = pd.read_excel("données/INSEE/tourisme/tourisme_com_2020.xlsx")
    cc_logement_2020 = pd.read_excel("données/INSEE/base-cc-logement - Copie.xlsx", sheet_name=0)
    cc_logement_2020 = cc_logement_2020[['CODGEO', 'P20_LOG', 'P20_RP', 'P20_RSECOCC', 'P20_LOGVAC', 'P20_RP_LOC']]
    tour_log = pd.merge(tourisme2020, cc_logement_2020, on='CODGEO')
    tour_log['tour_log'] = tour_log['camhot'] / tour_log['P20_LOG']
    tour_log = tour_log[['CODGEO', 'tour_log']]
    tour_log.columns = ['INSEE', 'tour_log']
    table_iris = pd.merge(table_iris, tour_log, how='left', on='INSEE')
    table_iris.drop('INSEE', axis=1, inplace=True)

    pop = pd.read_excel(r"données\INSEE\trage_2020_geo2023_com_AVerser.xlsx")
    tourisme = pd.read_excel(r"données\tourisme\2022_Carte_Offre_Marchande_Comm_Herault_Obs_Habitat.xlsx")
    tourisme = tourisme[['CODE INSEE', 'NOMBRE TOTAL DE LITS']]
    tourisme.columns = ['INSEE', 'nb_lit']
    pop_tour = pd.merge(pop, tourisme, left_on='CODEGEO', right_on='INSEE', how='left')
    pop_tour['nb_lit'] = pop_tour['nb_lit'].fillna(0)
    pop_tour['pop_tour'] = pop_tour['nb_lit'] / pop_tour['Ensemble']
    pop_tour = pop_tour[['INSEE', 'pop_tour']]
    table_iris = pd.merge(table_iris, pop_tour, how='left', on='INSEE')
    table_iris.drop('INSEE', axis=1, inplace=True)

    # Partie 7: Données ----
    # 567/578 observations où nombre_dispo_120_2022 > 0
    données_iris = table_iris.loc[:, ['code_officiel_iris', 'décret',
                'croissance_relative_RP_log', 'deriv_plpr',
                'taux_d_implantation', 'taux_d_implantation_parc_tot',
                'part_rs', 'part_vac', 'rev_disp_med',
                # 'croissance_prix_pas', 'prix_median_2020_2022',
                'indicateur_marche_2022', 'evol_indicateur_marche',
                'tour_log']] # 'valeurfonc', 'evol_pm2'
#[table_iris['nombre_dispo_120_2022'] > 0].dropna(subset=['taux_bnb_parc_total'])\

    # Partie 8: Carte ----
    iris['code_officiel_iris'] = iris['code_officiel_iris'].astype(str)
    données_iris['code_officiel_iris'] = données_iris['code_officiel_iris'].astype(float).astype(int).astype(str)
    carte_iris = pd.merge(iris, données_iris, on="code_officiel_iris")
    carte_iris.rename(columns={'code_officiel_iris': 'INSEE'}, inplace=True)
    carte_iris.rename(columns={'nom_officiel_iris': 'nom'}, inplace=True)
    données_iris.rename(columns={'code_officiel_iris': 'INSEE'}, inplace=True)
    # table_iris.rename(columns={'code_officiel_iris': 'INSEE'}, inplace=True)

    a = carte_iris[['INSEE', 'nom', 'nom_officiel_commune']]
    a = a.drop(columns=['nom_officiel_commune'])
    données_iris = pd.merge(a, données_iris, on="INSEE")

    backup_data(prefixe, table_iris, données_iris, carte_iris, data_exists)

def tables():
    return table_iris, données_iris, carte_iris
    

