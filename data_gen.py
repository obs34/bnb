import os
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

PATH_DATA_G = os.getenv('PATH_DATA_G')
G_AIRDNA = 'airdna'
G_DECRET = 'decret'

print("Chargement des données Airbnb...")
chemin = os.path.join(PATH_DATA_G, G_AIRDNA, 'offre_airdna_34_2022.xlsx')
bnb2022 = pd.read_excel(chemin)
bnb2022 = bnb2022[bnb2022['Liste logement']=='Entire home/apt']
bnb2022 = bnb2022[['INSEE','Communes','Code EPCI','EPCI','Longitude','Latitude','Revenu annuel €',
                   'Nb jours réservés','Nb total jours de disponibilité']]

chemin = os.path.join(PATH_DATA_G, G_AIRDNA, 'offre_airdna_34_2023.xlsx')
bnb2023 = pd.read_excel(chemin)
bnb2023 = bnb2023[bnb2023['Nature logement']=='Entire home/apt']
bnb2023 = bnb2023[['INSEE','Communes','Code EPCI','EPCI','Longitude','Latitude','Revenu annuel â‚¬',
                             'Nb jours rÃ©servÃ©s','Nb total jours de disponibilitÃ©']]
bnb2023.columns = ['INSEE','Communes','Code EPCI','EPCI','Longitude','Latitude','Revenu annuel €',
                             'Nb jours réservés','Nb total jours de disponibilité']
print("Chargement des données Airbnb terminé.")

chemin = os.path.join(PATH_DATA_G, G_DECRET, "2023_decret.xlsx")
decret = pd.read_excel(chemin, sheet_name="3")

def tables():
    return bnb2022, bnb2023, decret
