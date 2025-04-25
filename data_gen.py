import pandas as pd

PATH_AIRDNA = 'data/airdna/'
print("Chargement des données Airbnb...")
bnb2022 = pd.read_excel(PATH_AIRDNA + 'offre_airdna_34_2022.xlsx')
bnb2022 = bnb2022[bnb2022['Liste logement']=='Entire home/apt']
bnb2022 = bnb2022[['INSEE','Communes','Code EPCI','EPCI','Longitude','Latitude','Revenu annuel €',
                   'Nb jours réservés','Nb total jours de disponibilité']]


bnb2023 = pd.read_excel(PATH_AIRDNA + 'offre_airdna_34_2023.xlsx')
bnb2023 = bnb2023[bnb2023['Nature logement']=='Entire home/apt']
bnb2023 = bnb2023[['INSEE','Communes','Code EPCI','EPCI','Longitude','Latitude','Revenu annuel â‚¬',
                             'Nb jours rÃ©servÃ©s','Nb total jours de disponibilitÃ©']]
bnb2023.columns = ['INSEE','Communes','Code EPCI','EPCI','Longitude','Latitude','Revenu annuel €',
                             'Nb jours réservés','Nb total jours de disponibilité']
print("Chargement des données Airbnb terminé.")

décret = pd.read_excel("data/décret/2023_decret.xlsx", sheet_name="3")

def tables():
    return bnb2022, bnb2023, décret
