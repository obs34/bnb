def verif_col(df):
    """
    Vérifie les colonnes d'un dataframe
    :param df: dataframe à vérifier
    :return: None
    """
    # Vérifie si le dataframe est vide
    if df.empty:
        print("Le dataframe est vide")
        return

    # Vérifie si le dataframe contient des colonnes vides
    empty_columns = df.columns[df.isnull().all()]
    num_empty_columns = len(empty_columns)
    empty_column_names = list(empty_columns)
    if num_empty_columns > 0:
        print(f"Le dataframe contient {num_empty_columns} colonnes vides : {empty_column_names}")
    else:
        print("Le dataframe ne contient pas de colonnes vides")


def verif(df_original, code='code'):
    df = df_original.copy(deep=True)
    df[code] = df[code].astype(str)
    # département
    df_dep = df[df[code].str.startswith('34') & (df[code].str.len()==2)]
    if len(df_dep)==0:
        print("Hérault non présent")
    else:
        print(len(df_dep), "départements présents")
        verif_col(df_dep)
    # EPCI
    df_epci = df[~(df[code].str.startswith('34')) & (df[code].str.len()==9)]
    if len(df_epci) == 0:
        print("EPCI non présentes")
    else:
        print(len(df_epci), "EPCI présentes")
        verif_col(df_epci)
    # commune
    df_commune = df[(df[code].str.startswith('34')) & (df[code].str.len()==5)]
    if len(df_commune) == 0:
        print("Commune non présentes")
    else:
        print(len(df_commune), "communes présentes")
        verif_col(df_commune)
    # IRIS
    df_iris = df[(df[code].str.startswith('34')) & (df[code].str.len()==9)]
    if len(df_iris) == 0:
        print("IRIS non présentes")
    else:
        print(len(df_iris), "IRIS présentes")
        verif_col(df_iris)