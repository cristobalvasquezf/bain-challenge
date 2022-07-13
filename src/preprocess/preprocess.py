import pandas as pd


def join_precipitations_milk_data(df_precipitations: pd.DataFrame, df_milk: pd.DataFrame) -> pd.DataFrame:
    df_precipitations['mes'] = df_precipitations.date.apply(lambda x: x.month)
    df_precipitations['ano'] = df_precipitations.date.apply(lambda x: x.year)
    precio_leche_pp = pd.merge(df_milk, df_precipitations, on=['mes', 'ano'], how='inner')
    precio_leche_pp.drop('date', axis=1, inplace=True)
    return precio_leche_pp


def join_preprocess_milk_bank_data(df_bank: pd.DataFrame, df_milk_price_precipitations: pd.DataFrame) -> pd.DataFrame:
    df_bank['mes'] = df_bank['Periodo'].apply(lambda x: x.month)
    df_bank['ano'] = df_bank['Periodo'].apply(lambda x: x.year)
    precio_leche_pp_pib = pd.merge(df_milk_price_precipitations, df_bank, on=['mes', 'ano'], how='inner')
    precio_leche_pp_pib.drop(
        ['Periodo', 'Indice_de_ventas_comercio_real_no_durables_IVCM', 'mes-ano', 'mes_pal'],
        axis=1, inplace=True)
    return precio_leche_pp_pib
