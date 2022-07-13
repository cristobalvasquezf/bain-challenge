import pandas as pd
import locale

locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')


def load_and_preprocess_milk(file: str) -> pd.DataFrame:
    """
    This functions reads precio_leche.csv dataset for cleaning an preprocessing.
    :param file: precio_leche data set path.
    :return: A processed and clean dataframe
    """
    df_milk = pd.read_csv(file)
    df_milk.rename(columns={'Anio': 'ano', 'Mes': 'mes_pal'}, inplace=True)
    df_milk['mes'] = pd.to_datetime(df_milk['mes_pal'], format='%b')
    df_milk['mes'] = df_milk['mes'].apply(lambda x: x.month)
    df_milk['mes-ano'] = df_milk.apply(lambda x: f'{x.mes}-{x.ano}', axis=1)
    return df_milk
