import pandas as pd


def load_and_preprocess_precipitations(file: str) -> pd.DataFrame:
    """
    This functions process precipitations data applying the follow treatment:
    - parse date in the format year-month-day
    - sort by date column
    - drop duplicates
    - drop nan values

    :param file: path of precipitaciones.csv file.
    :return: Dataframe preocessed.
    """
    df_precipitations = pd.read_csv(file)
    df_precipitations["date"] = pd.to_datetime(df_precipitations["date"], format='%Y-%m-%d')
    df_precipitations_sorted = df_precipitations.sort_values(by="date", ascending=True).reset_index(drop=True)
    df_precipitations_sorted.drop_duplicates(subset='date', inplace=True)
    df_precipitaciones_sorted = df_precipitations_sorted[~df_precipitations_sorted["date"].isna()]
    return df_precipitaciones_sorted
