import prefect
import pandas as pd
import src.utils as utils


def process_pib_columns(df: pd.DataFrame):
    """
    This function process PIB columns applying the follow treatment:
    - drop nan values
    - parse values as integers
    - sort by Periodo column
    :param df: dataframe with central bank data.
    :return: Dataframe processed.
    """
    cols_pib = [x for x in list(df.columns) if 'PIB' in x]
    cols_pib.append('Periodo')
    logger = prefect.context.get("logger")
    logger.debug(f"Bank data pib columns {cols_pib}")
    df_pib = df[cols_pib].dropna(how='any', axis=0)
    period_column = df_pib["Periodo"]
    # apply the conversion to all columns except Periodo which is the last one in cols_pib list
    df_pib = df_pib[cols_pib[:-1]].applymap(utils.convert_int)
    df_pib["Periodo"] = period_column
    df_pib = df_pib.sort_values(by='Periodo', ascending=True)
    return df_pib


def process_imacec_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    This function process Imacec related columns applying the follow treatment:
    - drop nan values
    - capping values based on utils.to_100 function
    - sort by Periodo column
    :param df: dataframe with central bank data.
    :return: Dataframe processed.
    """
    cols_imacec = [x for x in list(df.columns) if 'Imacec' in x]
    cols_imacec.extend(['Periodo'])
    logger = prefect.context.get("logger")
    logger.debug(f"Bank data imacec columns {cols_imacec}")
    df_imacec = df[cols_imacec].dropna(how='any', axis=0)
    for col in cols_imacec:
        if col == 'Periodo':
            continue
        else:
            df_imacec[col] = df_imacec[col].apply(utils.to_100)
            assert (df_imacec[col].max() > 100)
            assert (df_imacec[col].min() > 30)
    df_imacec = df_imacec.sort_values(by='Periodo', ascending=True)
    return df_imacec


def process_iv(df: pd.DataFrame) -> pd.DataFrame:
    """
    This functions drop nan values based on columns:
    - Indice_de_ventas_comercio_real_no_durables_IVCM
    - Periodo
    Then sort the df based on Periodo column in ascending order and applies utils.to_100 function
    :param df: dataframe with central bank data.
    :return: Dataframe processed.
    """
    cols_iv = ['Indice_de_ventas_comercio_real_no_durables_IVCM', 'Periodo']
    df_iv = df[cols_iv]
    df_iv = df_iv.dropna()
    df_iv = df_iv.sort_values(by='Periodo', ascending=True)
    df_iv['num'] = df_iv.Indice_de_ventas_comercio_real_no_durables_IVCM.apply(
        lambda x: utils.to_100(x))
    return df_iv


def load_and_clean_central_bank_data(file: str) -> pd.DataFrame:
    """
    This functions process central bank data applying the follow treatment:
    - parse date in the format year-month-day
    - drop duplicates
    - drop nan values
    :param file: path of banco_central.csv file.
    :return: Dataframe processed and cleaned.
    """
    df_bank = pd.read_csv(file)
    logger = prefect.context.get("logger")
    logger.debug(f"Bank dataset original columns {df_bank.columns}")
    df_bank["Periodo"] = df_bank["Periodo"].apply(utils.match_date)
    # TODO: in this step errors parameter is setted as coerce and it could be an issue.
    #  Check date format for this dataset.
    #  2020-13-01 this date is present in data and breaks the expected format year-month-day.
    #  it could be an outlier.
    df_bank["Periodo"] = pd.to_datetime(df_bank["Periodo"], format='%Y-%m-%d', errors="coerce")
    df_bank.drop_duplicates(subset='Periodo', inplace=True)
    df_bank_clean = df_bank[~df_bank["Periodo"].isna()]
    return df_bank_clean


def join_preprocess_bank_data(df_pib: pd.DataFrame, df_imacec: pd.DataFrame, df_iv: pd.DataFrame) -> pd.DataFrame:
    """
    This functions generates the dataframe that will be used for model training.
    It applies two joins based on dataframes received.

    :param df_pib: dataframe generated by process_pib_columns
    :param df_imacec: dataframe generated by process_pib_columns
    :param df_iv: dataframe generated by process_iv
    :return: joined dataframe that will be used for model training
    """
    df_pib_imacec = pd.merge(df_pib, df_imacec, on='Periodo', how='inner')
    df_pib_imacec_iv = pd.merge(df_pib_imacec, df_iv, on='Periodo', how='inner')
    return df_pib_imacec_iv
