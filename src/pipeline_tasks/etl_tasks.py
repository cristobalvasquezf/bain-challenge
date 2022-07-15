import prefect
import pandas as pd
from prefect import task
from src.preprocess.preprocess_milk import load_and_preprocess_milk
from src.preprocess.preprocess_bank import load_and_clean_central_bank_data, process_pib_columns, \
    process_imacec_columns, process_iv, join_preprocess_bank_data
from src.preprocess.preprocess_precipitations import load_and_preprocess_precipitations
from src.preprocess.preprocess import join_precipitations_milk_data, join_preprocess_milk_bank_data


@task
def load_and_preprocess_precipitations_data_task(file_precipitation_path: str) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("Loading and preprocessing precipitations data")
    data_precipitations = load_and_preprocess_precipitations(file_precipitation_path)
    logger.info("Precipitation data loaded and cleaned successfully")
    logger.debug(f"Data columns after load_and_preprocess_precipitations_data_task: {data_precipitations.columns}")
    logger.debug(f"Data shape after load_and_preprocess_precipitations_data_task: {data_precipitations.shape}")
    return data_precipitations


@task
def load_and_preprocess_milk_data_task(file_milk_path: str) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("Loading and preprocessing milk data")
    data_milk = load_and_preprocess_milk(file_milk_path)
    logger.info("milk data loaded and cleaned successfully")
    logger.debug(f"Data columns after load_and_preprocess_milk_data_task: {data_milk.columns}")
    logger.debug(f"Data shape after load_and_preprocess_milk_data_task: {data_milk.shape}")
    return data_milk


@task
def load_and_preprocess_central_bank_data_task(file_bank_path: str) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("Loading and preprocessing bank data")
    df = load_and_clean_central_bank_data(file_bank_path)
    logger.info("bank data loaded and cleaned successfully")
    logger.debug(f"Data columns after load_and_preprocess_central_bank_data_task: {df.columns}")
    logger.debug(f"Data shape after load_and_preprocess_central_bank_data_task: {df.shape}")
    return df


@task
def process_pib_columns_task(df: pd.DataFrame) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    logger.info("Preprocessing bank data for pib columns")
    df = process_pib_columns(df)
    logger.info("Bank data preprocessing for pib columns successful")
    logger.debug(f"Data columns after process_pib_columns_task: {df.columns}")
    logger.debug(f"Data shape after process_pib_columns_task: {df.shape}")
    return df


@task
def process_imacec_columns_task(df: pd.DataFrame) -> pd.DataFrame:
    df = process_imacec_columns(df)
    logger = prefect.context.get("logger")
    logger.debug(f"Data columns after process_imacec_columns_task: {df.columns}")
    logger.debug(f"Data shape after process_imacec_columns_task: {df.shape}")
    return df


@task
def process_iv_task(df: pd.DataFrame) -> pd.DataFrame:
    df = process_iv(df)
    logger = prefect.context.get("logger")
    logger.debug(f"Data columns after process_iv_task: {df.columns}")
    logger.debug(f"Data shape after process_iv_task: {df.shape}")
    return df


@task
def join_preprocess_bank_data_task(df_pib: pd.DataFrame, df_imacec: pd.DataFrame, df_iv: pd.DataFrame) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    df_joined = join_preprocess_bank_data(df_pib, df_imacec, df_iv)
    logger.debug(f"Data columns after join_preprocess_bank_data_task: {df_joined.columns}")
    logger.debug(f"Data shape after join_preprocess_bank_data_task: {df_joined.shape}")
    return df_joined


@task
def join_precipitations_milk_data_task(df_precipitations: pd.DataFrame, df_milk: pd.DataFrame) -> pd.DataFrame:
    logger = prefect.context.get("logger")
    df_milk_price_by_pp = join_precipitations_milk_data(df_precipitations, df_milk)
    logger.debug(f"Data columns after join_precipitations_milk_data_task: {df_milk_price_by_pp.columns}")
    logger.debug(f"Data shape after join_precipitations_milk_data_task: {df_milk_price_by_pp.shape}")
    return df_milk_price_by_pp


@task
def join_preprocess_milk_bank_data_task(df_bank: pd.DataFrame, df_milk_price_pp: pd.DataFrame) -> pd.DataFrame:
    df_milk_price_pp_pib = join_preprocess_milk_bank_data(df_bank, df_milk_price_pp)
    logger = prefect.context.get("logger")
    logger.debug(f"Data columns after join_preprocess_milk_bank_data_task: {df_milk_price_pp_pib.columns}")
    logger.debug(f"Data shape after join_preprocess_milk_bank_data_task: {df_milk_price_pp_pib.shape}")
    return df_milk_price_pp_pib
