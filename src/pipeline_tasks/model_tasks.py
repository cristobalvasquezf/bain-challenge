import os
import joblib
import prefect
import pandas as pd
from typing import Tuple, List
from prefect import task
from src.model.model import model_generation, generate_feature_subsets
from src.containers import Model
from src import __version__
from pathlib import Path


@task
def generate_datasets_task(df: pd.DataFrame) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    df_x = df.drop(['Precio_leche'], axis=1)
    df_y = df['Precio_leche']
    features_search = generate_feature_subsets(df_x)

    logger = prefect.context.get("logger")
    logger.debug(f"All available features {df_x.columns}")
    logger.debug(f"Features search space: {features_search}")
    [logger.debug(f"Dataset {index} generated with features: {features}") for index, features in
     enumerate(features_search)]
    return [(df_x[features], df_y) for features in features_search]


@task
def model_generation_task(data: Tuple[pd.DataFrame, pd.DataFrame], test_size: float = 0.2) -> Model:
    logger = prefect.context.get("logger")
    logger.info(f"Training features: {data[0].columns}")
    model = model_generation(data, test_size)
    return model


@task
def select_best_model_task(models: List[Model]) -> Model:
    logger = prefect.context.get("logger")
    best_model = max(models, key=lambda model: model.r2)
    logger.info(f"Best hyper parameters {best_model.model.best_params_}")
    logger.info(f"Best rmse {best_model.rmse}")
    logger.info(f"Best r2 {best_model.r2}")
    return best_model


@task
def model_serialization_task(model: Model, save_model_dir: str) -> None:
    """

    :param model: model to serialize
    :param save_model_dir: a relative path where to store the model.
                           If the provided path doesn't exists a one will be created.
    :return:
    """
    logger = prefect.context.get("logger")
    logger.info(f"Saving model on {save_model_dir}")
    logger.info(f"Model version {__version__}")
    if not os.path.exists(save_model_dir):
        os.makedirs(os.path.join(os.getcwd(), save_model_dir))
    model_path = os.path.join(save_model_dir, f"pipeline-model-{__version__}.joblib")
    joblib.dump(model.model.best_estimator_, Path(model_path))
    logger.info(f"Model serialized successfully")
