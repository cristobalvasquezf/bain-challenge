import prefect
import pandas as pd
from typing import Tuple, List
from prefect import task
from src.model.model import model_generation, model_serialization, generate_feature_subsets
from src.containers import Model


@task
def generate_datasets_task(df: pd.DataFrame) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    df_x = df.drop(['Precio_leche'], axis=1)
    df_y = df['Precio_leche']
    features_search = generate_feature_subsets(df_x)
    return [(df_x[features], df_y) for features in features_search]


@task
def model_generation_task(data: Tuple[pd.DataFrame, pd.DataFrame], test_size: float = 0.2) -> Model:
    logger = prefect.context.get("logger")
    logger.info(f"Training features: {data[0].columns}")
    model = model_generation(data, test_size)
    logger.info(f"Best params {model.model.best_params_} for model x")
    return model


@task
def select_best_model_task(models: List[Model]) -> Model:
    logger = prefect.context.get("logger")
    best_model = max(models, key=lambda model: model.r2)
    logger.info(f"test id for model {id(best_model)}")
    logger.info(f"Best hyper parameters {best_model.model.best_params_}")
    logger.info(f"Best rmse {best_model.rmse}")
    logger.info(f"Best r2 {best_model.r2}")
    return best_model


@task
def model_serialization_task(model: Model, save_model_dir: str) -> None:
    logger = prefect.context.get("logger")
    logger.info(f"saving model on {save_model_dir}")
    model_serialization(model.model, save_model_dir)
    logger.info(f"Model serialized successfully")

# TODO remove this
# @task
# def find_best_model_task(df: pd.DataFrame, save_model_path: str, test_size: float = 0.2) -> Model:
#    # we create a list with lists of columns to train models using different data subset
#    df_x = df.drop(['Precio_leche'], axis=1)
#    df_y = df['Precio_leche']
#    features_search = generate_feature_subsets(df_x)
#    logger = prefect.context.get("logger")
#    logger.info(f"Finding best model based on {len(features_search)} datasets")
#
#    models = []
#    for index, cols in enumerate(features_search):
#        logger.info(f"Feature subset_{index} training with features: {cols}")
#        model = model_generation(df_x[cols], df_y, test_size)
#        logger.info(f"Best params for subset_{index} {model.model.best_params_}")
#        models.append(model)
#
#    best_model = max(models, key=lambda model: model.r2)
#    model_serialization(best_model.model, save_model_path)
#    logger.info(f"test id for model {id(best_model)}")
#    logger.info(f"Best model hyper parameters {best_model.model.best_params_}")
#    logger.info(f"Best model rmse {best_model.rmse}")
#    logger.info(f"Best model r2 {best_model.r2}")
#    logger.info(f"Best model features {best_model.features}")
#    return best_model
#
