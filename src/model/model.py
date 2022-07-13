import os
import joblib
import json
import prefect
import pandas as pd
import numpy as np
from typing import List, Tuple
from sklearn.model_selection import train_test_split
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler, PolynomialFeatures
from sklearn.pipeline import Pipeline
from sklearn.linear_model import Ridge
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.feature_selection import SelectKBest, mutual_info_regression
from src.containers import Model
from src import __version__

np.random.seed(0)


def generate_train_test_data(df_x: pd.DataFrame, df_y: pd.DataFrame, test_size: float) -> \
        Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    X_train, X_test, y_train, y_test = train_test_split(df_x, df_y, test_size=test_size, random_state=42)
    return X_train, X_test, y_train, y_test


def model_pipeline_definition() -> Pipeline:
    pipe = Pipeline([('scale', StandardScaler()),
                     ('selector', SelectKBest(mutual_info_regression)),
                     ('poly', PolynomialFeatures()),
                     ('model', Ridge())])
    return pipe


def model_serialization(grid_model: GridSearchCV, save_model_dir: str) -> None:
    model_path = os.path.join(save_model_dir, f"pipeline-model-{__version__}.pkl")
    logger = prefect.context.get("logger")
    logger.info(f"Saving serialized model in {model_path}")
    joblib.dump(grid_model.best_estimator_, model_path)
    logger.info("Model serialized successfully")


# TODO: this grid can be loaded from a config file using a pipeline parameter for easier experimentation
def grid_definition() -> dict:
    grid = {"selector__k": [3, 4, 5, 6, 7, 10],
            "model__alpha": [1, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01],
            "poly__degree": [1, 2, 3, 5, 7]}
    logger = prefect.context.get("logger")
    logger.info(f"Grid space: {json.dumps(grid)}")
    return grid


def hyperparameter_tuning(model_pipeline: Pipeline, x_train: pd.DataFrame, y_train: pd.DataFrame,
                          grid: dict) -> GridSearchCV:
    grid = GridSearchCV(estimator=model_pipeline,
                        param_grid=grid,
                        cv=3,
                        scoring='r2')
    grid.fit(x_train, y_train)
    return grid


def generate_feature_subsets(df_x: pd.DataFrame) -> List[List[str]]:
    df_x_cols_filtered_milk = [column for column in df_x.columns if 'leche' not in column]
    df_x_cols_all = df_x.columns
    return [df_x_cols_filtered_milk, df_x_cols_all]


def model_generation(data: Tuple[pd.DataFrame, pd.DataFrame], test_size: float) -> Model:
    """
    This functions generates a model based on a dataframe.
    Model generations consists in the follow steps:
    - split dataframe received using test_size param for this purpose
    - model pipeline definition
    - grid search for hyper parameters tuning
    - model evaluation imputing rms2 and r2

    :param data: dataframe to be used for model generation.
    :param test_size: size of data test.
    :return: Model dataclass which contains GridSearchCV results, rmse and r2
    """
    df_x, df_y = data
    x_train, x_test, y_train, y_test = generate_train_test_data(df_x, df_y, test_size)
    grid = grid_definition()
    model = model_pipeline_definition()
    grid_model = hyperparameter_tuning(model, x_train, y_train, grid)
    y_predicted = grid_model.predict(x_test)

    rmse, r2 = model_evaluation(y_test, y_predicted)

    return Model(model=grid_model, rmse=rmse, r2=r2, features=list(x_train.columns))


def model_evaluation(y_test, y_predicted) -> Tuple[float, float]:
    rmse = mean_squared_error(y_test, y_predicted)
    r2 = r2_score(y_test, y_predicted)
    return rmse, r2
