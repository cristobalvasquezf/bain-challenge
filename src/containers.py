from dataclasses import dataclass
from typing import List
from sklearn.model_selection import GridSearchCV


@dataclass
class Model:
    # implement model id
    model: GridSearchCV
    rmse: float
    r2: float
    features: List[str]
