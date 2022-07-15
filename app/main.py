from pydantic import BaseModel, conlist
from fastapi import FastAPI
from joblib import load
from typing import List
from src import __version__


class MilkInput(BaseModel):
    data: List[conlist(float, min_items=48, max_items=48)]


app = FastAPI(title="Milk price endpoint", description="API to predict milk price", version="0.1.0")


@app.get("/")
async def root():
    return {"message": "Milk price predictor"}


@app.post('/milk-price/predict', tags=["predictions"])
async def get_prediction(milk_input: MilkInput):
    model = load(f"pipeline-model-{__version__}.joblib")
    if not model:
        raise Exception(f"Model pipeline-model-{__version__}.pkl not initialized correctly")
    prediction = model.predict(milk_input.data).tolist()
    return {"prediction": prediction}
