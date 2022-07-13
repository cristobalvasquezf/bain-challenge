from pydantic import BaseModel, conlist
from fastapi import FastAPI
from joblib import load
from src import __version__, model


class MilkInput(BaseModel):
    # data: List[conlist(float, min_items=49, max_items=49)]
    data: list


from fastapi import APIRouter

app_iris_predict_v1 = APIRouter()


@app_iris_predict_v1.get("/")
async def root():
    return {"message": "Milk price predictor"}


@app_iris_predict_v1.post('/milk_price/predict', tags=["predictions"])
async def get_prediction(milk_input: MilkInput):
    data = dict(milk_input)['data']
    prediction = model.model.predict(data).tolist()
    log_proba = model.model.predict_log_proba(data).tolist()
    return {"prediction": prediction,
            "log_proba": log_proba}


app = FastAPI(title="Milk price endpoint", description="API to predict milk price", version="0.1.0")


@app.on_event('startup')
def load_model():
    model = load(f"../artifacts/pipeline-model-{__version__}.pkl")


app.include_router(app_iris_predict_v1, prefix='/v1')
