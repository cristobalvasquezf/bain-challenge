import time
import click
import prefect
from prefect import Flow, Parameter
from prefect.engine import state
from src.pipeline_tasks.etl_tasks import load_and_preprocess_precipitations_data_task, \
    load_and_preprocess_milk_data_task, \
    load_and_preprocess_central_bank_data_task, process_pib_columns_task, process_imacec_columns_task, \
    process_iv_task, join_precipitations_milk_data_task, join_preprocess_milk_bank_data_task, \
    join_preprocess_bank_data_task
from src.pipeline_tasks.model_tasks import generate_datasets_task, model_generation_task, select_best_model_task, \
    model_serialization_task
from prefect.executors import LocalDaskExecutor


def flow_definition(flow_name: str) -> Flow:
    with Flow(flow_name) as flow:
        # Parameters definition
        file_precipitation_path = Parameter("file_precipitation_path")
        file_milk_path = Parameter("file_milk_path")
        file_bank_path = Parameter("file_bank_path")
        save_model_dir = Parameter("save_model_dir")

        # etl stage
        df_precipitations = load_and_preprocess_precipitations_data_task(file_precipitation_path)
        df_milk = load_and_preprocess_milk_data_task(file_milk_path)
        df_bank_clean = load_and_preprocess_central_bank_data_task(file_bank_path)

        df_pib = process_pib_columns_task(df_bank_clean)
        df_imacec = process_imacec_columns_task(df_bank_clean)
        df_iv = process_iv_task(df_bank_clean)
        df_bank = join_preprocess_bank_data_task(df_pib, df_imacec, df_iv)

        df_milk_price_pp = join_precipitations_milk_data_task(df_precipitations, df_milk)
        df_milk_price_pp_pib = join_preprocess_milk_bank_data_task(df_bank, df_milk_price_pp)

        # model training, selection and serialization stage
        datasets = generate_datasets_task(df_milk_price_pp_pib)
        models = model_generation_task.map(datasets)

        best_model = select_best_model_task(models)
        model_serialization_task(best_model, save_model_dir)
    return flow


def parameters_definition() -> dict:
    return {
        "file_bank_path": "./data/banco_central.csv",
        "file_precipitation_path": "./data/precipitaciones.csv",
        "file_milk_path": "./data/precio_leche.csv",
        "save_model_dir": "artifacts"
    }


def run_pipeline(parallel: bool = False, workers: int = 4) -> state:
    flow = flow_definition("milk_price_flow")
    parameters = parameters_definition()
    state = flow.run(
        parameters=parameters,
        executor=None if parallel else LocalDaskExecutor(scheduler="threads", num_workers=workers)
    )
    return state


@click.command()
@click.option("-p", "--parallel", type=bool, default=False, show_default=True,
              help="Boolean flag to run the pipeline in parallel")
@click.option("-w", "--workers", type=int, default=4, show_default=True,
              help="Number of workers to be used in parallel execution")
def run_pipeline_command(parallel: bool, workers: int) -> state:
    """
    Pipeline execution to train milk price model.
    """
    logger = prefect.context.get("logger")
    start = time.perf_counter()
    state = run_pipeline(parallel, workers)
    end = time.perf_counter()
    logger.info(f"Pipeline execution time: {end - start}")
    return state


if __name__ == "__main__":
    run_pipeline_command()
