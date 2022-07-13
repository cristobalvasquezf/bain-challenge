import time
from prefect import Flow, Parameter
from src.pipeline_tasks.etl_tasks import load_and_preprocess_precipitations_data_task, \
    load_and_preprocess_milk_data_task, \
    load_and_preprocess_central_bank_data_task, process_pib_columns_task, process_imacec_columns_task,  \
    process_iv_task, join_precipitations_milk_data_task, join_preprocess_milk_bank_data_task, \
    join_preprocess_bank_data_task
from src.pipeline_tasks.model_tasks import generate_datasets_task, model_generation_task, select_best_model_task, \
    model_serialization_task
from prefect.executors import LocalDaskExecutor

with Flow("model-flow") as flow:
    # Parameters definition
    file_precipitation_path = Parameter("file_precipitation_path", default="./data/precipitaciones.csv")
    file_milk_path = Parameter("file_milk_path", default="./data/precio_leche.csv")
    file_bank_path = Parameter("file_bank_path", default="./data/banco_central.csv")
    save_model_dir = Parameter("save_model_dir", default="artifacts")

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

start = time.perf_counter()
flow.run(
    parameters={
        "file_bank_path": "./data/banco_central.csv",
        "file_precipitation_path": "./data/precipitaciones.csv",
        "file_milk_path": "./data/precio_leche.csv",
        "save_model_dir": "artifacts"
    }
    # , executor=LocalDaskExecutor(scheduler="threads", num_workers=8)
)
end = time.perf_counter()

flow.visualize()
print(f"time {end - start}")
a = [2.01400000e+03, 2.19030000e+02, 2.00000000e+00, 1.68885621e+00,
     8.38985390e+00, 1.78030705e+01, 7.29517830e+00, 3.01593178e+00,
     2.21861968e+01, 4.31574106e+01, 6.52280104e+01, 5.89162366e+08,
     5.30628153e+08, 1.18979099e+08, 1.07686125e+08, 1.12929737e+08,
     1.17324454e+08, 3.26848287e+08, 1.51002582e+08, 2.47771010e+07,
     5.84892910e+07, 9.85263519e+08, 9.87746023e+08, 1.69050011e+08,
     6.37721806e+08, 1.82004135e+08, 2.76725914e+08, 6.21419201e+08,
     1.06567859e+08, 2.15199970e+07, 5.30525285e+08, 3.29424874e+08,
     6.02969763e+08, 1.16462931e+08, 8.29465384e+08, 6.90060504e+08,
     5.23184092e+08, 9.85454361e+08, 1.07497828e+08, 9.35000000e+01,
     9.82000000e+01, 9.42000000e+01, 9.18000000e+01, 1.07454000e+02,
     1.02175000e+02, 8.88000000e+01, 9.37100000e+01, 9.34000000e+01,
     9.28000000e+01]
