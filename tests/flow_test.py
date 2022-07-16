import os
import shutil
import unittest
from unittest.mock import patch
from pipeline import flow_definition, run_pipeline
from src.model.model import grid_definition
from src import __version__


def parameters_side_effect():
    return {
        "file_bank_path": "../data/banco_central.csv",
        "file_precipitation_path": "../data/precipitaciones.csv",
        "file_milk_path": "../data/precio_leche.csv",
        "save_model_dir": "temp"
    }


class FlowTestSuite(unittest.TestCase):
    def setUp(self) -> None:
        self.num_tasks = 17
        self.flow = flow_definition("milk-flow-test")
        self.tasks = ['load_and_preprocess_milk_data_task',
                      'load_and_preprocess_central_bank_data_task', 'process_pib_columns_task',
                      'process_imacec_columns_task', 'join_precipitations_milk_data_task', 'select_best_model_task',
                      'load_and_preprocess_precipitations_data_task', 'process_iv_task',
                      'join_preprocess_bank_data_task', 'join_preprocess_milk_bank_data_task',
                      'generate_datasets_task',
                      'model_generation_task', 'model_serialization_task']
        self.parameters = ['file_precipitation_path', 'file_milk_path', 'file_bank_path', 'save_model_dir']

    def test_check_flow_tasks(self):
        flow_tasks_names = list(map(lambda x: x.name, self.flow.tasks))
        self.assertTrue("load_and_preprocess_precipitations_data_task" in flow_tasks_names)
        self.assertTrue("load_and_preprocess_milk_data_task" in flow_tasks_names)
        self.assertTrue("load_and_preprocess_central_bank_data_task" in flow_tasks_names)
        self.assertTrue("process_pib_columns_task" in flow_tasks_names)
        self.assertTrue("process_imacec_columns_task" in flow_tasks_names)
        self.assertTrue("process_iv_task" in flow_tasks_names)
        self.assertTrue("join_precipitations_milk_data_task" in flow_tasks_names)
        self.assertTrue("join_preprocess_milk_bank_data_task" in flow_tasks_names)
        self.assertTrue("join_preprocess_bank_data_task" in flow_tasks_names)
        self.assertTrue("generate_datasets_task" in flow_tasks_names)
        self.assertTrue("model_generation_task" in flow_tasks_names)
        self.assertTrue("select_best_model_task" in flow_tasks_names)
        self.assertTrue("load_and_preprocess_precipitations_data_task" in flow_tasks_names)
        self.assertTrue("model_serialization_task" in flow_tasks_names)
        self.assertEqual(self.num_tasks, len(self.flow.tasks))

    @patch("pipeline.parameters_definition")
    def test_flow_run_state(self, parameters_definition_mock):
        parameters_definition_mock.return_value = parameters_side_effect()
        pipeline_state = run_pipeline()
        tasks_results_dict = {
            str(key).replace("<Parameter:", "").replace(">", "").replace("<Task:", "").strip(): value
            for key, value in pipeline_state.result.items()}

        # model_generation_task contains another message on the result then is removed in order
        # to check all other tasks success result
        model_generation_task_result = tasks_results_dict.pop("model_generation_task")
        for task_name, result in tasks_results_dict.items():
            self.assertTrue("succeeded" in result.message)

        # tests to check dataframe shapes
        self.assertTrue(
            tasks_results_dict.get("load_and_preprocess_precipitations_data_task").result.shape == (496, 11))
        self.assertTrue(
            tasks_results_dict.get("load_and_preprocess_milk_data_task").result.shape == (506, 5))
        self.assertTrue(
            tasks_results_dict.get("load_and_preprocess_central_bank_data_task").result.shape == (611, 85))
        self.assertTrue(
            tasks_results_dict.get("process_pib_columns_task").result.shape == (93, 29))
        self.assertTrue(
            tasks_results_dict.get("process_iv_task").result.shape == (82, 3))
        self.assertTrue(
            tasks_results_dict.get("process_imacec_columns_task").result.shape == (298, 10))
        self.assertTrue(
            tasks_results_dict.get("join_precipitations_milk_data_task").result.shape == (496, 13))
        self.assertTrue(
            tasks_results_dict.get("join_preprocess_bank_data_task").result.shape == (81, 42))
        self.assertTrue(
            len(tasks_results_dict.get("generate_datasets_task").result) == 1)
        self.assertTrue(
            tasks_results_dict.get("generate_datasets_task").result[0][0].shape == (76, 48))
        self.assertTrue(
            tasks_results_dict.get("generate_datasets_task").result[0][1].shape == (76,))
        self.assertTrue(
            tasks_results_dict.get("select_best_model_task").result.r2 == 0.7402504436757733)
        self.assertTrue(
            tasks_results_dict.get("select_best_model_task").result.rmse == 115.50948966963995)
        expected_model_features = ['ano', 'mes', 'Coquimbo', 'Valparaiso', 'Metropolitana_de_Santiago',
                                   'Libertador_Gral__Bernardo_O_Higgins', 'Maule', 'Biobio', 'La_Araucania', 'Los_Rios',
                                   'PIB_Agropecuario_silvicola', 'PIB_Pesca', 'PIB_Mineria', 'PIB_Mineria_del_cobre',
                                   'PIB_Otras_actividades_mineras', 'PIB_Industria_Manufacturera', 'PIB_Alimentos',
                                   'PIB_Bebidas_y_tabaco', 'PIB_Textil', 'PIB_Maderas_y_muebles', 'PIB_Celulosa',
                                   'PIB_Refinacion_de_petroleo', 'PIB_Quimica',
                                   'PIB_Minerales_no_metalicos_y_metalica_basica', 'PIB_Productos_metalicos',
                                   'PIB_Electricidad', 'PIB_Construccion', 'PIB_Comercio', 'PIB_Restaurantes_y_hoteles',
                                   'PIB_Transporte', 'PIB_Comunicaciones', 'PIB_Servicios_financieros',
                                   'PIB_Servicios_empresariales', 'PIB_Servicios_de_vivienda',
                                   'PIB_Servicios_personales', 'PIB_Administracion_publica', 'PIB_a_costo_de_factores',
                                   'PIB', 'Imacec_empalmado', 'Imacec_produccion_de_bienes', 'Imacec_minero',
                                   'Imacec_industria', 'Imacec_resto_de_bienes', 'Imacec_comercio', 'Imacec_servicios',
                                   'Imacec_a_costo_de_factores', 'Imacec_no_minero', 'num']
        self.assertTrue(
            tasks_results_dict.get("select_best_model_task").result.features == expected_model_features)
        self.assertTrue(
            len(tasks_results_dict.get("select_best_model_task").result.features) == 48)
        self.assertTrue(os.path.exists(f"temp/pipeline-model-{__version__}.joblib"))

        expected_grid_search = model_generation_task_result.result[0].model
        expected_best_params = {'model__alpha': 1, 'poly__degree': 2, 'selector__k': 3}

        self.assertEqual(expected_grid_search.best_estimator_.n_features_in_, 48)
        self.assertEqual(len(expected_grid_search.best_estimator_.steps), 4)
        self.assertEqual(expected_grid_search.best_params_, expected_best_params)
        self.assertEqual(expected_grid_search.best_score_, 0.551716169890406)
        self.assertTrue(expected_grid_search.param_grid, grid_definition())
        self.assertTrue(expected_grid_search.scoring, 'r2')
        shutil.rmtree(os.path.join(parameters_side_effect().get("save_model_dir")))
