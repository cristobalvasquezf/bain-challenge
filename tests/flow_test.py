import unittest
from unittest.mock import patch
from pipeline import flow_definition, run_pipeline, parameters_definition


def parameters_side_effect():
    return {
        "file_bank_path": "../data/banco_central.csv",
        "file_precipitation_path": "../data/precipitaciones.csv",
        "file_milk_path": "../data/precio_leche.csv",
        "save_model_dir": "artifacts"
    }


class FlowTestSuite(unittest.TestCase):
    def setUp(self) -> None:
        self.num_tasks = 17
        self.flow = flow_definition("milk-flow-test")

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
        pipeline_state