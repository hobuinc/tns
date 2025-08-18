import pytest
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
test_path = 'test_lambdas.py::test_comp'
path = os.path.join(dir_path, test_path)
pytest.main([path])