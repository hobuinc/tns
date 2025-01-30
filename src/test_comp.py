import subprocess
import os
import pytest
import Path

from db_lambda import comp_handler

@pytest.fixture()
def terraform_dir():
    cur_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    t_dir = cur_dir / '..' / 'terraform' /
    yield t_dir

@pytest.fixture()
def env_setup(cur_dir):
    # vals = subprocess.run()
    yield cur_dir

def test_comp(env_setup):
    assert env_setup == 4