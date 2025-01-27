import subprocess
import os

def run(cmd: list[str], cwd):
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, encoding='utf8', cwd=cwd)
    ret = p.communicate()
    if p.returncode != 0:
        error = ret[1]
        raise Exception(error)
    return ret[0]

def create():
    fp = os.path.dirname(os.path.realpath(__file__))
    tf_dir = os.path.join(fp, '../../terraform')
    cmd = 'conda run -n tns terraform apply --auto-approve'.split()
    run(cmd, tf_dir)

create()