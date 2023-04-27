from write import BALANCES
import sys

def verify(in_file, out_file):
    with open(in_file, 'r') as inputs_file:
        cmds = [c.strip() for c in inputs_file.readlines()[1:-1]]

    with open(out_file, 'r') as outputs_file:
        outputs = [o.strip() for o in outputs_file.readlines()[1:-1]]

    for i, o in zip(cmds, outputs):
        _, ia = i.split(' ')
        oa, b = o.split(' = ')
        assert ia == oa
        assert BALANCES[ia] == int(b)

log_dir = sys.argv[1]

for i in range(10):
    in_file = f"{log_dir}/{i}.in"
    out_file = f"{log_dir}/{i}.out"

    verify(in_file, out_file)