import json
import matplotlib.pyplot as plt

from os.path import isdir, isfile, abspath, expanduser, expandvars

p = abspath('./result.json')
with open(p) as f:
    res = json.load(f)
reskeys = [int(k) for k in sorted(res.keys(), key=int)]

for y in ('insertion', 'find', 'size', 'totalSize'):
    fix, ax = plt.subplots()
    for types in ("int", "number", "test", "long", "decimal", "double"):
        ax.plot(
            reskeys,
            [res[str(k)][types][y] for k in reskeys],
            'x-',
            label=types
        )
    ax.legend()
    ax.set_title(f"{y} by number documents")
    ax.set_xlabel("Number of documents")
    ax.set_ylabel(y)
    plt.savefig(f'{y}_flat.png')
    ax.set_xscale("log", base=10)
    ax.set_yscale("log", base=10)
    plt.savefig(f'{y}.png')
