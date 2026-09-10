import glob, json, os, sys, time
sys.path.insert(0, glob.glob("/opt/venv/lib/python*/site-packages")[0])

# Cold, first thing after boot: the production import.
t = time.perf_counter()
import pandas
total_ms = (time.perf_counter() - t) * 1000.0

deps, root, builtin = [], [], 0
for name, mod in list(sys.modules.items()):
    f = getattr(mod, "__file__", None)
    if not f:
        builtin += 1
    elif f.startswith("/opt/venv"):
        deps.append(f)
    else:
        root.append(f)

def size(paths):
    n = 0
    for p in paths:
        try:
            n += os.stat(p).st_size
        except OSError:
            pass
    return n

print(json.dumps({
    "import_pandas_ms": round(total_ms, 1),
    "modules_on_deps_disk": len(deps),
    "modules_on_virtiofs_root": len(root),
    "modules_builtin": builtin,
    "bytes_on_deps_disk": size(deps),
    "bytes_on_virtiofs_root": size(root),
}))
