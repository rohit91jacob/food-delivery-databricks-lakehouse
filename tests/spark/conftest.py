import importlib.util

# Without the `spark` dependency group these modules can't even be imported; skip collecting them.
if importlib.util.find_spec("pyspark") is None or importlib.util.find_spec("delta") is None:
    collect_ignore_glob = ["test_*.py"]
