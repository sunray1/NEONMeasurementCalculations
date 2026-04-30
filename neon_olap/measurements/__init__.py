import pkgutil
import importlib
import inspect

from . import __path__ 

MEASUREMENTS = {}

for _, module_name, _ in pkgutil.iter_modules(__path__):
    module = importlib.import_module(f"{__name__}.{module_name}")

    for name, func in inspect.getmembers(module, inspect.isfunction):
        if not name.startswith("_"):
            MEASUREMENTS[name] = func