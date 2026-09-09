"""Guard: the dual `try/except ImportError` blocks in the bundled serving
modules must import the SAME names in both branches.

SageMaker copies these files flat into `model.tar.gz/code/` and imports them
as top-level siblings, so only the `except ImportError` branch runs there.
The pytest suite always resolves the real `src.ml` package, so it only ever
exercises the `try` branch — an asymmetric `except` branch would ship broken
and never be caught by the rest of the suite.
"""

import ast
import pathlib

# Files bundled into model.tar.gz/code/ by train.py::_SERVING_FILES, plus the
# training-time siblings that share the same dual-import pattern.
_BUNDLED = ["config.py", "features.py", "model.py", "inference.py", "serve.py"]
_TRAINING_SIBLINGS = ["dataset.py", "train.py"]
_ML_DIR = pathlib.Path(__file__).resolve().parents[2] / "src" / "ml"


def _imported_names(body: list[ast.stmt]) -> set[str]:
    names: set[str] = set()
    for node in body:
        if isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
        elif isinstance(node, ast.Import):
            names.update(
                (alias.asname or alias.name).split(".")[0] for alias in node.names
            )
    return names


def _dual_import_blocks(tree: ast.Module) -> list[ast.Try]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Try)
        and any(
            isinstance(h.type, ast.Name) and h.type.id == "ImportError"
            for h in node.handlers
        )
    ]


def _assert_symmetric(filename: str) -> None:
    path = _ML_DIR / filename
    tree = ast.parse(path.read_text(encoding="utf-8"))
    # config.py has no cross-module imports, so no dual block — that's fine;
    # we only care that any block present is symmetric.
    for block in _dual_import_blocks(tree):
        try_names = _imported_names(block.body)
        except_names = _imported_names(block.handlers[0].body)
        assert try_names == except_names, (
            f"{filename}: dual-import branches disagree. "
            f"only in try: {sorted(try_names - except_names)}; "
            f"only in except: {sorted(except_names - try_names)}"
        )


def test_bundled_serving_modules_have_symmetric_dual_imports() -> None:
    for filename in _BUNDLED:
        _assert_symmetric(filename)


def test_training_sibling_modules_have_symmetric_dual_imports() -> None:
    for filename in _TRAINING_SIBLINGS:
        _assert_symmetric(filename)
