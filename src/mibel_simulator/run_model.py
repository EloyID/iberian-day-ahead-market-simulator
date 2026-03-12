from __future__ import annotations

import warnings
from collections.abc import Mapping

from pyomo.environ import Suffix
from pyomo.opt import SolverFactory

from mibel_simulator.make_model import make_model

_SOLVER_ALLOWED_OPTIONS: dict[str, set[str]] = {
    "gurobi": {
        "MIPGap",
        "TimeLimit",
        "Threads",
        "MIPFocus",
        "Method",
        "Presolve",
    },
    "cbc": {"seconds", "ratio", "threads"},
    "glpk": {"tmlim", "mipgap"},
}


def _normalize_solver_name(solver_factory_type: str) -> str:
    solver_name = solver_factory_type.lower()
    if "gurobi" in solver_name:
        return "gurobi"
    if solver_name.startswith("cbc"):
        return "cbc"
    if solver_name.startswith("glpk"):
        return "glpk"
    return solver_name


def _default_solver_options(solver_factory_type: str) -> dict:
    # Keep historical behavior only for gurobi-like solvers.
    if _normalize_solver_name(solver_factory_type) == "gurobi":
        return {"MIPGap": 0}
    return {}


def _apply_solver_options(
    opt,
    solver_factory_type: str,
    solver_options: Mapping[str, object] | None,
) -> None:
    merged_options: dict[str, object] = {
        **_default_solver_options(solver_factory_type),
        **dict(solver_options or {}),
    }
    normalized_solver = _normalize_solver_name(solver_factory_type)
    allowed_options = _SOLVER_ALLOWED_OPTIONS.get(normalized_solver)

    for option_name, option_value in merged_options.items():
        if allowed_options is not None and option_name not in allowed_options:
            warnings.warn(
                f"Skipping incompatible solver option '{option_name}' for solver '{solver_factory_type}'.",
                stacklevel=2,
            )
            continue
        opt.options[option_name] = option_value


def _build_solver(
    solver_factory_type: str,
    solver_options: Mapping[str, object] | None,
):
    opt = SolverFactory(solver_factory_type)
    if opt is None:
        raise ValueError(
            f"Unknown solver '{solver_factory_type}'. Please provide a valid Pyomo SolverFactory name."
        )
    if not opt.available():
        _SOLVER_INSTALL_HINTS: dict[str, str] = {
            "gurobi": (
                "Install gurobipy (`pip install gurobipy`) and configure a valid Gurobi licence. "
                "See https://www.gurobi.com/documentation/ for licence setup."
            ),
            "cbc": (
                "Install the CBC binary. "
                "On Debian/Ubuntu: `sudo apt-get install coinor-cbc`. "
                "On macOS: `brew install cbc`. "
                "On Windows: download from https://github.com/coin-or/Cbc/releases."
            ),
            "glpk": (
                "Install the GLPK binary. "
                "On Debian/Ubuntu: `sudo apt-get install glpk-utils`. "
                "On macOS: `brew install glpk`. "
                "On Windows: download from https://www.gnu.org/software/glpk/."
            ),
        }
        hint = _SOLVER_INSTALL_HINTS.get(
            _normalize_solver_name(solver_factory_type),
            f"Ensure the '{solver_factory_type}' binary is installed and accessible in your PATH.",
        )
        raise RuntimeError(
            f"Solver '{solver_factory_type}' is recognised by Pyomo but its executable is not "
            f"available on this system.\n{hint}"
        )
    _apply_solver_options(
        opt=opt,
        solver_factory_type=solver_factory_type,
        solver_options=solver_options,
    )
    return opt


def run_model(
    det_cab,
    capacidad_inter_PBC_pt,
    france_fixed_exchange,
    solver_factory_type: str = "gurobi",
    solver_options: Mapping[str, object] | None = None,
):
    ########################### Load Model ########################
    model = make_model(
        det_cab=det_cab,
        capacidad_inter_PBC_pt=capacidad_inter_PBC_pt,
        france_fixed_exchange=france_fixed_exchange,
    )

    ########################### Solve ########################

    model.dual = Suffix(direction=Suffix.IMPORT)
    opt = _build_solver(
        solver_factory_type=solver_factory_type,
        solver_options=solver_options,
    )
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    model_binaries = model.clone()

    model.v_u_activated_BLOCK_ORDERS.fix()
    model.v_u_activated_SCO_ORDERS.fix()
    model.v_u_activated_FRANCE_EXPORT_BIDS.fix()
    model.v_u_activated_FRANCE_IMPORT_BIDS.fix()

    opt = _build_solver(
        solver_factory_type=solver_factory_type,
        solver_options=solver_options,
    )
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    return model, model_binaries, results
