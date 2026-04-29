import warnings
from collections.abc import Mapping

from pyomo.opt import SolverFactory
from pyomo.environ import Suffix, TransformationFactory

from iberian_day_ahead_market_simulator.make_model import make_model

_SOLVER_ALLOWED_OPTIONS: dict[str, set[str]] = {
    "gurobi": {
        "MIPGap",
        "TimeLimit",
        "Threads",
        "MIPFocus",
        "Method",
        "Presolve",
    },
    "highs": {"mip_rel_gap", "time_limit", "threads"},
}


def _normalize_solver_name(solver_factory_type: str) -> str:
    solver_name = solver_factory_type.lower()
    if "gurobi" in solver_name:
        return "gurobi"
    if solver_name.startswith("highs"):
        return "highs"
    return solver_name


def _default_solver_options(solver_factory_type: str) -> dict:
    # Keep historical behavior only for gurobi-like solvers.
    if _normalize_solver_name(solver_factory_type) == "gurobi":
        return {"MIPGap": 0}
    if _normalize_solver_name(solver_factory_type) == "highs":
        return {"mip_rel_gap": 0, "threads": 1}
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
        raise RuntimeError(
            f"Solver '{solver_factory_type}' is recognised by Pyomo but its executable is not "
            f"available on this system."
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
    model.dual = Suffix(direction=Suffix.IMPORT)

    if solver_factory_type == "highs":
        TransformationFactory("core.relax_integer_vars").apply_to(model)

    opt = _build_solver(
        solver_factory_type=solver_factory_type,
        solver_options=solver_options,
    )
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    return model, model_binaries, results
