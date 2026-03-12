import pandas as pd
import pytest

import mibel_simulator.clearing_process as clearing_process
import mibel_simulator.run_model as run_model_module
from mibel_simulator import columns as cols


class DummyVar:
    def fix(self):
        return None


class DummyModel:
    def __init__(self):
        self.v_u_activated_BLOCK_ORDERS = DummyVar()
        self.v_u_activated_SCO_ORDERS = DummyVar()
        self.v_u_activated_FRANCE_EXPORT_BIDS = DummyVar()
        self.v_u_activated_FRANCE_IMPORT_BIDS = DummyVar()
        self.OBJ = 1.0

    def clone(self):
        return self


def test_run_model_uses_selected_solver_factory(monkeypatch):
    called_solver_names = []
    seen_options = []

    class DummyOpt:
        def __init__(self):
            self.options = {}

        def available(self):
            return True

        def solve(self, model, tee=False):
            seen_options.append(dict(self.options))
            return {"status": "ok", "tee": tee}

    def fake_solver_factory(name):
        called_solver_names.append(name)
        return DummyOpt()

    monkeypatch.setattr(run_model_module, "make_model", lambda **kwargs: DummyModel())
    monkeypatch.setattr(run_model_module, "SolverFactory", fake_solver_factory)

    _, _, results = run_model_module.run_model(
        det_cab=pd.DataFrame(),
        capacidad_inter_PBC_pt=pd.DataFrame(),
        france_fixed_exchange=None,
        solver_factory_type="gurobi",
    )

    assert called_solver_names == ["gurobi", "gurobi"]
    assert all(options.get("MIPGap") == 0 for options in seen_options)
    assert results["status"] == "ok"


def test_run_model_raises_on_unknown_solver(monkeypatch):
    monkeypatch.setattr(run_model_module, "make_model", lambda **kwargs: DummyModel())
    monkeypatch.setattr(run_model_module, "SolverFactory", lambda name: None)

    with pytest.raises(ValueError, match="Unknown solver"):
        run_model_module.run_model(
            det_cab=pd.DataFrame(),
            capacidad_inter_PBC_pt=pd.DataFrame(),
            france_fixed_exchange=None,
            solver_factory_type="not_a_solver",
        )


def test_run_model_filters_incompatible_solver_options(monkeypatch):
    seen_options = []

    class DummyOpt:
        def __init__(self):
            self.options = {}

        def available(self):
            return True

        def solve(self, model, tee=False):
            seen_options.append(dict(self.options))
            return {"status": "ok", "tee": tee}

    monkeypatch.setattr(run_model_module, "make_model", lambda **kwargs: DummyModel())
    monkeypatch.setattr(run_model_module, "SolverFactory", lambda name: DummyOpt())

    with pytest.warns(UserWarning, match="Skipping incompatible solver option"):
        _, _, _ = run_model_module.run_model(
            det_cab=pd.DataFrame(),
            capacidad_inter_PBC_pt=pd.DataFrame(),
            france_fixed_exchange=None,
            solver_factory_type="cbc",
            solver_options={"seconds": 10, "MIPGap": 0.1},
        )

    assert all("MIPGap" not in options for options in seen_options)
    assert all(options.get("seconds") == 10 for options in seen_options)


def test_iterative_function_forwards_solver_factory_type(monkeypatch):
    captured_solver = {"value": None, "options": None}

    def fake_run_model(
        det_cab,
        capacidad_inter_PBC_pt,
        france_fixed_exchange,
        solver_factory_type,
        solver_options=None,
    ):
        captured_solver["value"] = solver_factory_type
        captured_solver["options"] = solver_options
        return DummyModel(), DummyModel(), {"status": "ok"}

    monkeypatch.setattr(
        clearing_process,
        "filter_paradoxal_orders_from_det_cab",
        lambda det_cab, paradoxal_orders: det_cab,
    )
    monkeypatch.setattr(clearing_process, "run_model", fake_run_model)
    monkeypatch.setattr(
        clearing_process,
        "get_cleared_energy_series",
        lambda model: pd.Series(dtype=float),
    )
    monkeypatch.setattr(
        clearing_process,
        "get_clearing_prices_df",
        lambda model: pd.DataFrame(),
    )
    monkeypatch.setattr(
        clearing_process,
        "get_cleared_paradoxal_orders_summary",
        lambda *args, **kwargs: pd.DataFrame({cols.FLOAT_NET_INCOME: [1.0]}),
    )
    monkeypatch.setattr(
        clearing_process,
        "get_spain_portugal_transmissions",
        lambda model: pd.DataFrame(),
    )
    monkeypatch.setattr(clearing_process.pyo, "value", lambda obj: 1.0)

    _ = clearing_process.iterative_function(
        (
            pd.DataFrame(),
            pd.DataFrame(),
            {cols.IDS_MIC_SCOS: [], cols.IDS_BID_BLOCKS: []},
            None,
            "glpk",
            {"tmlim": 120},
        )
    )

    assert captured_solver["value"] == "glpk"
    assert captured_solver["options"] == {"tmlim": 120}
