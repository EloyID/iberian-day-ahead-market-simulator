from pyomo.environ import Suffix
from pyomo.opt import SolverFactory
import pyomo.environ as pyo

from mibel_simulator.make_model import make_model


def run_model(
    det_cab_date,
    capacidad_inter_PT_date,
    france_fixed_exchange,
):
    ########################### Load Model ########################
    model = make_model(
        det_cab_date=det_cab_date,
        capacidad_inter_PT_date=capacidad_inter_PT_date,
        france_fixed_exchange=france_fixed_exchange,
    )

    ########################### Solve ########################

    model.dual = Suffix(direction=Suffix.IMPORT)
    opt = SolverFactory("gurobi")
    opt.options["MIPGap"] = 0
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    model_binaries = model.clone()

    model.v_u_activated_BLOCK_ORDERS.fix()
    model.v_u_activated_SCO_ORDERS.fix()
    model.v_u_activated_FRANCE_EXPORT_BIDS.fix()
    model.v_u_activated_FRANCE_IMPORT_BIDS.fix()

    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    return model, model_binaries, results
