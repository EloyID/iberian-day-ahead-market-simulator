from pyomo.environ import Suffix
from pyomo.opt import SolverFactory

from mibel_simulator.make_model import make_model


def run_model(
    det_cab_date,
    capacidad_inter_PT_date,
    exclusive_block_orders_grouped,
):
    ########################### Load Model ########################
    model = make_model(
        det_cab_date=det_cab_date,
        capacidad_inter_PT_date=capacidad_inter_PT_date,
        exclusive_block_orders_grouped=exclusive_block_orders_grouped,
    )

    ########################### Solve ########################

    model.dual = Suffix(direction=Suffix.IMPORT)
    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    model_binaries = model.clone()

    model.v_u_activated_BLOCK_ORDERS.fix()
    model.v_u_activated_SCO_ORDERS.fix()

    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    return model, model_binaries, results
