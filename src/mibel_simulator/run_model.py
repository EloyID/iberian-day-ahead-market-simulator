from mibel_simulator.const import (
    FLOAT_EXPORT_CAPACITY,
    FLOAT_IMPORT_CAPACITY,
    ID_BLOCK_ORDER,
    ID_BLOCK_ORDER_CHILD,
    ID_BLOCK_ORDER_PARENT,
    ID_ORDER,
    ID_SCO,
    ID_SCO_CHILD,
    ID_SCO_PARENT,
    INT_PERIODO,
    CAT_PAIS,
    ID_INDIVIDUAL_BID,
    CAT_BUY_SELL,
    INT_NUM_TRAMO,
    INT_NUM_BLOQ,
    FLOAT_BID_PRICE,
    FLOAT_BID_POWER,
    FLOAT_MAX_POWER,
    FLOAT_MAR,
    FLOAT_MAV,
    SPAIN_ZONE,
    PORTUGAL_ZONE,
)
from pyomo.environ import Binary
import pyomo.environ as pyo
from pyomo.environ import (
    ConcreteModel,
    Set,
    Param,
    Var,
    NonNegativeReals,
    NonPositiveReals,
    Constraint,
    Objective,
    minimize,
    maximize,
    UnitInterval,
    Suffix,
    Any,
)
from pyomo.opt import SolverFactory

from mibel_simulator.make_model import make_model


def run_model(
    det_cab_date,
    capacidad_inter_PT_date,
    parent_child_scos,
    parent_child_bloques,
    exclusive_block_orders_grouped,
    sco_bids_tramo_grouped,
):
    ########################### Load Model ########################
    model = make_model(
        det_cab_date,
        capacidad_inter_PT_date,
        parent_child_scos,
        parent_child_bloques,
        exclusive_block_orders_grouped,
        sco_bids_tramo_grouped,
    )

    ########################### Solve ########################

    model.dual = Suffix(direction=Suffix.IMPORT)
    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    model_binaries = model.clone()

    model.v_u_activated_BLOCK_ORDERS.fix()
    model.v_u_activated_SCOS.fix()

    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    return model, model_binaries, results
