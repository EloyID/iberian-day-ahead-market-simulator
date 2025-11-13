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
    Constraint,
    Objective,
    minimize,
    maximize,
    UnitInterval,
    Suffix,
    Any,
)
from pyomo.opt import SolverFactory


def run_market_model(
    det_cab_date,
    capacidad_inter_PT_date,
    parent_child_scos,
    parent_child_bloques,
    exclusive_block_orders_grouped,
    sco_bids_tramo_grouped,
):

    det_cab_date_V = det_cab_date.query(f'{CAT_BUY_SELL} == "V"').copy()
    det_cab_date_V_bloque = det_cab_date_V.query(f"{ID_BLOCK_ORDER}.notna()").copy()
    det_cab_date_V_simple = det_cab_date_V.query(
        f"{ID_BLOCK_ORDER}.isna() and {ID_SCO}.isna()"
    ).copy()
    det_cab_date_V_sco = det_cab_date_V.query(f"{ID_SCO}.notna()").copy()
    assert len(det_cab_date_V) == len(det_cab_date_V_bloque) + len(
        det_cab_date_V_simple
    ) + len(det_cab_date_V_sco), "Some offer is not classified as block, simple or sco"
    det_cab_date_C = det_cab_date.query(f'{CAT_BUY_SELL} == "C"').copy()

    parent_child_scos_filtered = parent_child_scos.query(
        f'{ID_ORDER} in @det_cab_date_V_sco["{ID_ORDER}"].unique()'
    )

    model = ConcreteModel("Day-Ahead Market")

    ######################### Sets #########################
    periods = det_cab_date[INT_PERIODO].sort_values().unique().tolist()
    model.PERIODS = Set(initialize=periods)
    countries = [SPAIN_ZONE, PORTUGAL_ZONE]
    model.COUNTRIES = Set(initialize=countries)

    # model.ALL_SELLER_BIDS = Set(initialize=det_cab_date_V[ID_INDIVIDUAL_BID].tolist())
    buyer_bids = det_cab_date_C[ID_INDIVIDUAL_BID].tolist()
    model.BUYER_BIDS = Set(initialize=buyer_bids)
    block_order_bids = det_cab_date_V_bloque[ID_INDIVIDUAL_BID].tolist()
    model.BLOCK_ORDER_BIDS = Set(initialize=block_order_bids)
    simple_seller_bids = det_cab_date_V_simple[ID_INDIVIDUAL_BID].tolist()
    model.SIMPLE_SELLER_BIDS = Set(initialize=simple_seller_bids)
    sco_seller_bids = det_cab_date_V_sco[ID_INDIVIDUAL_BID].tolist()
    model.SCO_SELLER_BIDS = Set(initialize=sco_seller_bids)

    # mav_simple_seller_bids = det_cab_date_V_simple.query("MAV > 0")[
    #     ID_INDIVIDUAL_BID
    # ].tolist()
    # model.MAV_SIMPLE_SELLER_BIDS = Set(initialize=mav_simple_seller_bids)

    block_orders = det_cab_date_V_bloque[ID_BLOCK_ORDER].unique().tolist()
    model.BLOCK_ORDERS = Set(initialize=block_orders)
    sco_tramos = det_cab_date_V_sco[ID_SCO].unique().tolist()
    model.SCO_TRAMOS = Set(initialize=sco_tramos)

    ## Bids subsets
    BUYER_BIDS_PER_PERIOD_AND_COUNTRY = {
        (period, country): det_cab_date_C.query(
            f"{INT_PERIODO} == @period and {CAT_PAIS} == @country"
        )[ID_INDIVIDUAL_BID].tolist()
        for period in periods
        for country in countries
    }
    model.BUYER_BIDS_PER_PERIOD_AND_COUNTRY = Set(
        model.PERIODS,
        model.COUNTRIES,
        initialize=BUYER_BIDS_PER_PERIOD_AND_COUNTRY,
    )

    BLOCK_ORDER_BIDS_BY_BLOCK_AND_PERIOD = {
        (bloque_id, period): det_cab_date_V_bloque.query(
            f"{ID_BLOCK_ORDER} == @bloque_id and {INT_PERIODO} == @period"
        )[ID_INDIVIDUAL_BID].tolist()
        for bloque_id in block_orders
        for period in periods
    }
    model.BLOCK_ORDER_BIDS_BY_BLOCK_AND_PERIOD = Set(
        model.BLOCK_ORDERS,
        model.PERIODS,
        initialize=BLOCK_ORDER_BIDS_BY_BLOCK_AND_PERIOD,
    )

    SIMPLE_SELLER_BIDS_PER_PERIOD_AND_COUNTRY = {
        (period, country): det_cab_date_V_simple.query(
            f"{INT_PERIODO} == @period and {CAT_PAIS} == @country"
        )[ID_INDIVIDUAL_BID].tolist()
        for period in periods
        for country in countries
    }
    model.SIMPLE_SELLER_BIDS_PER_PERIOD_AND_COUNTRY = Set(
        model.PERIODS,
        model.COUNTRIES,
        initialize=SIMPLE_SELLER_BIDS_PER_PERIOD_AND_COUNTRY,
    )

    BLOCK_ORDER_BIDS_BY_BLOCK = {
        bloque_id: det_cab_date_V_bloque.query(f"{ID_BLOCK_ORDER} == @bloque_id")[
            ID_INDIVIDUAL_BID
        ].tolist()
        for bloque_id in block_orders
    }
    model.BLOCK_ORDER_BIDS_BY_BLOCK = Set(
        model.BLOCK_ORDERS, initialize=BLOCK_ORDER_BIDS_BY_BLOCK
    )
    SCO_SELLER_BIDS_PER_PERIOD_AND_COUNTRY = {
        (period, country): det_cab_date_V_sco.query(
            f"{INT_PERIODO} == @period and {CAT_PAIS} == @country"
        )[ID_INDIVIDUAL_BID].tolist()
        for period in periods
        for country in countries
    }
    model.SCO_SELLER_BIDS_PER_PERIOD_AND_COUNTRY = Set(
        model.PERIODS,
        model.COUNTRIES,
        initialize=SCO_SELLER_BIDS_PER_PERIOD_AND_COUNTRY,
    )

    ## Block orders subsets
    BLOCK_ORDERS_BY_COUNTRY = {
        country: det_cab_date_V_bloque.query(f"{CAT_PAIS} == @country")[ID_BLOCK_ORDER]
        .unique()
        .tolist()
        for country in countries
    }
    model.BLOCK_ORDERS_BY_COUNTRY = Set(
        model.COUNTRIES,
        initialize=BLOCK_ORDERS_BY_COUNTRY,
    )

    # The lower the BlockNumber, the parent
    model.BLOQUE_PARENT_CHILDREN = Set(
        initialize=parent_child_bloques[[ID_BLOCK_ORDER_PARENT, ID_BLOCK_ORDER_CHILD]]
        .astype(str)
        .agg("$".join, axis=1),
    )

    model.EXCLUSIVE_BLOCK_ORDERS_GROUPED = Set(
        initialize=exclusive_block_orders_grouped.apply(lambda x: "$".join(x))
    )

    # The lower the NumTramo, the parent
    model.SCO_PARENT_CHILDREN = Set(
        initialize=parent_child_scos_filtered[[ID_SCO_PARENT, ID_SCO_CHILD]]
        .astype(str)
        .agg("$".join, axis=1),
    )

    model.SCO_BIDS_TRAMO_GROUPED = Set(
        initialize=sco_bids_tramo_grouped.apply(lambda x: "$".join(x))
    )

    ######################### Parameters #########################

    model.p_price_min_SIMPLE_SELLERS_BIDS = Param(
        model.SIMPLE_SELLER_BIDS,
        initialize=det_cab_date_V_simple.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_PRICE],
        doc="Minimum price of each generator - simple bids",
    )

    model.p_price_min_BLOCK_ORDERS = Param(
        model.BLOCK_ORDERS,
        initialize=det_cab_date_V_bloque.drop_duplicates(ID_BLOCK_ORDER).set_index(
            ID_BLOCK_ORDER
        )[FLOAT_BID_PRICE],
        doc="Minimum price of each generator - block orders",
    )

    model.p_price_min_SCO_SELLER_BIDS = Param(
        model.SCO_SELLER_BIDS,
        initialize=det_cab_date_V_sco.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_PRICE],
        doc="Minimum price of each generator - SCO bids",
    )

    model.p_price_max_BUYERS_BIDS = Param(
        model.BUYER_BIDS,
        initialize=det_cab_date_C.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_PRICE],
        doc="Maximum price of each consumer",
    )

    model.p_quantity_SIMPLE_SELLER_BIDS = Param(
        model.SIMPLE_SELLER_BIDS,
        initialize=det_cab_date_V_simple.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_POWER],
        within=NonNegativeReals,
        doc="Quantity offered by each generator",
    )

    model.p_quantity_SCO_SELLER_BIDS = Param(
        model.SCO_SELLER_BIDS,
        initialize=det_cab_date_V_sco.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_POWER],
        within=NonNegativeReals,
        doc="Quantity offered by each generator - SCO bids",
    )

    model.p_quantity_BLOCK_ORDER_BIDS = Param(
        model.BLOCK_ORDER_BIDS,
        initialize=det_cab_date_V_bloque.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_POWER],
        within=NonNegativeReals,
        doc="Quantity offered by each generator - block orders",
    )

    model.p_quantity_BUYER_BIDS = Param(
        model.BUYER_BIDS,
        initialize=det_cab_date_C.set_index(ID_INDIVIDUAL_BID)[FLOAT_BID_POWER],
        within=NonNegativeReals,
        doc="Quantity demanded by each consumer",
    )

    model.p_congestion_spain_portugal_exportacion = Param(
        model.PERIODS,
        initialize=capacidad_inter_PT_date.set_index(INT_PERIODO)[
            FLOAT_EXPORT_CAPACITY
        ],
        within=NonNegativeReals,
        doc="Maximum capacity Spain export to Portugal",
    )

    model.p_congestion_spain_portugal_importacion = Param(
        model.PERIODS,
        initialize=capacidad_inter_PT_date.set_index(INT_PERIODO)[
            FLOAT_IMPORT_CAPACITY
        ],
        doc="Maximum capacity Spain import from Portugal (negative value)",
    )

    model.p_MAR = Param(
        model.BLOCK_ORDERS,
        initialize=det_cab_date_V_bloque.drop_duplicates(ID_BLOCK_ORDER).set_index(
            ID_BLOCK_ORDER
        )[FLOAT_MAR],
        within=NonNegativeReals,
        doc="Minimum acceptance ratio (MAR) of each block order",
    )

    model.p_MAV = Param(
        model.SCO_SELLER_BIDS,
        initialize=det_cab_date_V_sco.set_index(ID_INDIVIDUAL_BID)[FLOAT_MAV],
        within=NonNegativeReals,
        doc="Minimum acceptance volume (MAV) of each SCO seller bid",
    )

    model.p_SCO_TRAMO_PER_BID = Param(
        model.SCO_SELLER_BIDS,
        initialize=det_cab_date_V_sco.set_index(ID_INDIVIDUAL_BID)[ID_SCO].to_dict(),
        doc="Tramo identifier for each SCO bid",
        within=Any,
    )

    ######################### Variables #########################

    model.v_x_SIMPLE_SELLER_BIDS = Var(
        model.SIMPLE_SELLER_BIDS,
        within=UnitInterval,
        doc="Quantity ratio sold by each generator",
    )

    model.v_x_BLOCK_ORDERS = Var(
        model.BLOCK_ORDERS,
        within=UnitInterval,
        doc="Quantity ratio sold by each block order",
    )

    model.v_x_BUYER_BIDS = Var(
        model.BUYER_BIDS,
        within=UnitInterval,
        doc="Quantity ratio bought by each consumer",
    )

    model.v_transmission_spain_portugal = Var(
        model.PERIODS,
        doc="Quantity transmitted between Spain and Portugal",
    )

    model.v_x_SCO_SELLER_BIDS = Var(
        model.SCO_SELLER_BIDS,
        within=UnitInterval,
        doc="Quantity ratio sold by each SCO bid",
    )

    model.v_u_activated_SCO_TRAMOS = Var(
        model.SCO_TRAMOS,
        within=Binary,
        doc="Whether a SCO tramo is activated or not",
    )

    model.v_u_activated_BLOCK_ORDERS = Var(
        model.BLOCK_ORDERS,
        within=Binary,
        doc="Whether a block order is activated or not",
    )

    ########################## Objective #########################

    def social_welfare(m):
        return (
            sum(
                m.v_x_BUYER_BIDS[b]
                * m.p_quantity_BUYER_BIDS[b]
                * m.p_price_max_BUYERS_BIDS[b]
                for b in m.BUYER_BIDS
            )
            - sum(
                m.v_x_SIMPLE_SELLER_BIDS[s]
                * m.p_quantity_SIMPLE_SELLER_BIDS[s]
                * m.p_price_min_SIMPLE_SELLERS_BIDS[s]
                for s in m.SIMPLE_SELLER_BIDS
            )
            - sum(
                m.v_x_BLOCK_ORDERS[bo]
                * m.p_quantity_BLOCK_ORDER_BIDS[s]
                * m.p_price_min_BLOCK_ORDERS[bo]
                for bo in m.BLOCK_ORDERS
                for s in m.BLOCK_ORDER_BIDS_BY_BLOCK[bo]
            )
            - sum(
                m.v_x_SCO_SELLER_BIDS[s]
                * m.p_quantity_SCO_SELLER_BIDS[s]
                * m.p_price_min_SCO_SELLER_BIDS[s]
                for s in m.SCO_SELLER_BIDS
            )
        )

    model.OBJ = Objective(rule=social_welfare, sense=maximize)

    ######################## Constraints ########################

    model.c_Congestion_Spain_Portugal = Constraint(
        model.PERIODS,
        rule=lambda m, p: m.v_transmission_spain_portugal[p]
        <= m.p_congestion_spain_portugal_exportacion[p],
        doc="Cannot exceed the transmission capacity between Spain and Portugal",
    )

    model.c_Congestion_Portugal_Spain = Constraint(
        model.PERIODS,
        rule=lambda m, p: m.v_transmission_spain_portugal[p]
        >= m.p_congestion_spain_portugal_importacion[p],
        doc="Cannot exceed the transmission capacity between Portugal and Spain",
    )

    model.c_Balance = Constraint(
        model.PERIODS * model.COUNTRIES,
        rule=lambda m, p, c: sum(
            m.v_x_BUYER_BIDS[b] * m.p_quantity_BUYER_BIDS[b]
            for b in m.BUYER_BIDS_PER_PERIOD_AND_COUNTRY[p, c]
        )
        + (
            m.v_transmission_spain_portugal[p]
            if c == SPAIN_ZONE
            else -m.v_transmission_spain_portugal[p]
        )
        == sum(
            m.v_x_SIMPLE_SELLER_BIDS[s] * m.p_quantity_SIMPLE_SELLER_BIDS[s]
            for s in m.SIMPLE_SELLER_BIDS_PER_PERIOD_AND_COUNTRY[p, c]
        )
        + sum(
            m.v_x_BLOCK_ORDERS[bo] * m.p_quantity_BLOCK_ORDER_BIDS[s]
            for bo in m.BLOCK_ORDERS_BY_COUNTRY[c]
            for s in m.BLOCK_ORDER_BIDS_BY_BLOCK_AND_PERIOD[bo, p]
        )
        + sum(
            m.v_x_SCO_SELLER_BIDS[s] * m.p_quantity_SCO_SELLER_BIDS[s]
            for s in m.SCO_SELLER_BIDS_PER_PERIOD_AND_COUNTRY[p, c]
        ),
        doc="Supply and demand balance in each country and period",
    )

    model.c_Block_Orders_Activation = Constraint(
        model.BLOCK_ORDERS,
        rule=lambda m, bo: m.v_u_activated_BLOCK_ORDERS[bo] >= m.v_x_BLOCK_ORDERS[bo],
        doc="If any bid in the block order is accepted, the whole block must be accepted",
    )

    model.c_MAR_Block_Orders_Quantity = Constraint(
        model.BLOCK_ORDERS,
        rule=lambda m, bo: m.v_x_BLOCK_ORDERS[bo]
        >= m.p_MAR[bo] * m.v_u_activated_BLOCK_ORDERS[bo],
        doc="If a block order is activated, the minimum acceptance ratio (MAR) must be met",
    )

    model.c_SCO_Tramo_Activation = Constraint(
        model.SCO_SELLER_BIDS,
        rule=lambda m, s: m.v_u_activated_SCO_TRAMOS[m.p_SCO_TRAMO_PER_BID[s]]
        >= m.v_x_SCO_SELLER_BIDS[s],
        doc="If any bid in the SCO tramo is accepted, the whole tramo must be accepted",
    )

    model.c_MAV_SCO_Quantity = Constraint(
        model.SCO_SELLER_BIDS,
        rule=lambda m, s: m.v_x_SCO_SELLER_BIDS[s]
        >= (m.p_MAV[s] / m.p_quantity_SCO_SELLER_BIDS[s])
        * m.v_u_activated_SCO_TRAMOS[m.p_SCO_TRAMO_PER_BID[s]],
        doc="If a SCO tramo is activated, the minimum acceptance volume (MAV) must be met",
    )

    # model.c_Simple_Seller_Bids_Activation = Constraint(
    #     model.MAV_SIMPLE_SELLER_BIDS,
    #     rule=lambda m, s: m.v_u_active_MAV_SIMPLE_SELLER_BIDS[s]
    #     >= m.v_x_SIMPLE_SELLER_BIDS[s],
    #     doc="If a simple seller bid with MAV is activated, the minimum acceptance volume (MAV) must be met",
    # )

    # model.c_MAV_Simple_Seller_Bids_Quantity = Constraint(
    #     model.MAV_SIMPLE_SELLER_BIDS,
    #     rule=lambda m, s: m.v_x_SIMPLE_SELLER_BIDS[s]
    #     >= (m.p_MAV[s] / m.p_quantity_SIMPLE_SELLER_BIDS[s])
    #     * m.v_u_active_MAV_SIMPLE_SELLER_BIDS[s],
    #     doc="If a simple seller bid with MAV is activated, the minimum acceptance volume (MAV) must be met",
    # )

    # !!! Uncomment for block orders parent-child constraints
    # model.c_Parent_Child_Block_Orders = Constraint(
    #     model.BLOQUE_PARENT_CHILDREN,
    #     rule=lambda m, bc: m.v_x_BLOCK_ORDERS[bc.split("$")[0]]
    #     >= m.v_x_BLOCK_ORDERS[bc.split("$")[1]],
    #     doc="If a child block order is accepted, the parent block order must be accepted",
    # )

    model.c_Parent_Child_SCO = Constraint(
        model.SCO_PARENT_CHILDREN,
        rule=lambda m, sc: m.v_u_activated_SCO_TRAMOS[sc.split("$")[0]]
        >= m.v_u_activated_SCO_TRAMOS[sc.split("$")[1]],
        doc="If a child SCO tramo is accepted, the parent SCO tramo must be accepted",
    )

    model.c_Exclusive_Block_Orders = Constraint(
        model.EXCLUSIVE_BLOCK_ORDERS_GROUPED,
        rule=lambda m, bg: sum(m.v_u_activated_BLOCK_ORDERS[bo] for bo in bg.split("$"))
        <= 1,
        doc="At most one block order in an exclusive group can be accepted",
    )

    ########################### Solve ########################

    model.dual = Suffix(direction=Suffix.IMPORT)
    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    model_binaries = model.clone()

    model.v_u_activated_BLOCK_ORDERS.fix()
    model.v_u_activated_SCO_TRAMOS.fix()

    opt = SolverFactory("gurobi")
    results = opt.solve(model, tee=False)  # True)
    # results.write()

    return model, model_binaries, results
