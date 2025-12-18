from mibel_simulator.const import PORTUGAL_ZONE, SPAIN_ZONE
import mibel_simulator.columns as cols
from pyomo.environ import Binary
from pyomo.environ import (
    ConcreteModel,
    Set,
    Param,
    Var,
    NonNegativeReals,
    NonPositiveReals,
    Constraint,
    Objective,
    maximize,
    UnitInterval,
    Any,
)


def make_model(
    det_cab_date,
    capacidad_inter_PT_date,
    exclusive_block_orders_grouped,
):

    # fmt: off

    det_cab_date_C =        det_cab_date.query(f'{cols.CAT_BUY_SELL} == "C"').copy()
    det_cab_date_V =        det_cab_date.query(f'{cols.CAT_BUY_SELL} == "V"').copy()

    det_cab_date_V_bloque = det_cab_date_V.query(f"{cols.ID_BLOCK_ORDER}.notna()").copy()
    det_cab_date_V_simple = det_cab_date_V.query(f"{cols.ID_BLOCK_ORDER}.isna() and {cols.ID_SCO}.isna()").copy()
    det_cab_date_V_sco =    det_cab_date_V.query(f"{cols.ID_SCO}.notna()").copy()

    # TODO: move this check to data validation step
    assert len(det_cab_date_V) == len(det_cab_date_V_bloque) + len(
        det_cab_date_V_simple
    ) + len(det_cab_date_V_sco), "Some offer is not classified as block, simple or sco"

    model = ConcreteModel("MIBEL Market")

    periods = det_cab_date[cols.INT_PERIODO].sort_values().unique().tolist()
    countries = [SPAIN_ZONE, PORTUGAL_ZONE]

    ##### Sets #####

    model.PERIODS =             Set(initialize=periods,     doc="Market sessions along the day")
    model.COUNTRIES =           Set(initialize=countries,   doc="Countries/regions participating in the market")

    buyer_bids =            det_cab_date_C          [cols.ID_INDIVIDUAL_BID].tolist()
    block_order_bids =      det_cab_date_V_bloque   [cols.ID_INDIVIDUAL_BID].tolist()
    simple_seller_bids =    det_cab_date_V_simple   [cols.ID_INDIVIDUAL_BID].tolist()
    sco_seller_bids =       det_cab_date_V_sco      [cols.ID_INDIVIDUAL_BID].tolist()
    block_orders =          det_cab_date_V_bloque   [cols.ID_BLOCK_ORDER].unique().tolist()
    sco_orders =            det_cab_date_V_sco      [cols.ID_SCO].unique().tolist()

    model.BUYER_BIDS =          Set(initialize=buyer_bids,          doc="Buyer individual bid ids")
    model.SIMPLE_SELLER_BIDS =  Set(initialize=simple_seller_bids,  doc="Seller simple individual bid ids")
    model.BLOCK_ORDER_BIDS =    Set(initialize=block_order_bids,    doc="Seller block order individual bid ids")
    model.SCO_SELLER_BIDS =     Set(initialize=sco_seller_bids,     doc="Seller SCO individual bid ids")
    model.BLOCK_ORDERS =        Set(initialize=block_orders,        doc="Seller block order ids")
    model.SCO_ORDERS =          Set(initialize=sco_orders,          doc="Seller SCO order ids")

    buyer_bids_per_period_and_country_fnc =          lambda period, country:   det_cab_date_C         .query(f"{cols.INT_PERIODO} == @period         and {cols.CAT_PAIS} == @country"   )[cols.ID_INDIVIDUAL_BID].tolist()
    block_order_bids_by_block_and_period_fnc =       lambda bloque_id, period: det_cab_date_V_bloque  .query(f"{cols.ID_BLOCK_ORDER} == @bloque_id   and {cols.INT_PERIODO} == @period" )[cols.ID_INDIVIDUAL_BID].tolist()
    simple_seller_bids_per_period_and_country_fnc =  lambda period, country:   det_cab_date_V_simple  .query(f"{cols.INT_PERIODO} == @period         and {cols.CAT_PAIS} == @country"   )[cols.ID_INDIVIDUAL_BID].tolist()
    block_order_bids_by_block_fnc =                  lambda bloque_id:         det_cab_date_V_bloque  .query(f"{cols.ID_BLOCK_ORDER} == @bloque_id"                                     )[cols.ID_INDIVIDUAL_BID].tolist()
    sco_seller_bids_per_period_and_country_fnc =     lambda period, country:   det_cab_date_V_sco     .query(f"{cols.INT_PERIODO} == @period         and {cols.CAT_PAIS} == @country"   )[cols.ID_INDIVIDUAL_BID].tolist()
    block_orders_by_country_fnc =                    lambda country:           det_cab_date_V_bloque  .query(f"{cols.CAT_PAIS} == @country"                                             )[cols.ID_BLOCK_ORDER].unique().tolist()


    buyer_bids_per_period_and_country =         {(period, country):   buyer_bids_per_period_and_country_fnc(period, country)          for period in periods           for country in countries}
    block_order_bids_by_block_and_period =      {(bloque_id, period): block_order_bids_by_block_and_period_fnc(bloque_id, period)     for bloque_id in block_orders   for period in periods}
    simple_seller_bids_per_period_and_country = {(period, country):   simple_seller_bids_per_period_and_country_fnc(period, country)  for period in periods           for country in countries}
    block_order_bids_by_block =                 {bloque_id:           block_order_bids_by_block_fnc(bloque_id)                        for bloque_id in block_orders}
    sco_seller_bids_per_period_and_country =    {(period, country):   sco_seller_bids_per_period_and_country_fnc(period, country)     for period in periods           for country in countries}
    block_orders_by_country =                   {country:             block_orders_by_country_fnc(country)                            for country in countries}

    model.BUYER_BIDS_PER_PERIOD_AND_COUNTRY =           Set(model.PERIODS,      model.COUNTRIES, initialize=buyer_bids_per_period_and_country,          doc="Buyer individual bid ids per peri        od and country")
    model.SIMPLE_SELLER_BIDS_PER_PERIOD_AND_COUNTRY =   Set(model.PERIODS,      model.COUNTRIES, initialize=simple_seller_bids_per_period_and_country,  doc="Simple seller individual bids per period and country")
    model.SCO_SELLER_BIDS_PER_PERIOD_AND_COUNTRY =      Set(model.PERIODS,      model.COUNTRIES, initialize=sco_seller_bids_per_period_and_country,     doc="SCO seller individual bids per period and country")
    model.BLOCK_ORDER_BIDS_BY_BLOCK_AND_PERIOD =        Set(model.BLOCK_ORDERS, model.PERIODS,   initialize=block_order_bids_by_block_and_period,       doc="Block order individual bids per block order and period")
    model.BLOCK_ORDER_BIDS_BY_BLOCK =                   Set(model.BLOCK_ORDERS,                  initialize=block_order_bids_by_block,                  doc="Block order individual bids per block order")
    model.BLOCK_ORDERS_BY_COUNTRY =                     Set(model.COUNTRIES,                     initialize=block_orders_by_country,                    doc="Block orders per country")

    exclusive_block_orders_grouped_joined =  exclusive_block_orders_grouped.apply(lambda x: "$".join(x))

    model.EXCLUSIVE_BLOCK_ORDERS_GROUPED =  Set(initialize=exclusive_block_orders_grouped_joined,  doc="Groups of exclusive block orders, a list of block_ids joined by $")

    ##### Parameters #####
    
    p_price_min_SIMPLE_SELLERS_BIDS =           det_cab_date_V_simple    .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_PRICE].to_dict()
    p_price_min_BLOCK_ORDERS =                  det_cab_date_V_bloque    .drop_duplicates(cols.ID_BLOCK_ORDER).set_index(cols.ID_BLOCK_ORDER)[cols.FLOAT_BID_PRICE].to_dict()
    p_price_min_SCO_SELLER_BIDS =               det_cab_date_V_sco       .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_PRICE].to_dict()
    p_price_max_BUYERS_BIDS =                   det_cab_date_C           .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_PRICE].to_dict()
    p_quantity_SIMPLE_SELLER_BIDS =             det_cab_date_V_simple    .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_POWER].to_dict()
    p_quantity_SCO_SELLER_BIDS =                det_cab_date_V_sco       .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_POWER].to_dict()
    p_quantity_BLOCK_ORDER_BIDS =               det_cab_date_V_bloque    .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_POWER].to_dict()
    p_quantity_BUYER_BIDS =                     det_cab_date_C           .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_BID_POWER].to_dict()
    p_congestion_spain_portugal_exportacion =   capacidad_inter_PT_date  .set_index(cols.INT_PERIODO)            [cols.FLOAT_EXPORT_CAPACITY].to_dict()
    p_congestion_spain_portugal_importacion =   capacidad_inter_PT_date  .set_index(cols.INT_PERIODO)            [cols.FLOAT_IMPORT_CAPACITY].to_dict()
    p_MAR =                                     det_cab_date_V_bloque    .drop_duplicates(cols.ID_BLOCK_ORDER).set_index(cols.ID_BLOCK_ORDER)[cols.FLOAT_MAR].to_dict()
    p_MAV =                                     det_cab_date_V_sco       .set_index(cols.ID_INDIVIDUAL_BID)      [cols.FLOAT_MAV].to_dict()
    p_SCO_ORDER_PER_BID =                       det_cab_date_V_sco       .set_index(cols.ID_INDIVIDUAL_BID)      [cols.ID_SCO].to_dict()

    model.p_price_min_SIMPLE_SELLERS_BIDS =          Param(model.SIMPLE_SELLER_BIDS,  initialize=p_price_min_SIMPLE_SELLERS_BIDS,                                   doc="Minimum price of each generator - simple bids")
    model.p_price_min_BLOCK_ORDERS =                 Param(model.BLOCK_ORDERS,        initialize=p_price_min_BLOCK_ORDERS,                                          doc="Minimum price of each generator - block orders")
    model.p_price_min_SCO_SELLER_BIDS =              Param(model.SCO_SELLER_BIDS,     initialize=p_price_min_SCO_SELLER_BIDS,                                       doc="Minimum price of each generator - SCO bids")
    model.p_price_max_BUYERS_BIDS =                  Param(model.BUYER_BIDS,          initialize=p_price_max_BUYERS_BIDS,                                           doc="Maximum price of each consumer")
    model.p_quantity_SIMPLE_SELLER_BIDS =            Param(model.SIMPLE_SELLER_BIDS,  initialize=p_quantity_SIMPLE_SELLER_BIDS,            within=NonNegativeReals, doc="Quantity offered by each generator")
    model.p_quantity_SCO_SELLER_BIDS =               Param(model.SCO_SELLER_BIDS,     initialize=p_quantity_SCO_SELLER_BIDS,               within=NonNegativeReals, doc="Quantity offered by each generator - SCO bids")
    model.p_quantity_BLOCK_ORDER_BIDS =              Param(model.BLOCK_ORDER_BIDS,    initialize=p_quantity_BLOCK_ORDER_BIDS,              within=NonNegativeReals, doc="Quantity offered by each generator - block orders")
    model.p_quantity_BUYER_BIDS =                    Param(model.BUYER_BIDS,          initialize=p_quantity_BUYER_BIDS,                    within=NonNegativeReals, doc="Quantity demanded by each consumer")
    model.p_congestion_spain_portugal_exportacion =  Param(model.PERIODS,             initialize=p_congestion_spain_portugal_exportacion,  within=NonNegativeReals, doc="Maximum capacity Spain export to Portugal")
    model.p_congestion_spain_portugal_importacion =  Param(model.PERIODS,             initialize=p_congestion_spain_portugal_importacion,  within=NonPositiveReals, doc="Maximum capacity Spain import from Portugal (negative value)")
    model.p_MAR =                                    Param(model.BLOCK_ORDERS,        initialize=p_MAR,                                    within=NonNegativeReals, doc="Minimum acceptance ratio (MAR) of each block order")
    model.p_MAV =                                    Param(model.SCO_SELLER_BIDS,     initialize=p_MAV,                                    within=NonNegativeReals, doc="Minimum acceptance volume (MAV) of each SCO seller bid")
    model.p_SCO_ORDER_PER_BID =                      Param(model.SCO_SELLER_BIDS,     initialize=p_SCO_ORDER_PER_BID,                      within=Any,              doc="Order identifier for each SCO bid")

    ##### Variables #####

    model.v_x_SIMPLE_SELLER_BIDS =        Var(model.SIMPLE_SELLER_BIDS, within=UnitInterval, doc="Quantity ratio sold by each generator")
    model.v_x_BUYER_BIDS =                Var(model.BUYER_BIDS,         within=UnitInterval, doc="Quantity ratio bought by each consumer")
    model.v_x_BLOCK_ORDERS =              Var(model.BLOCK_ORDERS,       within=UnitInterval, doc="Quantity ratio sold by each block order")
    model.v_x_SCO_SELLER_BIDS =           Var(model.SCO_SELLER_BIDS,    within=UnitInterval, doc="Quantity ratio sold by each SCO bid")
    model.v_u_activated_SCO_ORDERS =      Var(model.SCO_ORDERS,         within=Binary,       doc="Whether a SCO order is activated or not")
    model.v_u_activated_BLOCK_ORDERS =    Var(model.BLOCK_ORDERS,       within=Binary,       doc="Whether a block order is activated or not")
    model.v_transmission_spain_portugal = Var(model.PERIODS,                                 doc="Quantity transmitted between Spain and Portugal")

    ##### Objective #####

    # fmt: on

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

    ##### Constraints #####

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

    model.c_SCO_Order_Activation = Constraint(
        model.SCO_SELLER_BIDS,
        rule=lambda m, s: m.v_u_activated_SCO_ORDERS[m.p_SCO_ORDER_PER_BID[s]]
        >= m.v_x_SCO_SELLER_BIDS[s],
        doc="If any bid in the SCO order is accepted, the whole order must be accepted",
    )

    model.c_MAV_SCO_Quantity = Constraint(
        model.SCO_SELLER_BIDS,
        rule=lambda m, s: m.v_x_SCO_SELLER_BIDS[s]
        >= (m.p_MAV[s] / m.p_quantity_SCO_SELLER_BIDS[s])
        * m.v_u_activated_SCO_ORDERS[m.p_SCO_ORDER_PER_BID[s]],
        doc="If a SCO order is activated, the minimum acceptance volume (MAV) must be met in each bid",
    )

    model.c_Exclusive_Block_Orders = Constraint(
        model.EXCLUSIVE_BLOCK_ORDERS_GROUPED,
        rule=lambda m, bg: sum(m.v_u_activated_BLOCK_ORDERS[bo] for bo in bg.split("$"))
        <= 1,
        doc="At most one block order in an exclusive group can be accepted",
    )

    return model
