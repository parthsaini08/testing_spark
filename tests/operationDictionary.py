import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current) + "/src/"
sys.path.append(parent)
from use_cases import spark_age_demographic as sp_ad  # noqa: E402
from use_cases import spark_document_count as sp_dc  # noqa: E402
from use_cases import spark_net_wealth as sp_nw  # noqa: E402
from use_cases import spark_price_at_an_instant as sp_paai  # noqa: E402
from use_cases import spark_product_relative_reach as sp_prr  # noqa: E402

operation_dictionary = {
    "product_relative_reach": {
        "input_topic": "accounts",
        "function": sp_prr.productRelativeReach,
    },
    "price_at_an_instant": {
        "input_topic": "transactions",
        "function": sp_paai.PriceAtAnInstant,
    },
    "age_demographic": {
        "input_topic": "customers",
        "function": sp_ad.AgeDemographic,
    },
    "document_count": {
        "input_topic": "customers",
        "function": sp_dc.countDocs,
    },
    "net_wealth": {
        "input_topic": "transactions",
        "function": sp_nw.NetWealth,
    },
}
