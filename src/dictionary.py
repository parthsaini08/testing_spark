from use_cases import spark_age_demographic as sp_ad
from use_cases import spark_aggregate_transactions as sp_agg_trans
from use_cases import spark_document_count as sp_dc
from use_cases import spark_get_high_risk_accounts as sp_fraud_trans
from use_cases import spark_net_wealth as sp_nw
from use_cases import spark_price_at_an_instant as sp_paai
from use_cases import spark_product_relative_reach as sp_prr
from use_cases import spark_trending_list_of_products as sp_tlp
from use_cases import spark_trending_symbols as sp_ts

# mapping the operation to the functions and the topic names
dictionary = {
    "customers_count": {
        "input_topic": "customers",
        "input_reading_offset": "earliest",
        "function": sp_dc.countDocs,
        "output_mode": "complete",
    },
    "transactions_count": {
        "input_topic": "transactions",
        "input_reading_offset": "earliest",
        "function": sp_dc.countDocs,
        "output_mode": "complete",
    },
    "accounts_count": {
        "input_topic": "accounts",
        "input_reading_offset": "earliest",
        "function": sp_dc.countDocs,
        "output_mode": "complete",
    },
    "price_at_an_instant": {
        "input_topic": "transactions",
        "input_reading_offset": "latest",
        "function": sp_paai.PriceAtAnInstant,
        "output_mode": "append",
    },
    "product_relative_reach": {
        "input_topic": "accounts",
        "input_reading_offset": "earliest",
        "function": sp_prr.productRelativeReach,
        "output_mode": "complete",
    },
    "net_wealth": {
        "input_topic": "transactions",
        "input_reading_offset": "latest",
        "function": sp_nw.NetWealth,
        "output_mode": "update",
    },
    "trending_list_of_products": {
        "input_topic": "accounts",
        "input_reading_offset": "latest",
        "function": sp_tlp.TrendingListOfProducts,
        "output_mode": "complete",
        "producer_processing_time": "2 seconds",
    },
    "trending_list_of_symbols": {
        "input_topic": "transactions",
        "input_reading_offset": "latest",
        "function": sp_ts.TrendingSymbols,
        "output_mode": "complete",
        "producer_processing_time": "2 seconds",
    },
    "age_demographic": {
        "input_topic": "customers",
        "input_reading_offset": "earliest",
        "function": sp_ad.AgeDemographic,
        "output_mode": "update",
    },
    "transaction_stats": {
        "input_topic": "transactions",
        "input_reading_offset": "earliest",
        "function": sp_agg_trans.TransactionsAggregator,
        "output_mode": "update",
        "producer_processing_time": "1 minute",
    },
    "high_risk_accounts": {
        "input_topic": "transactions",
        "input_reading_offset": "latest",
        "function": sp_fraud_trans.OutlierDetector,
        "output_mode": "append",
    },
}
