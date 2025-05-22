
from deltalake import Field
from deltalake import Schema as DeltaSchema
from polars import Datetime, Float64, String
from polars import Schema as PolarsSchema

# Type mappings between Polars and Delta Lake
POLARS_TO_DELTA_TYPE_MAP = {
    Float64: "double",
    String: "string",
    Datetime("us"): "timestamp",
}


# Liquidation schema definitions
LIQUIDATION_POLARS_SCHEMA = PolarsSchema({
    "time": Datetime("us"),
    "symbol": String(),
    "side": String(),
    "original_quantity": Float64,
    "price": Float64,
    "avg_price": Float64,
    "status": String(),
    "last_filled_quantity": Float64,
    "filled_accum_quantity": Float64,
})

LIQUIDATION_SCHEMA = DeltaSchema([
    Field(name=name, type=POLARS_TO_DELTA_TYPE_MAP[dtype], nullable=False)
    for name, dtype in LIQUIDATION_POLARS_SCHEMA.items()
])


# Price schema definitions
PRICE_POLARS_SCHEMA = PolarsSchema({
    "timestamp": Datetime("us"),
    "symbol": String(),
    "price": Float64,
    "volume_24h": Float64,
    "price_change_24h": Float64,
    "price_change_percent_24h": Float64,
    "high_24h": Float64,
    "low_24h": Float64,
})

PRICE_SCHEMA = DeltaSchema([
    Field(name=name, type=POLARS_TO_DELTA_TYPE_MAP[dtype], nullable=True if name not in ["timestamp", "symbol", "price"] else False)
    for name, dtype in PRICE_POLARS_SCHEMA.items()
])


def get_polars_schema(schema_type: str) -> PolarsSchema:
    """Get Polars schema by type name"""
    schemas = {
        "liquidation": LIQUIDATION_POLARS_SCHEMA,
        "price": PRICE_POLARS_SCHEMA,
    }
    if schema_type not in schemas:
        raise ValueError(f"Unknown schema type: {schema_type}")
    return schemas[schema_type]