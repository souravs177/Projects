from pyspark.sql.types import DoubleType, StringType, StructField, StructType


SCHEMAS = {
    "claims": StructType(
        [
            StructField("claim_id", StringType(), True),
            StructField("member_id", StringType(), True),
            StructField("provider_id", StringType(), True),
            StructField("service_date", StringType(), True),
            StructField("diagnosis_code", StringType(), True),
            StructField("procedure_code", StringType(), True),
            StructField("billed_amount", DoubleType(), True),
            StructField("paid_amount", DoubleType(), True),
            StructField("claim_status", StringType(), True),
        ]
    ),
    "members": StructType(
        [
            StructField("member_id", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("date_of_birth", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("plan_id", StringType(), True),
        ]
    ),
    "providers": StructType(
        [
            StructField("provider_id", StringType(), True),
            StructField("provider_name", StringType(), True),
            StructField("specialty", StringType(), True),
            StructField("npi", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
        ]
    ),
}


def get_schema(name: str) -> StructType:
    try:
        return SCHEMAS[name]
    except KeyError as error:
        raise ValueError(f"Schema '{name}' is not defined.") from error
