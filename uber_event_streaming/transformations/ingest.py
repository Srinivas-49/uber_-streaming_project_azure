
from pyspark import pipelines as dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==========================================
# 1. AZURE EVENT HUBS CONFIGURATION
# ==========================================

EH_NAMESPACE = "event-uber-0000"
EH_NAME = "ubertopic0000"

EH_CONN_STR = dbutils.secrets.get(scope="uber-streaming", key="eh-connection-string")


# ==========================================
# 2. KAFKA CONSUMER CONFIGURATION
# ==========================================

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":
        f"{EH_NAMESPACE}.servicebus.windows.net:9093",

    "subscribe": EH_NAME,

    "kafka.security.protocol": "SASL_SSL",

    "kafka.sasl.mechanism": "PLAIN",

    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" '
        f'password="{EH_CONN_STR}";'
    ),

    "kafka.request.timeout.ms": "60000",

    "kafka.session.timeout.ms": "30000",

    "maxOffsetsPerTrigger": "10000",

    "failOnDataLoss": "false",

    "startingOffsets": "earliest"
}

# ==========================================
# 3. BRONZE LAYER - RAW RIDES
# ==========================================

@dlt.table(
    name="rides_raw",
    comment="Raw Uber ride events from Azure Event Hubs",
    table_properties={
        "quality": "bronze"
    }
)
def get_rides_raw():

    df = (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
    )

    # Convert Kafka binary value into readable string
    df = df.withColumn(
        "rides",
        col("value").cast("string")
    )

    # Convert Kafka binary key into readable string
    df = df.withColumn(
        "ride_key",
        col("key").cast("string")
    )

    return df


