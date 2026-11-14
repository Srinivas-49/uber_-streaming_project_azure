"""Event Hubs Kafka input; pipeline configuration must supply namespace and hub."""
from pyspark import pipelines as dlt
from pyspark.sql import functions as F

NAMESPACE = spark.conf.get("uber.eventhubs.namespace", "")
EVENT_HUB = spark.conf.get("uber.eventhubs.name", "")
MAX_OFFSETS = int(spark.conf.get("uber.eventhubs.max_offsets_per_trigger", "10000"))
if not NAMESPACE or not EVENT_HUB:
    raise ValueError("Set uber.eventhubs.namespace and uber.eventhubs.name in pipeline configuration")
if MAX_OFFSETS < 1:
    raise ValueError("max_offsets_per_trigger must be positive")

CONNECTION = dbutils.secrets.get(scope="uber-streaming", key="eh-connection-string")
OPTIONS = {
    "kafka.bootstrap.servers": f"{NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EVENT_HUB,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        'username="$ConnectionString" '
        f'password="{CONNECTION}";'
    ),
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger": str(MAX_OFFSETS),
    "failOnDataLoss": "true",
    "startingOffsets": "earliest"
}

@dlt.table(name="rides_raw", comment="Raw ride events; Kafka envelope preserved", table_properties={"quality": "bronze"})
def get_rides_raw():
    return (spark.readStream.format("kafka").options(**OPTIONS).load()
            .withColumn("rides", F.col("value").cast("string"))
            .withColumn("ride_key", F.col("key").cast("string")))
