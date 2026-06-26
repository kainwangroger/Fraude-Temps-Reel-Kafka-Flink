import os

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

TRANSACTION_TOPIC = "transactions"
ALERT_TOPIC = "fraud-alerts"
GROUP_ID = "flink-fraud-detector"

JOB_NAME = "fraud-detection-flink"


KAFKA_JAR = os.getenv(
    "KAFKA_JAR",
    "file:///opt/flink/lib/flink-sql-connector-kafka-2.3.0.jar"
)


def setup_tables(t_env):
    t_env.get_config().set("pipeline.jars", KAFKA_JAR)

    t_env.execute_sql(f"""
        CREATE TABLE transactions (
            transaction_id    STRING,
            card_id           BIGINT,
            amount            DOUBLE,
            currency          STRING,
            merchant          STRING,
            city              STRING,
            latitude          DOUBLE,
            longitude         DOUBLE,
            `timestamp`       STRING,
            device_id         BIGINT,
            ip_address        STRING,
            is_fraud          BOOLEAN,
            proctime AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TRANSACTION_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = '{GROUP_ID}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fraud_alerts (
            transaction_id    STRING,
            card_id           BIGINT,
            rule              STRING,
            severity          STRING,
            amount            DOUBLE,
            city              STRING,
            latitude          DOUBLE,
            longitude         DOUBLE,
            alert_time        TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{ALERT_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format' = 'json'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE fraud_alerts_aggregate (
            card_id           BIGINT,
            rule              STRING,
            severity          STRING,
            window_start      TIMESTAMP(3),
            window_end        TIMESTAMP(3),
            metric_value      DOUBLE,
            alert_time        TIMESTAMP(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{ALERT_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'format' = 'json'
        )
    """)


def register_rules(t_env):
    t_env.execute_sql("""
        INSERT INTO fraud_alerts
        SELECT
            transaction_id,
            card_id,
            'montant_excessif' AS rule,
            'high'              AS severity,
            amount,
            city,
            latitude,
            longitude,
            PROCTIME() AS alert_time
        FROM transactions
        WHERE amount > 10000
    """)

    t_env.execute_sql("""
        INSERT INTO fraud_alerts_aggregate
        SELECT
            card_id,
            'velocite_anormale'    AS rule,
            'medium'               AS severity,
            HOP_START(proctime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE) AS window_start,
            HOP_END(proctime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)   AS window_end,
            CAST(COUNT(*) AS DOUBLE) AS metric_value,
            PROCTIME() AS alert_time
        FROM transactions
        GROUP BY
            card_id,
            HOP(proctime, INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
        HAVING COUNT(*) >= 3
    """)

    t_env.execute_sql("""
        INSERT INTO fraud_alerts_aggregate
        SELECT
            card_id,
            'changement_pays_rapide' AS rule,
            'high'                    AS severity,
            HOP_START(proctime, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) AS window_start,
            HOP_END(proctime, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)   AS window_end,
            CAST(COUNT(DISTINCT city) AS DOUBLE) AS metric_value,
            PROCTIME() AS alert_time
        FROM transactions
        GROUP BY
            card_id,
            HOP(proctime, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)
        HAVING COUNT(DISTINCT city) >= 2
    """)


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    setup_tables(t_env)
    register_rules(t_env)

    print(f"Job Flink '{JOB_NAME}' soumis en mode streaming...")
    t_env.execute(JOB_NAME)


if __name__ == "__main__":
    main()
