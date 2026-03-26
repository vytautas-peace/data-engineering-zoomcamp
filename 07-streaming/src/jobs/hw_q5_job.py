# Aggregation job with a session window with a 5-minute gap
# on `PULocationID`, using `lpep_pickup_datetime` 
# as the event time aggregation job for homework Q5

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
import time


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime VARCHAR,
            lpep_dropoff_datetime VARCHAR,
            PULocationID INTEGER,
            DOLocationID INTEGER,
            passenger_count INTEGER,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            event_timestamp AS TO_TIMESTAMP(lpep_pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK for event_timestamp as event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def create_events_aggregated_sink(t_env):
    table_name = 'output_q5'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INTEGER,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            num_trips BIGINT,
            PRIMARY KEY (PULocationID, session_start, session_end) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def log_aggregation():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        aggregated_table = create_events_aggregated_sink(t_env)

        # 1. CREATE THE VIEW (This fixes the 'view not found' error)
        t_env.execute_sql(f"""
            CREATE TEMPORARY VIEW session_stats AS
            SELECT
                PULocationID,
                window_start AS session_start,
                window_end AS session_end,
                COUNT(*) AS num_trips
            FROM TABLE (
                SESSION (
                    TABLE {source_table}, 
                    DESCRIPTOR(event_timestamp), 
                    INTERVAL '5' MINUTE
                )
            )
            GROUP BY PULocationID, window_start, window_end
        """)

        # 2. TRIGGER THE JOB (This sends data to Postgres)
        # Note: We do NOT use .wait() or .print() here so the script can proceed
        print("Submitting job to Flink...")
        t_env.execute_sql(f"INSERT INTO {aggregated_table} SELECT * FROM session_stats").wait()

        # 3. MANUAL OBSERVATION (This allows you to see the count climb)
        print("Job submitted. Monitoring for 60 seconds...")
        for i in range(12):
            time.sleep(5)
            print(f"Checking progress... {5 * (i+1)}s elapsed")

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_aggregation()