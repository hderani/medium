# Databricks notebook source
# MAGIC %md
# MAGIC # Leveraging `transformWithStateInPandas` for Stateful 
# MAGIC # Stream Processing

# COMMAND ----------

# MAGIC %md
# MAGIC **Use Case:** 
# MAGIC
# MAGIC > Real-Time Enrichment of Flight Delay Events with Passenger Profiles and Preferences

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Stateful Stream Processing?
# MAGIC
# MAGIC Stateful stream processing refers to processing a continuous stream of events in real-time while maintaining state based on the events seen so far. This allows the system to track changes and patterns over time in the event stream, and enables making decisions or taking actions based on this information.
# MAGIC
# MAGIC Stateful stream processing in Apache Spark Structured Streaming is supported using built-in operators (such as windowed aggregation, stream-stream join, drop duplicates etc.) for predefined logic and using flatMapGroupWithState or mapGroupWithState for arbitrary logic. The arbitrary logic allows users to write their custom state manipulation code in their pipelines. However, as the adoption of streaming grows in the enterprise, more complex and sophisticated streaming applications demand several additional features to make it easier for developers to write stateful streaming pipelines.
# MAGIC
# MAGIC In order to support these new, growing stateful streaming applications or operational use cases, the Spark community is introducing a new Spark operator called `transformWithState`. This operator will allow for flexible data modeling, composite types, timers, TTL, chaining stateful operators after transformWithState, schema evolution, reusing state from a different query and integration with a host of other Databricks features such as Unity Catalog, Delta Live Tables, and Spark Connect.
# MAGIC
# MAGIC https://www.databricks.com/blog/introducing-transformwithstate-apache-sparktm-structured-streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is `transformWithStateInPandas`?
# MAGIC
# MAGIC `transformWithStateInPandas` is a stateful processing API in PySpark that allows developers to process streaming data with fine-grained state management using Pandas DataFrames. It enables handling complex business logic by maintaining and updating state across multiple streams in real-time.
# MAGIC
# MAGIC ### **Benefits**:
# MAGIC - Handles complex business logic
# MAGIC - Supports multiple state maps
# MAGIC - Flexibility in handling out-of-order events
# MAGIC - Efficient for event-driven processing
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Not Use Stream-Stream Joins?
# MAGIC
# MAGIC Stream-stream joins are a popular approach for real-time data enrichment, but they pose significant challenges in scenarios like this use case.
# MAGIC
# MAGIC ### **Challenges**:
# MAGIC 1. **High Latency**: Joining multiple streams, each potentially large and fast-moving, can significantly increase processing time
# MAGIC 2. **Deduplication Complexity**: Stream-stream joins do not natively address the need to deduplicate or retain only the latest record for each key
# MAGIC 3. **Out-of-Order Data**: When events arrive out of sequence, managing state consistency becomes complex
# MAGIC 4. **State Explosion**: With multiple streams, state management can grow disproportionately large, affecting performance and scalability.
# MAGIC
# MAGIC By using `transformWithStateInPandas`, we can address these challenges through explicit state handling, fine-grained control over deduplication, and flexible processing tailored to our specific requirements.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Case: Real-Time Enrichment of Flight Delay Events with Passenger Profiles and Preferences
# MAGIC
# MAGIC ### **Scenario**:
# MAGIC An airline wants to analyze real-time flight delay events combined with passenger profiles and travel preferences to deliver personalized notifications and offers.
# MAGIC
# MAGIC ### **Data Sources** (All Streaming):
# MAGIC 1. **Flight Delay Events Stream**: Real-time updates for delay events
# MAGIC 2. **Passenger Profiles Stream**: Real-time updates for passenger names, emails, and profile changes
# MAGIC 3. **Passenger Preferences Stream**: Streaming data on passengers preferred airline and seat class

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solution Architecture
# MAGIC
# MAGIC ### **Explanation**:
# MAGIC - **Flight Delay Events Stream, Passenger Profiles Stream, and Passenger Preferences Stream**: Input streaming data sources, normalized to ensure consistent schema and metadata alignment
# MAGIC - **Normalized Union**: Combines all three streams after aligning schemas and adding necessary metadata fields
# MAGIC - **CustomStreamJoinProcessor**: Handles state management, deduplication, and timer-based processing for enriching events
# MAGIC - **Enriched Output Stream**: Produces the final enriched dataset with detailed fields

# COMMAND ----------

# DBTITLE 1,Import Libraries
from pyspark.sql.types import *
from pyspark.sql.functions import lit, to_timestamp, col
import os

from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle ## Stateful Streaming API
from typing import Iterator
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set spark configurations

# COMMAND ----------

spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
)
spark.conf.set("spark.sql.shuffle.partitions", "10")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schemas & Stream Setup

# COMMAND ----------

base_dir = "/tmp/airline_stream_demo"

flight_delay_schema = StructType(
    [
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("flight_id", StringType()),
        StructField("delay_duration", StringType()),
    ]
)

profile_schema = StructType(
    [
        StructField("user_id", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)

preference_schema = StructType(
    [
        StructField("user_id", StringType()),
        StructField("preferred_category", StringType()),
        StructField("updated_at", TimestampType()),
    ]
)

# Read streams
flight_delay_stream = spark.readStream.schema(flight_delay_schema).json(
    os.path.join(base_dir, "flight_delay")
)
profile_stream = spark.readStream.schema(profile_schema).json(
    os.path.join(base_dir, "profile_updates")
)
preference_stream = spark.readStream.schema(preference_schema).json(
    os.path.join(base_dir, "preference_updates")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Normalize & Union Streams
# MAGIC
# MAGIC This step combines three input streams—Flight Delay Events, Passenger Profiles, and Passenger Preferences—into a unified data stream with a consistent schema. The process involves:
# MAGIC
# MAGIC - Schema Alignment: Ensuring all input streams share the same structure by mapping their fields to a common format
# MAGIC
# MAGIC - Adding Metadata Fields: Introducing additional metadata - event source - to use in state management
# MAGIC
# MAGIC - Union of Streams: Merging all normalized streams into one, preparing the data for the join and enrichment logic in subsequent steps
# MAGIC
# MAGIC This process is crucial for creating a seamless input for the CustomStreamJoinProcessor, ensuring that all streams are properly formatted and synchronized before processing.

# COMMAND ----------

flight_normalized = flight_delay_stream.select(
    "user_id",
    "event_type",
    "timestamp",
    "flight_id",
    "delay_duration",
    lit(None).cast(StringType()).alias("name"),
    lit(None).cast(StringType()).alias("email"),
    lit(None).cast(StringType()).alias("preferred_category"),
    lit(None).alias("profile_updated_at"),
    lit(None).alias("preference_updated_at"),
    lit("flight_delay").alias("source"),
)

profile_normalized = profile_stream.select(
    "user_id",
    lit(None).cast(StringType()).alias("event_type"),
    lit(None).cast(TimestampType()).alias("timestamp"),
    lit(None).cast(StringType()).alias("flight_id"),
    lit(None).cast(StringType()).alias("delay_duration"),
    "name",
    "email",
    lit(None).cast(StringType()).alias("preferred_category"),
    col("updated_at").alias("profile_updated_at"),
    lit(None).alias("preference_updated_at"),
    lit("profile").alias("source"),
)

preference_normalized = preference_stream.select(
    "user_id",
    lit(None).cast(StringType()).alias("event_type"),
    lit(None).cast(TimestampType()).alias("timestamp"),
    lit(None).cast(StringType()).alias("flight_id"),
    lit(None).cast(StringType()).alias("delay_duration"),
    lit(None).cast(StringType()).alias("name"),
    lit(None).cast(StringType()).alias("email"),
    "preferred_category",
    lit(None).alias("profile_updated_at"),
    col("updated_at").alias("preference_updated_at"),
    lit("preference").alias("source"),
)

union_stream = flight_normalized.union(profile_normalized).union(preference_normalized)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Custom Stateful Processor
# MAGIC <br>
# MAGIC 1. Our custom StatefulProcessor `CustomStreamJoinProcessor` can join these three streams by user_id, managing:
# MAGIC
# MAGIC - Latest passenger profile info (user_profiles state and last_emitted_state)
# MAGIC
# MAGIC - Latest passenger preferences (user_preferences state and last_emitted_state)
# MAGIC
# MAGIC - Flight delay event stream (user_activity state)
# MAGIC
# MAGIC After a 10-second processing delay, our processor outputs enriched events like:
# MAGIC
# MAGIC | user_id | event_type | timestamp           | flight_id | delay_duration | name         | email                  | preferred_category | profile_updated_at   | preference_updated_at |
# MAGIC |---------|------------|---------------------|-----------|----------------|--------------|------------------------|--------------------|----------------------|-----------------------|
# MAGIC | U123    | delay      | 2025-05-27 14:01:00 | F456      | 45 minutes     | Alice Smith  | alice@abc.com          | Business Class     | 2025-05-26 12:00:00 | 2025-05-26 10:30:00  |
# MAGIC
# MAGIC 2. Event Handling
# MAGIC - Event handling is implemented in the `handleInputRows` method, which processes incoming rows of data from the input streams. Each row is mapped to specific stateful operations, allowing the logic to fetch and update corresponding state information. The method ensures that data from the streams (flight delays, user profiles, preferences) is appropriately associated with the relevant state objects. 
# MAGIC
# MAGIC 3. Deduplication
# MAGIC - Deduplication is handled within the `handleExpiredTimer` method using timers. A timer is set for each unique key - user_id - to track the most recent processing timestamp. When a timer expires, the method checks if the event timestamp is newer than the last emitted record's timestamp. If the event is a duplicate or older, it is ignored. This ensures that only the latest, non-duplicate data is included in the output, optimizing state management and maintaining data consistency.
# MAGIC
# MAGIC In transformWithStateInPandas, we control when results are emitted using timers:
# MAGIC  - State is updated in handleInputRows
# MAGIC  - State is emitted in handleExpiredTimer when a timer expires

# COMMAND ----------

class CustomStreamJoinProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:

        self.profile_state = handle.getMapState(
            "user_profiles",
            "user_id string",
            StructType(
                [
                    StructField("name", StringType()),
                    StructField("email", StringType()),
                    StructField("profile_updated_at", TimestampType()),
                ]
            ),
        )

        self.preferences_state = handle.getMapState(
            "user_preferences",
            "user_id string",
            StructType(
                [
                    StructField("preferred_category", StringType()),
                    StructField("preference_updated_at", TimestampType()),
                ]
            ),
        )

        self.flight_delay_state = handle.getMapState(
            "user_flight_delay",
            "user_id string",
            StructType(
                [
                    StructField("event_type", StringType()),
                    StructField("timestamp", TimestampType()),
                    StructField("flight_id", StringType()),
                    StructField("delay_duration", StringType()),
                ]
            ),
        )

        self.last_emitted_state = handle.getMapState(
            "last_emitted_state",
            "user_id string",
            StructType([
                StructField("last_emitted_ts", TimestampType()),
            ])
        )

        self.handle = handle

    def handleInputRows(
        self, key, rows: Iterator[pd.DataFrame], timerValues
    ) -> Iterator[pd.DataFrame]:
        df = pd.concat(rows, ignore_index=True)

        for _, row in df.iterrows():
            user_id = row["user_id"]
            source = row["source"]

            if source == "flight_delay" and pd.notnull(row["timestamp"]):
                prev = self.flight_delay_state.getValue((user_id,))
                if prev is None or row["timestamp"] > prev["timestamp"]:
                    self.flight_delay_state.updateValue(
                        (user_id,),
                        (
                            row["event_type"],
                            row["timestamp"],
                            row["flight_id"],
                            row["delay_duration"],
                        ),
                    )
                    # In transformWithStateInPandas, we control when results are emitted using timers
                    # State is updated in handleInputRows
                    # State is emitted in handleExpiredTimer when a timer expires
                    # We must register a timer to schedule this emission
                    # So this sets the timer to fire 10 seconds (10000 milliseconds) after the event time

                    self.handle.registerTimer(
                        int(row["timestamp"].timestamp() * 1000) + (10 * 1000)
                    ) 

            elif source == "profile":
                self.profile_state.updateValue(
                    (user_id,), (row["name"], row["email"], row["profile_updated_at"])
                )

            elif source == "preference":
                self.preferences_state.updateValue(
                    (user_id,), (row["preferred_category"], row["preference_updated_at"])
                )

        return iter(
            []
        )  # if there is no state from flight_delay state, do not output anything - since we are implementing a left join

    def handleExpiredTimer(
        self, key, timerValues, expiredTimerInfo
    ) -> Iterator[pd.DataFrame]:

        user_flight_delay = self.flight_delay_state.getValue(key)
        user_profile = self.profile_state.getValue(key)
        user_preferences = self.preferences_state.getValue(key)

        if user_flight_delay:
            current_ts = user_flight_delay["timestamp"]
            last_emitted = self.last_emitted_state.getValue(key)

            if last_emitted is not None and current_ts <= last_emitted["last_emitted_ts"]:
                return iter([])  # Duplicate or old event — skip

            # Update last emitted state
            self.last_emitted_state.updateValue(key, (current_ts,))

            output_row = {
                "user_id": str(key[0]),
                "event_type": user_flight_delay["event_type"],
                "timestamp": user_flight_delay["timestamp"],
                "flight_id": user_flight_delay["flight_id"],
                "delay_duration": user_flight_delay["delay_duration"],
                "name": user_profile["name"] if user_profile else None,
                "email": user_profile["email"] if user_profile else None,
                "preferred_category": (
                    user_preferences["preferred_category"] if user_preferences else None
                ),
                "profile_updated_at": (
                    user_profile["profile_updated_at"]
                    if user_profile
                    else None
                ),
                "preference_updated_at": (
                    user_preferences["preference_updated_at"] if user_preferences else None
                ),
            }

            return iter([pd.DataFrame([output_row])])

        return iter(
            []
        )  # if there is no state from flight_delay state, do not output anything - since we are implementing a left join

    def close(self) -> None:
        pass

# COMMAND ----------

# MAGIC %md
# MAGIC #### Output Schema and Stream Execution

# COMMAND ----------

output_schema = StructType(
    [
        StructField("user_id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("flight_id", StringType()),
        StructField("delay_duration", StringType()),
        StructField("name", StringType()),
        StructField("email", StringType()),
        StructField("preferred_category", StringType()),
        StructField("profile_updated_at", TimestampType()),
        StructField("preference_updated_at", TimestampType()),
    ]
)

enriched_stream = union_stream.groupBy("user_id").transformWithStateInPandas(
    statefulProcessor=CustomStreamJoinProcessor(),
    outputStructType=output_schema,
    outputMode="Append",
    timeMode="ProcessingTime",
)

# COMMAND ----------

enriched_stream.display()