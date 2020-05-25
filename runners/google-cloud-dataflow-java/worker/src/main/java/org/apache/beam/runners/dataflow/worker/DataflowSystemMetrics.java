/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.dataflow.worker;

import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

/** This holds system metrics related constants used in Batch and Streaming. */
public class DataflowSystemMetrics {

  public static final MetricName THROTTLING_MSECS_METRIC_NAME =
      MetricName.named("dataflow-throttling-metrics", "throttling-msecs");

  // TODO: Provide an utility in SDK 'ThrottlingReporter' to update throttling time.

  /** System counters populated by streaming dataflow workers. */
  public enum StreamingSystemCounterNames {
    WINDMILL_SHUFFLE_BYTES_READ("WindmillShuffleBytesRead"),
    WINDMILL_STATE_BYTES_READ("WindmillStateBytesRead"),
    WINDMILL_STATE_BYTES_WRITTEN("WindmillStateBytesWritten"),
    WINDMILL_MAX_WORK_ITEM_COMMIT_BYTES("WindmillMaxWorkItemCommitBytes"),
    JAVA_HARNESS_USED_MEMORY("dataflow_java_harness_used_memory"),
    JAVA_HARNESS_MAX_MEMORY("dataflow_java_harness_max_memory"),
    JAVA_HARNESS_RESTARTS("dataflow_java_harness_restarts"),
    WINDMILL_QUOTA_THROTTLING("dataflow_streaming_engine_throttled_msecs"),
    MEMORY_THRASHING("dataflow_streaming_engine_user_worker_thrashing"),
    STATE_CACHE_WEIGHT("state_cache_weight"),
    STATE_CACHE_MAX_WEIGHT("state_cache_max_weight"),
    STATE_CACHE_SIZE("state_cache_size"),
    STATE_CACHE_HITS("state_cache_hits"),
    STATE_CACHE_REQUESTS("state_cache_requests"),
    STATE_CACHE_HIT_RATE("state_cache_hit_rate"),
    STATE_CACHE_EVICTIONS("state_cache_evictions"),
    STATE_CACHE_INVALIDATE_REQUESTS("state_cache_invalidate_requests"),
    STATE_CACHE_INVALIDATES_FROM_INCONSISTENT_TOKEN(
        "state_cache_invalidates_from_inconsistent_token"),
    STATE_CACHE_STALE_WORK_TOKEN_MISSES("state_cache_stale_work_token_misses"),
    COMMIT_DURATION_MS("commit_duration_ms"),
    COMMIT_SIZE_BYTES_PER_COMMIT("commit_size_bytes_per_commit"),
    COMMIT_SIZE_BYTES("commit_size_bytes"),
    CURRENT_COMMIT_SIZE_BYTES("current_commit_size_bytes"),
    WORK_ITEMS_RECEIVED("work_items_received"),
    GET_WORK_ITEM_BATCHES_RECEIVED("get_work_item_batches_received"),
    WORK_ITEMS_PER_BATCH("work_items_per_batch"),
    COMPUTATION_WORK_ITEMS_RECEIVED("computation_work_items_received"),
    GET_WORK_ITEM_WAIT_TIME_MS("get_work_item_wait_time_ms"),
    STATE_FETCH_BATCHES("state_fetch_batches"),
    STATE_FETCH_BATCH_SIZE("state_fetch_batch_size"),
    STATE_FETCH_LATENCY_MS("state_fetch_latency_ms"),
    MEMORY_MONITOR_IS_THRASHING("memory_monitor_is_thrashing"),
    MEMORY_MONITOR_NUM_PUSHBACKS("memory_monitor_num_pushbacks"),
    ;

    private final String name;

    StreamingSystemCounterNames(String name) {
      this.name = name;
    }

    public CounterName counterName() {
      return CounterName.named(name);
    }
  }

  /** System counters populated by streaming dataflow worker for each stage. */
  public enum StreamingPerStageSystemCounterNames {

    /**
     * Total amount of time spent processing a stage, aggregated across all the concurrent tasks for
     * a stage.
     */
    TOTAL_PROCESSING_MSECS("dataflow_total_processing_msecs"),

    /**
     * Total amount of time spent processing a stage, aggregated across all the concurrent tasks for
     * a stage.
     */
    TIMER_PROCESSING_MSECS("dataflow_timer_processing_msecs"),

    /**
     * This is based on user updated metric "throttled-msecs", reported as part of system metrics so
     * that streaming autoscaler can access it.
     */
    THROTTLED_MSECS("dataflow_throttled_msecs"),

    STATE_FETCHES("state_fetches_per_stage"),

    STATE_FETCH_LATENCY("state_fetch_latency_per_stage");

    private final String namePrefix;

    StreamingPerStageSystemCounterNames(String namePrefix) {
      this.namePrefix = namePrefix;
    }

    public CounterName counterName(NameContext nameContext) {
      Preconditions.checkNotNull(nameContext.systemName());
      return CounterName.named(namePrefix + "-" + nameContext.systemName());
    }
  }
}
