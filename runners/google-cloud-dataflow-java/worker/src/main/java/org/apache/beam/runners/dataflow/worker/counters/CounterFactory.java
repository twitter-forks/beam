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
package org.apache.beam.runners.dataflow.worker.counters;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import com.google.auto.value.AutoValue;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;
import org.HdrHistogram.Recorder;
import org.apache.beam.runners.dataflow.worker.counters.Counter.AtomicCounterValue;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.math.LongMath;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.AtomicDouble;

/** Factory interface for creating counters. */
public class CounterFactory {

  protected <InputT, AccumT> Counter<InputT, AccumT> createCounter(
      CounterName name, AtomicCounterValue<InputT, AccumT> counterValue) {
    return new Counter<>(name, counterValue);
  }

  /** Create a long sum counter. */
  public Counter<Long, Long> longSum(CounterName name) {
    return createCounter(name, new LongSumCounterValue());
  }

  /** Create a long min counter. */
  public Counter<Long, Long> longMin(CounterName name) {
    return createCounter(name, new LongMinCounterValue());
  }

  /** Create a long max counter. */
  public Counter<Long, Long> longMax(CounterName name) {
    return createCounter(name, new LongMaxCounterValue());
  }

  /** Create a long mean counter. */
  public Counter<Long, CounterMean<Long>> longMean(CounterName name) {
    return createCounter(name, new LongMeanCounterValue());
  }

  /** Create an integer sum counter. */
  public Counter<Integer, Integer> intSum(CounterName name) {
    return createCounter(name, new IntegerSumCounterValue());
  }

  /** Create an integer min counter. */
  public Counter<Integer, Integer> intMin(CounterName name) {
    return createCounter(name, new IntegerMinCounterValue());
  }

  /** Create an integer max counter. */
  public Counter<Integer, Integer> intMax(CounterName name) {
    return createCounter(name, new IntegerMaxCounterValue());
  }

  /** Create an integer mean counter. */
  public Counter<Integer, CounterMean<Integer>> intMean(CounterName name) {
    return createCounter(name, new IntegerMeanCounterValue());
  }

  /** Create a double sum counter. */
  public Counter<Double, Double> doubleSum(CounterName name) {
    return createCounter(name, new DoubleSumCounterValue());
  }

  /** Create a double min counter. */
  public Counter<Double, Double> doubleMin(CounterName name) {
    return createCounter(name, new DoubleMinCounterValue());
  }

  /** Create a double max counter. */
  public Counter<Double, Double> doubleMax(CounterName name) {
    return createCounter(name, new DoubleMaxCounterValue());
  }

  /** Create a double mean counter. */
  public Counter<Double, CounterMean<Double>> doubleMean(CounterName name) {
    return createCounter(name, new DoubleMeanCounterValue());
  }

  /** Create a boolean OR counter. */
  public Counter<Boolean, Boolean> booleanOr(CounterName name) {
    return createCounter(name, new BooleanOrCounterValue());
  }

  /** Create a boolean AND counter. */
  public Counter<Boolean, Boolean> booleanAnd(CounterName name) {
    return createCounter(name, new BooleanAndCounterValue());
  }

  /** Create a value distribution counter. */
  public Counter<Long, CounterDistribution> distribution(CounterName name) {
    return createCounter(name, new DistributionCounterValue());
  }

  /**
   * An immutable object that contains a sum of type {@code T} and a count of how many values have
   * been added.
   */
  public interface CounterMean<T> {
    /** Gets the aggregate value of this {@code CounterMean}. */
    T getAggregate();

    /** Gets the count of this {@code CounterMean}. */
    long getCount();

    /** Return the {@link CounterMean} resulting from adding the given value. */
    CounterMean<T> addValue(T value);

    /** Return the {@link CounterMean} resulting from adding the given sum and count. */
    CounterMean<T> addValue(T sum, long count);
  }

  /**
   * An immutable object that contains value distribution statistics and methods for incrementing.
   */
  @AutoValue
  public abstract static class CounterDistribution {
    CounterDistribution() {}

    public abstract long getMin();

    public abstract long getMax();

    public abstract long getCount();

    public abstract long getSum();

    // Use a double since the sum of squares is likely to overflow 64-bit integer.
    public abstract double getSumOfSquares();

    /**
     * Histogram buckets of value counts for a distribution.
     *
     * <p>Buckets have an inclusive lower bound and exclusive upper bound and use "1,2,5 bucketing".
     * For detailed explanation, refer to comments on the Histogram message in:
     * //google/dataflow/service/v1b3/work_items.proto
     */
    public abstract List<Long> getBuckets();

    /** Starting index of the first stored bucket. */
    public abstract int getFirstBucketOffset();

    /** Helper for constructing a specific {@link CounterDistribution}. */
    public static Builder builder() {
      return new AutoValue_CounterFactory_CounterDistribution.Builder();
    }

    /** Builder for creating {@link CounterDistribution} instances. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder min(long value);

      public abstract Builder max(long value);

      public abstract Builder count(long value);

      public abstract Builder sum(long value);

      public abstract Builder sumOfSquares(double value);

      public abstract Builder buckets(List<Long> buckets);

      public abstract Builder firstBucketOffset(int offset);

      public abstract CounterDistribution build();

      public final Builder minMax(long min, long max) {
        return this.min(min).max(max);
      }

      public final Builder buckets(int firstBucketOffset, List<Long> buckets) {
        return this.firstBucketOffset(firstBucketOffset).buckets(buckets);
      }
    }

    private static final CounterDistribution EMPTY =
        CounterDistribution.builder()
            .minMax(Long.MAX_VALUE, 0L)
            .count(0L)
            .sum(0L)
            .sumOfSquares(0.0)
            .buckets(0, ImmutableList.of())
            .build();

    /** Retrieve the empty distribution. */
    public static CounterDistribution empty() {
      return EMPTY;
    }

    /** There are 3 buckets for every power of ten: 1, 2, and 5. */
    private static final int BUCKETS_PER_10 = 3;

    /** Calculate the bucket index for the given value. */
    @VisibleForTesting
    static int calculateBucket(long value) {
      if (value == 0) {
        return 0;
      }

      int log10Floor = LongMath.log10(value, RoundingMode.FLOOR);
      long powerOfTen = LongMath.pow(10, log10Floor);
      int bucketOffsetWithinPowerOf10;
      if (value < 2 * powerOfTen) {
        bucketOffsetWithinPowerOf10 = 0; // [0, 2)
      } else if (value < 5 * powerOfTen) {
        bucketOffsetWithinPowerOf10 = 1; // [2, 5)
      } else {
        bucketOffsetWithinPowerOf10 = 2; // [5, 10)
      }

      return 1 + (log10Floor * BUCKETS_PER_10) + bucketOffsetWithinPowerOf10;
    }
  }

  private abstract static class BaseCounterValue<InputT, AccumT>
      implements AtomicCounterValue<InputT, AccumT> {
    protected AccumT extractValue(boolean delta) {
      return delta ? getAndReset() : getAggregate();
    }
  }

  /** Base class for Long-counters that use a long to track their aggregate. */
  private abstract static class LongCounterValue extends BaseCounterValue<Long, Long> {
    protected final AtomicLong aggregate = new AtomicLong();

    @Override
    public Long getAggregate() {
      return aggregate.get();
    }
  }

  /** Implements a {@link Counter} for tracking the minimum long value. */
  public static class LongMinCounterValue extends LongCounterValue {

    @Override
    public void addValue(Long value) {
      long current;
      long update;
      do {
        current = aggregate.get();
        update = Math.min(value, current);
      } while (update < current && !aggregate.compareAndSet(current, update));
    }

    @Override
    public Long getAndReset() {
      return aggregate.getAndSet(Long.MAX_VALUE);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.longMin(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the maximum long value. */
  public static class LongMaxCounterValue extends LongCounterValue {
    @Override
    public void addValue(Long value) {
      long current;
      long update;
      do {
        current = aggregate.get();
        update = Math.max(value, current);
      } while (update > current && !aggregate.compareAndSet(current, update));
    }

    @Override
    public Long getAndReset() {
      return aggregate.getAndSet(Long.MIN_VALUE);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.longMax(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the sum of long values. */
  public static class LongSumCounterValue extends LongCounterValue {
    @Override
    public void addValue(Long value) {
      aggregate.addAndGet(value);
    }

    @Override
    public Long getAndReset() {
      return aggregate.getAndSet(0);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.longSum(name, delta, extractValue(delta));
    }
  }

  private abstract static class BaseMeanCounterValue<T>
      extends BaseCounterValue<T, CounterMean<T>> {
    private final AtomicReference<CounterMean<T>> aggregate = new AtomicReference<>();

    @Override
    public void addValue(T value) {
      CounterMean<T> current;
      CounterMean<T> update;
      do {
        current = aggregate.get();
        update = current.addValue(value);
      } while (!aggregate.compareAndSet(current, update));
    }

    @Override
    public CounterMean<T> getAggregate() {
      return aggregate.get();
    }

    @Override
    public CounterMean<T> getAndReset() {
      return aggregate.getAndSet(zero());
    }

    /** Return the zero of the mean counter. */
    protected abstract CounterMean<T> zero();
  }

  /** Implements a {@link Counter} for tracking the mean of long values. */
  public static class LongMeanCounterValue extends BaseMeanCounterValue<Long> {
    @Override
    protected CounterMean<Long> zero() {
      return LongCounterMean.ZERO;
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.longMean(name, delta, extractValue(delta));
    }
  }

  /** Base class for Integer counters that use an AtomicInteger to track their aggregate. */
  private abstract static class IntegerCounterValue extends BaseCounterValue<Integer, Integer> {
    protected final AtomicInteger aggregate = new AtomicInteger();

    @Override
    public Integer getAggregate() {
      return aggregate.get();
    }
  }

  /** Implements a {@link Counter} for tracking the minimum integer value. */
  public static class IntegerMinCounterValue extends IntegerCounterValue {
    @Override
    public void addValue(Integer value) {
      int current;
      int update;
      do {
        current = aggregate.get();
        update = Math.min(value, current);
      } while (update < current && !aggregate.compareAndSet(current, update));
    }

    @Override
    public Integer getAndReset() {
      return aggregate.getAndSet(Integer.MAX_VALUE);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.intMin(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the maximum integer value. */
  public static class IntegerMaxCounterValue extends IntegerCounterValue {
    @Override
    public void addValue(Integer value) {
      int current;
      int update;
      do {
        current = aggregate.get();
        update = Math.max(value, current);
      } while (update > current && !aggregate.compareAndSet(current, update));
    }

    @Override
    public Integer getAndReset() {
      return aggregate.getAndSet(Integer.MIN_VALUE);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.intMax(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the sum of integer values. */
  public static class IntegerSumCounterValue extends IntegerCounterValue {
    @Override
    public void addValue(Integer value) {
      aggregate.addAndGet(value);
    }

    @Override
    public Integer getAndReset() {
      return aggregate.getAndSet(0);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.intSum(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the mean of integer values. */
  public static class IntegerMeanCounterValue extends BaseMeanCounterValue<Integer> {
    @Override
    protected CounterMean<Integer> zero() {
      return IntegerCounterMean.ZERO;
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.intMean(name, delta, extractValue(delta));
    }
  }

  /** Base class for Double counters that use an AtomicDouble to track their aggregate. */
  private abstract static class DoubleCounterValue extends BaseCounterValue<Double, Double> {
    protected final AtomicDouble aggregate = new AtomicDouble();

    @Override
    public Double getAggregate() {
      return aggregate.get();
    }
  }

  /** Implements a {@link Counter} for tracking the minimum double value. */
  public static class DoubleMinCounterValue extends DoubleCounterValue {

    @Override
    public void addValue(Double value) {
      double current;
      double update;
      do {
        current = aggregate.get();
        update = Math.min(value, current);
      } while (update < current && !aggregate.compareAndSet(current, update));
    }

    @Override
    public Double getAndReset() {
      return aggregate.getAndSet(Double.POSITIVE_INFINITY);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.doubleMin(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the maximum double value. */
  public static class DoubleMaxCounterValue extends DoubleCounterValue {
    @Override
    public void addValue(Double value) {
      double current;
      double update;
      do {
        current = aggregate.get();
        update = Math.max(value, current);
      } while (update > current && !aggregate.compareAndSet(current, update));
    }

    @Override
    public Double getAndReset() {
      return aggregate.getAndSet(Double.NEGATIVE_INFINITY);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.doubleMax(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the sum of double values. */
  public static class DoubleSumCounterValue extends DoubleCounterValue {
    @Override
    public void addValue(Double value) {
      aggregate.addAndGet(value);
    }

    @Override
    public Double getAndReset() {
      return aggregate.getAndSet(0);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.doubleSum(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for tracking the mean of double values. */
  public static class DoubleMeanCounterValue extends BaseMeanCounterValue<Double> {
    @Override
    protected CounterMean<Double> zero() {
      return DoubleCounterMean.ZERO;
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.doubleMean(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for {@link Boolean} that aggregate via AND. */
  public static class BooleanAndCounterValue extends BaseCounterValue<Boolean, Boolean> {
    private final AtomicBoolean aggregate = new AtomicBoolean();

    @Override
    public void addValue(Boolean value) {
      if (!value) {
        aggregate.set(value);
      }
    }

    @Override
    public Boolean getAndReset() {
      return aggregate.getAndSet(true);
    }

    @Override
    public Boolean getAggregate() {
      return aggregate.get();
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.boolAnd(name, delta, extractValue(delta));
    }
  }

  /** Implements a {@link Counter} for {@link Boolean} that aggregate via AND. */
  public static class BooleanOrCounterValue extends BaseCounterValue<Boolean, Boolean> {
    private final AtomicBoolean aggregate = new AtomicBoolean();

    @Override
    public void addValue(Boolean value) {
      if (value) {
        aggregate.set(value);
      }
    }

    @Override
    public Boolean getAndReset() {
      return aggregate.getAndSet(false);
    }

    @Override
    public Boolean getAggregate() {
      return aggregate.get();
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.boolOr(name, delta, extractValue(delta));
    }
  }

  /** Class for representing a long-valued mean. */
  public static class LongCounterMean implements CounterMean<Long> {

    public static final CounterMean<Long> ZERO = new LongCounterMean(0L, 0L);

    private final long aggregate;
    private final long count;

    private LongCounterMean(long aggregate, long count) {
      this.aggregate = aggregate;
      this.count = count;
    }

    @Override
    public Long getAggregate() {
      return aggregate;
    }

    @Override
    public long getCount() {
      return count;
    }

    @Override
    public CounterMean<Long> addValue(Long value) {
      return new LongCounterMean(aggregate + value, count + 1);
    }

    @Override
    public CounterMean<Long> addValue(Long sum, long newCount) {
      return new LongCounterMean(aggregate + sum, count + newCount);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof LongCounterMean)) {
        return false;
      }
      LongCounterMean that = (LongCounterMean) obj;
      return this.aggregate == that.aggregate && this.count == that.count;
    }

    @Override
    public int hashCode() {
      return Objects.hash(aggregate, count);
    }

    @Override
    public String toString() {
      return aggregate + "/" + count;
    }
  }

  /** Class for representing a long-valued mean. */
  public static class IntegerCounterMean implements CounterMean<Integer> {

    public static final CounterMean<Integer> ZERO = new IntegerCounterMean(0, 0L);

    private final int aggregate;
    private final long count;

    private IntegerCounterMean(int aggregate, long count) {
      this.aggregate = aggregate;
      this.count = count;
    }

    @Override
    public Integer getAggregate() {
      return aggregate;
    }

    @Override
    public long getCount() {
      return count;
    }

    @Override
    public CounterMean<Integer> addValue(Integer value) {
      return new IntegerCounterMean(aggregate + value, count + 1);
    }

    @Override
    public CounterMean<Integer> addValue(Integer sum, long newCount) {
      return new IntegerCounterMean(aggregate + sum, count + newCount);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof IntegerCounterMean)) {
        return false;
      }
      IntegerCounterMean that = (IntegerCounterMean) obj;
      return this.aggregate == that.aggregate && this.count == that.count;
    }

    @Override
    public int hashCode() {
      return Objects.hash(aggregate, count);
    }

    @Override
    public String toString() {
      return aggregate + "/" + count;
    }
  }

  /** Class for representing a long-valued mean. */
  public static class DoubleCounterMean implements CounterMean<Double> {

    public static final CounterMean<Double> ZERO = new DoubleCounterMean(0.0, 0L);

    private final double aggregate;
    private final long count;

    private DoubleCounterMean(double aggregate, long count) {
      this.aggregate = aggregate;
      this.count = count;
    }

    @Override
    public Double getAggregate() {
      return aggregate;
    }

    @Override
    public long getCount() {
      return count;
    }

    @Override
    public CounterMean<Double> addValue(Double value) {
      return new DoubleCounterMean(aggregate + value, count + 1);
    }

    @Override
    public CounterMean<Double> addValue(Double sum, long newCount) {
      return new DoubleCounterMean(aggregate + sum, count + newCount);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      } else if (!(obj instanceof DoubleCounterMean)) {
        return false;
      }
      DoubleCounterMean that = (DoubleCounterMean) obj;
      return this.aggregate == that.aggregate && this.count == that.count;
    }

    @Override
    public int hashCode() {
      return Objects.hash(aggregate, count);
    }

    @Override
    public String toString() {
      return aggregate + "/" + count;
    }
  }

  /** Implements a {@link Counter} for tracking a distribution of long values. */
  public static class DistributionCounterValue extends BaseCounterValue<Long, CounterDistribution> {
    private final static AtomicLongFieldUpdater<DistributionCounterValue> SUM_UPDATER =
        AtomicLongFieldUpdater.newUpdater(DistributionCounterValue.class, "sumValue");

    private final static AtomicLongFieldUpdater<DistributionCounterValue> SUM_OF_SQUARES_UPDATER =
        AtomicLongFieldUpdater.newUpdater(DistributionCounterValue.class, "sumOfSquares");

    private final Recorder histogram = new Recorder(2);

    private volatile long sumValue = 0;
    private volatile long sumOfSquares = 0;

    @Override
    public void addValue(Long value) {
      histogram.recordValue(value);
      SUM_UPDATER.addAndGet(this, value);
      SUM_OF_SQUARES_UPDATER.addAndGet(this, (long) Math.pow(value, 2));
    }

    private static CounterDistribution toCounterDistribution(Histogram v, long sum, double sumOfSquares) {
      if (v.getTotalCount() == 0)
        return CounterDistribution.EMPTY;

      List<Long> buckets = new ArrayList<>();
      int firstBucket = -1;
      for (HistogramIterationValue r : v.recordedValues()) {
        int bucket = CounterDistribution.calculateBucket(r.getValueIteratedTo());
        if (firstBucket == -1)
          firstBucket = bucket;

        if (bucket >= buckets.size()) {
          for (int i = buckets.size(); i <= bucket; i++) {
            buckets.add(0L);
          }
        }

        Long existingValue = buckets.get(bucket);
        buckets.set(bucket, existingValue + r.getCountAtValueIteratedTo());
      }

      if (firstBucket > 0) {
        buckets = buckets.subList(firstBucket, buckets.size());
      }

      return CounterDistribution
          .builder()
          .min(v.getMinValue())
          .max(v.getMaxValue())
          .count(v.getTotalCount())
          .sum(sum)
          .sumOfSquares(sumOfSquares)
          .buckets(firstBucket, ImmutableList.copyOf(buckets))
          .build();
    }

    @Override
    public CounterDistribution getAggregate() {
      throw new UnsupportedOperationException("Cumulative histograms are unsupported");
    }

    @Override
    public CounterDistribution getAndReset() {
      final Histogram snapshot;
      final long sum;
      final double sumOfSquares;

      snapshot = histogram.getIntervalHistogram();
      histogram.reset();

      sum = SUM_UPDATER.getAndSet(this, 0);
      sumOfSquares = SUM_OF_SQUARES_UPDATER.getAndSet(this, 0);

      return toCounterDistribution(snapshot, sum, sumOfSquares);
    }

    @Override
    public <UpdateT> UpdateT extractUpdate(
        CounterName name, boolean delta, CounterUpdateExtractor<UpdateT> updateExtractor) {
      return updateExtractor.distribution(name, delta, extractValue(delta));
    }
  }
}
