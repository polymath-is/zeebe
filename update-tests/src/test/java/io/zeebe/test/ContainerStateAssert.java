/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import static io.zeebe.test.UpdateTestCaseProvider.PROCESS_ID;
import static org.awaitility.Awaitility.await;

import io.zeebe.test.PartitionsActuatorClient.PartitionStatus;
import io.zeebe.util.Either;
import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;
import org.assertj.core.api.AbstractObjectAssert;
import org.awaitility.core.ConditionFactory;

public class ContainerStateAssert
    extends AbstractObjectAssert<ContainerStateAssert, ContainerState> {

  public ContainerStateAssert() {
    this(null);
  }

  public ContainerStateAssert(final ContainerState actual) {
    super(actual, ContainerStateAssert.class);
  }

  public static ContainerStateAssert assertThat(final ContainerState actual) {
    return new ContainerStateAssert(actual);
  }

  public ContainerStateAssert eventually() {
    return new EventualProxy(UnaryOperator.identity());
  }

  public ContainerStateAssert eventually(final UnaryOperator<ConditionFactory> conditionFactory) {
    return new EventualProxy(conditionFactory);
  }

  public ContainerStateAssert hasCompletedProcess(final String processId) {
    final boolean isCompleted = actual.hasElementInState(PROCESS_ID, "ELEMENT_COMPLETED");
    if (!isCompleted) {
      failWithMessage("expected process %s to be completed, but was not", processId);
    }

    return myself;
  }

  public ContainerStateAssert hasSnapshotAvailable(final int partitionId) {
    final Either<Throwable, Map<String, PartitionStatus>> response =
        actual.getPartitionsActuatorClient().queryPartitions();
    if (response.isLeft()) {
      failWithMessage("expected partitions query to be successful, but was %s", response.getLeft());
    }

    final Map<String, PartitionStatus> partitions = response.get();
    final PartitionStatus partitionStatus = partitions.get(String.valueOf(partitionId));
    if (partitionStatus == null) {
      failWithMessage(
          "expected partitions query to return info about partition %d, but got %s",
          partitionId, partitions.keySet());
    }

    if (partitionStatus.snapshotId == null || partitionStatus.snapshotId.isBlank()) {
      failWithMessage("expected to have a snapshot, but got nothing");
    }

    return myself;
  }

  public ContainerStateAssert hasNoSnapshotAvailable(final int partitionId) {
    final Either<Throwable, Map<String, PartitionStatus>> response =
        actual.getPartitionsActuatorClient().queryPartitions();
    if (response.isLeft()) {
      failWithMessage("expected partitions query to be successful, but was %s", response.getLeft());
    }

    final Map<String, PartitionStatus> partitions = response.get();
    final PartitionStatus partitionStatus = partitions.get(String.valueOf(partitionId));
    if (partitionStatus == null) {
      failWithMessage(
          "expected partitions query to return info about partition %d, but got %s",
          partitionId, partitions.keySet());
    }

    if (partitionStatus.snapshotId != null && !partitionStatus.snapshotId.isBlank()) {
      failWithMessage("expected to have no snapshot, but got %s", partitionStatus.snapshotId);
    }

    return myself;
  }

  private class EventualProxy extends ContainerStateAssert {
    private final UnaryOperator<ConditionFactory> conditionFactory;

    public EventualProxy(final UnaryOperator<ConditionFactory> conditionFactory) {
      this.conditionFactory = conditionFactory;
    }

    @Override
    public ContainerStateAssert hasCompletedProcess(final String processId) {
      conditionFactory
          .apply(defaultConditionFactory().await("has completed process " + processId))
          .untilAsserted(() -> ContainerStateAssert.this.hasCompletedProcess(processId));
      return myself;
    }

    @Override
    public ContainerStateAssert hasSnapshotAvailable(final int partitionId) {
      conditionFactory
          .apply(
              defaultConditionFactory().await("has snapshot available on partition " + partitionId))
          .untilAsserted(() -> ContainerStateAssert.this.hasSnapshotAvailable(partitionId));
      return myself;
    }

    @Override
    public ContainerStateAssert hasNoSnapshotAvailable(final int partitionId) {
      conditionFactory
          .apply(
              defaultConditionFactory()
                  .await("has no snapshot available on partition " + partitionId))
          .untilAsserted(() -> ContainerStateAssert.this.hasNoSnapshotAvailable(partitionId));
      return myself;
    }

    private ConditionFactory defaultConditionFactory() {
      return await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(250));
    }
  }
}
