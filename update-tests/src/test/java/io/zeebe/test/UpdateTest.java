/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import static io.zeebe.test.UpdateTestCaseProvider.PROCESS_ID;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.test.PartitionsActuatorClient.PartitionStatus;
import io.zeebe.test.util.asserts.EitherAssert;
import io.zeebe.util.Either;
import io.zeebe.util.VersionUtil;
import java.time.Duration;
import java.util.Map;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ContainerStateExtension.class)
interface UpdateTest {
  String LAST_VERSION = VersionUtil.getPreviousVersion();
  String CURRENT_VERSION = "current-test";

  default void awaitProcessCompletion(final ContainerState state) {
    Awaitility.await("until process is completed")
        .atMost(Duration.ofSeconds(5))
        .pollInterval(Duration.ofMillis(200))
        .untilAsserted(
            () -> assertThat(state.hasElementInState(PROCESS_ID, "ELEMENT_COMPLETED")).isTrue());
  }

  default void assertSnapshotAvailable(final ContainerState state) {
    final Either<Throwable, Map<String, PartitionStatus>> response =
        state.getPartitionsActuatorClient().queryPartitions();
    EitherAssert.assertThat(response).isRight();

    final PartitionStatus partitionStatus = response.get().get("1");
    assertThat(partitionStatus).isNotNull();
    assertThat(partitionStatus.snapshotId).isNotBlank();
  }

  default void assertNoSnapshotAvailable(final ContainerState state) {
    final Either<Throwable, Map<String, PartitionStatus>> response =
        state.getPartitionsActuatorClient().queryPartitions();
    EitherAssert.assertThat(response).isRight();

    final PartitionStatus partitionStatus = response.get().get("1");
    assertThat(partitionStatus).isNotNull();
    assertThat(partitionStatus.snapshotId).isBlank();
  }
}
