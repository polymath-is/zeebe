/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.system;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.clustering.ClusteringRule;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.broker.system.management.BrokerAdminService;
import io.zeebe.engine.processing.streamprocessor.StreamProcessor.Phase;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;

public class BrokerAdminServiceTest {
  private final Timeout testTimeout = Timeout.seconds(60);
  private final ClusteringRule clusteringRule =
      new ClusteringRule(1, 1, 1, cfg -> cfg.getData().setLogIndexDensity(1));
  private final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  private BrokerAdminService leaderAdminService;
  private Broker leader;

  @Before
  public void before() {
    leader = clusteringRule.getBroker(clusteringRule.getLeaderForPartition(1).getNodeId());
    leaderAdminService = leader.getBrokerAdminService();
  }

  @Test
  public void shouldTakeSnapshotWhenRequested() {
    // given
    clientRule.createSingleJob("test");

    // when
    leaderAdminService.takeSnapshot();

    // then
    waitForSnapshotAtBroker(leaderAdminService);
  }

  @Test
  public void shouldPauseStreamProcessorWhenRequested() {
    // given
    clientRule.createSingleJob("test");

    // when
    leaderAdminService.pauseStreamProcessing();

    // then
    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);
  }

  @Test
  public void shouldResumeStreamProcessorWhenRequested() {
    // given
    clientRule.createSingleJob("test");

    // when
    leaderAdminService.pauseStreamProcessing();
    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);
    leaderAdminService.resumeStreamProcessing();

    // then
    assertStreamProcessorPhase(leaderAdminService, Phase.PROCESSING);
  }

  @Test
  public void shouldPauseExporterWhenRequested() {
    // given
    clientRule.createSingleJob("test");

    // when
    leaderAdminService.pauseExporting();

    // then
    assertExporterPhase(leaderAdminService, "PAUSED");
  }

  @Test
  public void shouldResumeExportingWhenRequested() {
    // given
    clientRule.createSingleJob("test");

    // when
    leaderAdminService.pauseExporting();
    assertExporterPhase(leaderAdminService, "PAUSED");
    leaderAdminService.resumeExporting();

    // then
    assertExporterPhase(leaderAdminService, "EXPORTING");
  }

  @Test
  public void shouldPauseStreamProcessorAndExporterAndTakeSnapshotWhenPrepareUgrade() {
    // given
    clientRule.createSingleJob("test");

    // when
    leaderAdminService.prepareForUpgrade();

    // then
    waitForSnapshotAtBroker(leaderAdminService);

    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);
    assertExporterPhase(leaderAdminService, "PAUSED");
    assertProcessedPositionIsInSnapshot(leaderAdminService);
  }

  @Test
  public void shouldPauseStreamProcessorAfterRestart() {
    // given
    leaderAdminService.pauseStreamProcessing();
    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);

    // when
    clusteringRule.restartCluster();

    // then
    leader = clusteringRule.getBroker(clusteringRule.getLeaderForPartition(1).getNodeId());
    leaderAdminService = leader.getBrokerAdminService();
    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);
  }

  @Test
  public void shouldResumeStreamProcessorAfterRestart() {
    // given
    leaderAdminService.pauseStreamProcessing();
    assertStreamProcessorPhase(leaderAdminService, Phase.PAUSED);
    leaderAdminService.resumeStreamProcessing();
    assertStreamProcessorPhase(leaderAdminService, Phase.PROCESSING);

    // when
    clusteringRule.restartCluster();

    // then
    leader = clusteringRule.getBroker(clusteringRule.getLeaderForPartition(1).getNodeId());
    leaderAdminService = leader.getBrokerAdminService();
    assertStreamProcessorPhase(leaderAdminService, Phase.PROCESSING);
  }

  @Test
  public void shouldPauseExporterAfterRestart() {
    // given
    leaderAdminService.pauseExporting();
    assertExporterPhase(leaderAdminService, "PAUSED");

    // when
    clusteringRule.restartCluster();

    // then
    leader = clusteringRule.getBroker(clusteringRule.getLeaderForPartition(1).getNodeId());
    leaderAdminService = leader.getBrokerAdminService();
    assertExporterPhase(leaderAdminService, "PAUSED");
  }

  @Test
  public void shouldResumeExporterAfterRestart() {
    // given
    leaderAdminService.pauseExporting();
    assertExporterPhase(leaderAdminService, "PAUSED");
    leaderAdminService.resumeExporting();
    assertExporterPhase(leaderAdminService, "EXPORTING");

    // when
    clusteringRule.restartCluster();

    // then
    leader = clusteringRule.getBroker(clusteringRule.getLeaderForPartition(1).getNodeId());
    leaderAdminService = leader.getBrokerAdminService();
    assertExporterPhase(leaderAdminService, "EXPORTING");
  }

  private void assertStreamProcessorPhase(
      final BrokerAdminService brokerAdminService, final Phase expected) {
    Awaitility.await()
        .untilAsserted(
            () ->
                brokerAdminService
                    .getPartitionStatus()
                    .forEach(
                        (p, status) ->
                            assertThat(status.getStreamProcessorPhase()).isEqualTo(expected)));
  }

  private void assertExporterPhase(
      final BrokerAdminService brokerAdminService, final String expected) {
    Awaitility.await()
        .untilAsserted(
            () ->
                brokerAdminService
                    .getPartitionStatus()
                    .forEach(
                        (p, status) -> assertThat(status.getExporterPhase()).isEqualTo(expected)));
  }

  private void assertProcessedPositionIsInSnapshot(final BrokerAdminService brokerAdminService) {
    Awaitility.await()
        .untilAsserted(
            () ->
                brokerAdminService
                    .getPartitionStatus()
                    .forEach(
                        (p, status) ->
                            assertThat(status.getProcessedPosition())
                                .isEqualTo(status.getProcessedPositionInSnapshot())));
  }

  private void waitForSnapshotAtBroker(final BrokerAdminService adminService) {
    Awaitility.await()
        .untilAsserted(
            () ->
                adminService
                    .getPartitionStatus()
                    .values()
                    .forEach(
                        status -> assertThat(status.getProcessedPositionInSnapshot()).isNotNull()));
  }
}
