/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.health;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.zeebe.client.ZeebeClient;
import io.zeebe.containers.ZeebeContainer;
import io.zeebe.test.util.testcontainers.ManagedVolume;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.agrona.CloseHelper;
import org.elasticsearch.client.RestClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

public class DiskSpaceRecoveryITTest {
  private static final Logger LOG = LoggerFactory.getLogger(DiskSpaceRecoveryITTest.class);
  private static final String ELASTIC_HOSTNAME = "elastic";
  private static final String ELASTIC_HOST = "http://" + ELASTIC_HOSTNAME + ":9200";

  private Network network;
  private ZeebeContainer zeebeBroker;
  private ElasticsearchContainer elastic;
  private ZeebeClient client;

  @Before
  public void setUp() {
    network = Network.newNetwork();
    zeebeBroker = createZeebe().withNetwork(network);
    elastic = createElastic().withNetwork(network);
  }

  @After
  public void tearDown() {
    if (zeebeBroker != null) {
      CloseHelper.quietClose(zeebeBroker);
    }

    if (elastic != null) {
      CloseHelper.quietClose(elastic);
    }

    if (network != null) {
      CloseHelper.quietClose(network);
    }
  }

  @Test
  public void shouldRecoverAfterOutOfDiskSpaceWhenExporterStarts() {
    // given
    zeebeBroker.start();
    client = createClient();

    // when
    LOG.info("Wait until broker is out of disk space");
    await()
        .timeout(Duration.ofMinutes(3))
        .pollInterval(1, TimeUnit.MICROSECONDS)
        .untilAsserted(
            () ->
                assertThatThrownBy(this::publishMessage)
                    .hasRootCauseMessage(
                        "RESOURCE_EXHAUSTED: Cannot accept requests for partition 1. Broker is out of disk space"));

    LOG.info("Start Elastic and wait until broker compacts to make more space");
    elastic.start();

    // then
    await()
        .pollInterval(Duration.ofSeconds(10))
        .timeout(Duration.ofMinutes(3))
        .untilAsserted(() -> assertThatCode(this::publishMessage).doesNotThrowAnyException());
  }

  private ZeebeClient createClient() {
    return ZeebeClient.newClientBuilder()
        .gatewayAddress(zeebeBroker.getExternalGatewayAddress())
        .usePlaintext()
        .build();
  }

  @Test
  public void shouldNotProcessWhenOutOfDiskSpaceOnStart() {
    // given
    zeebeBroker.withEnv("ZEEBE_BROKER_DATA_DISKUSAGECOMMANDWATERMARK", "0.0001");

    // when
    zeebeBroker.start();
    client = createClient();

    // then
    assertThatThrownBy(this::publishMessage)
        .hasRootCauseMessage(
            "RESOURCE_EXHAUSTED: Cannot accept requests for partition 1. Broker is out of disk space");
  }

  private void publishMessage() {
    client
        .newPublishMessageCommand()
        .messageName("test")
        .correlationKey(String.valueOf(1))
        .send()
        .join();
  }

  private ZeebeContainer createZeebe() {
    final Map<String, String> volumeOptions =
        Map.of("type", "tmpfs", "device", "tmpfs", "o", "size=16m");
    final var volume =
        ManagedVolume.newVolume(cmd -> cmd.withDriver("local").withDriverOpts(volumeOptions));

    return new ZeebeContainer("camunda/zeebe:current-test")
        .withEnv(
            "ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_CLASSNAME",
            "io.zeebe.exporter.ElasticsearchExporter")
        .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_URL", ELASTIC_HOST)
        .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULK_DELAY", "1")
        .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULK_SIZE", "1")
        .withEnv("ZEEBE_BROKER_DATA_SNAPSHOTPERIOD", "1m")
        .withEnv("ZEEBE_BROKER_DATA_LOGSEGMENTSIZE", "1MB")
        .withEnv("ZEEBE_BROKER_NETWORK_MAXMESSAGESIZE", "1MB")
        .withEnv("ZEEBE_BROKER_DATA_DISKUSAGECOMMANDWATERMARK", "0.5")
        .withEnv("ZEEBE_BROKER_DATA_LOGINDEXDENSITY", "1")
        .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_INDEX_MESSAGE", "true")
        .withCreateContainerCmdModifier(volume::attachVolumeToContainer);
  }

  private ElasticsearchContainer createElastic() {
    final String image =
        "docker.elastic.co/elasticsearch/elasticsearch:"
            + RestClient.class.getPackage().getImplementationVersion();

    return new ElasticsearchContainer(image)
        .withEnv("discovery.type", "single-node")
        .withNetworkAliases(ELASTIC_HOSTNAME);
  }
}
