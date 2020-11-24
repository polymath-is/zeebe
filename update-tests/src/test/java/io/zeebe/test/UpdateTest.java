/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test;

import io.zeebe.util.VersionUtil;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ContainerStateExtension.class)
interface UpdateTest {
  String LAST_VERSION = VersionUtil.getPreviousVersion();
  String CURRENT_VERSION = "current-test";
}
