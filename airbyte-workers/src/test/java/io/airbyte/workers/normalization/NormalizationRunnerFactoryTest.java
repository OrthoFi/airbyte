/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.workers.normalization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.airbyte.workers.normalization.DefaultNormalizationRunner.DestinationType;
import io.airbyte.workers.process.ProcessFactory;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NormalizationRunnerFactoryTest {

  private ProcessFactory processFactory;

  @BeforeEach
  void setup() {
    processFactory = mock(ProcessFactory.class);
  }

  @Test
  void testMappings() {
    for (Entry<String, ImmutablePair<String, DestinationType>> entry : NormalizationRunnerFactory.NORMALIZATION_MAPPING.entrySet()) {
      assertEquals(entry.getValue().getValue(),
          ((DefaultNormalizationRunner) NormalizationRunnerFactory.create(
              String.format("%s:0.1.0", entry.getKey()), processFactory, "test")).getDestinationType());
    }
    assertThrows(IllegalStateException.class,
        () -> NormalizationRunnerFactory.create("airbyte/destination-csv:0.1.0", processFactory, "test"));
  }

}
