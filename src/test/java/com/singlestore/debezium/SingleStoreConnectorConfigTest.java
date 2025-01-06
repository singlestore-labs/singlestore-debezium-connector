package com.singlestore.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.debezium.config.Configuration;
import java.util.List;
import org.junit.Test;

public class SingleStoreConnectorConfigTest {

  @Test
  public void testDriverParametersValidationSuccessful() {
    SingleStoreConnectorConfig config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("driver.parameters", "asd=123;qwe=234")
            .build());

    assertTrue(config.validate(List.of(SingleStoreConnectorConfig.DRIVER_PARAMETERS),
        (field, value, problemMessage) -> {
          assert false;
        }));

    config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("driver.parameters", "")
            .build());

    assertTrue(config.validate(List.of(SingleStoreConnectorConfig.DRIVER_PARAMETERS),
        (field, value, problemMessage) -> {
          assert false;
        }));
  }

  @Test
  public void testDriverParametersValidationFailure() {
    SingleStoreConnectorConfig config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("driver.parameters", "asd=123;qwe=234=234")
            .build());

    assertFalse(config.validate(List.of(SingleStoreConnectorConfig.DRIVER_PARAMETERS),
        (field, value, problemMessage) -> {
          assertEquals("Invalid parameter: 'qwe=234=234'", problemMessage);
        }));

    config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("driver.parameters", "asd=123;qwe")
            .build());

    assertFalse(config.validate(List.of(SingleStoreConnectorConfig.DRIVER_PARAMETERS),
        (field, value, problemMessage) -> {
          assertEquals("Invalid parameter: 'qwe'", problemMessage);
        }));

    config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("driver.parameters", "asd=123;qwe=234=234;qwe")
            .build());

    int[] parameterIndex = {0};
    assertFalse(config.validate(List.of(SingleStoreConnectorConfig.DRIVER_PARAMETERS),
        (field, value, problemMessage) -> {
          if (parameterIndex[0] == 0) {
            assertEquals("Invalid parameter: 'qwe=234=234'", problemMessage);
            parameterIndex[0]++;
          } else {
            assertEquals("Invalid parameter: 'qwe'", problemMessage);
          }
        }));
  }

  @Test
  public void testOffsetsValidationSuccessful() {
    SingleStoreConnectorConfig config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("offsets", "0123456789abcdefABCDEF,NULL,NULL,0123")
            .build());

    assertTrue(config.validate(List.of(SingleStoreConnectorConfig.OFFSETS),
        (field, value, problemMessage) -> {
          assert false;
        }));

    config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("offsets", "")
            .build());

    assertTrue(config.validate(List.of(SingleStoreConnectorConfig.OFFSETS),
        (field, value, problemMessage) -> {
          assert false;
        }));
  }

  @Test
  public void testOffsetsValidationFailure() {
    SingleStoreConnectorConfig config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("offsets", "0123456789abcdefABCDEFGE,NULL,NULL,0123")
            .build());

    assertFalse(config.validate(List.of(SingleStoreConnectorConfig.OFFSETS),
        (field, value, problemMessage) -> {
          assertEquals("Invalid offset: '0123456789abcdefABCDEFGE'", problemMessage);
        }));

    config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("offsets", "0123456789abcdefABCDEF,NULL1,NULL,0123")
            .build());

    assertFalse(config.validate(List.of(SingleStoreConnectorConfig.OFFSETS),
        (field, value, problemMessage) -> {
          assertEquals("Invalid offset: 'NULL1'", problemMessage);
        }));

    config = new SingleStoreConnectorConfig(
        Configuration.create()
            .with("offsets", "0123456789abcdefABCDEFGE,NULL1,NULL,0123")
            .build());

    int[] offsetIndex = {0};
    assertFalse(config.validate(List.of(SingleStoreConnectorConfig.OFFSETS),
        (field, value, problemMessage) -> {
          if (offsetIndex[0] == 0) {
            assertEquals("Invalid offset: '0123456789abcdefABCDEFGE'", problemMessage);
            offsetIndex[0]++;
          } else {
            assertEquals("Invalid offset: 'NULL1'", problemMessage);
          }
        }));
  }
}
