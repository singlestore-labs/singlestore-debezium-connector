package com.singlestore.debezium;

import static io.debezium.heartbeat.DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY;
import static io.debezium.heartbeat.Heartbeat.HEARTBEAT_INTERVAL;
import static io.debezium.schema.AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

public class MetricsIT extends IntegrationTestBase {

  @Test
  public void testHeartBeat() throws InterruptedException {
    Configuration config = defaultJdbcConfigWithTable("product");
    config = config.edit().withDefault(TOPIC_HEARTBEAT_PREFIX, "__heartbeat")
        .withDefault(HEARTBEAT_ACTION_QUERY, "SELECT 1").withDefault(HEARTBEAT_INTERVAL, 1000)
        .build();
    start(SingleStoreConnector.class, config);
    assertConnectorIsRunning();
    try {
      List<SourceRecord> records = consumeRecordsByTopic(1).allRecordsInOrder();
      assertEquals(1, records.size());
      assertEquals("__heartbeat.singlestore_topic", records.get(0).topic());
    } finally {
      stopConnector();
    }
  }

  @Test
  public void testCustomMetrics() throws Exception {
    //create snapshot
    String statements =
        "CREATE TABLE IF NOT EXISTS " + TEST_DATABASE
            + ".AM (pk INT, aa VARCHAR(50), PRIMARY KEY(pk));" +
            "DELETE FROM " + TEST_DATABASE + ".AM WHERE 1 = 1;" +
            "INSERT INTO " + TEST_DATABASE + ".AM VALUES(0, 'test0');" +
            "INSERT INTO " + TEST_DATABASE + ".AM VALUES(1, 'test1');" +
            "INSERT INTO " + TEST_DATABASE + ".AM VALUES(2, 'test2');" +
            "INSERT INTO " + TEST_DATABASE + ".AM VALUES(4, 'test4');" +
            "SNAPSHOT DATABASE " + TEST_DATABASE + ";";
    execute(statements);
    final Configuration config = defaultJdbcConfigBuilder().withDefault(
            SingleStoreConnectorConfig.DATABASE_NAME, TEST_DATABASE)
        .withDefault(SingleStoreConnectorConfig.TABLE_NAME, "AM")
        .with(CommonConnectorConfig.CUSTOM_METRIC_TAGS, "env=test,bu=bigdata").build();

    Map<String, String> customMetricTags = new SingleStoreConnectorConfig(
        config).getCustomMetricTags();
    start(SingleStoreConnector.class, config);
    assertConnectorIsRunning();
    assertSnapshotWithCustomMetrics(customMetricTags);
    assertStreamingWithCustomMetrics(customMetricTags);
  }

  private void assertSnapshotWithCustomMetrics(Map<String, String> customMetricTags)
      throws Exception {
    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = getSnapshotMetricsObjectName("singlestore", "singlestore_topic",
        customMetricTags);
    waitForSnapshotWithCustomMetricsToBeCompleted(customMetricTags);
    assertThat(mBeanServer.getAttribute(objectName, "TotalTableCount")).isEqualTo(1);
    assertThat(mBeanServer.getAttribute(objectName, "CapturedTables")).isEqualTo(
        new String[]{"db.AM"});
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(4L);
    assertThat(mBeanServer.getAttribute(objectName, "SnapshotRunning")).isEqualTo(false);
    assertThat(mBeanServer.getAttribute(objectName, "SnapshotAborted")).isEqualTo(false);
    assertThat(mBeanServer.getAttribute(objectName, "SnapshotCompleted")).isEqualTo(true);
    assertThat(mBeanServer.getAttribute(objectName, "SnapshotPaused")).isEqualTo(false);
    assertThat(mBeanServer.getAttribute(objectName, "SnapshotPausedDurationInSeconds")).isEqualTo(
        0L);
  }

  private void assertStreamingWithCustomMetrics(Map<String, String> customMetricTags)
      throws Exception {
    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = getStreamingMetricsObjectName("singlestore",
        "singlestore_topic", customMetricTags);
    // Insert for streaming events
    waitForStreamingWithCustomMetricsToStart(customMetricTags);
    String statements =
        "UPDATE " + TEST_DATABASE + ".AM SET aa = 'test1updated' WHERE pk = 1;" +
            "DELETE FROM " + TEST_DATABASE + ".AM WHERE pk = 2;" +
            "INSERT INTO " + TEST_DATABASE + ".AM VALUES(5, 'test1');" +
            "INSERT INTO " + TEST_DATABASE + ".AM VALUES(6, 'test2');";
    execute(statements);
    Thread.sleep(2000);
    // Check streaming metrics
    assertThat(mBeanServer.getAttribute(objectName, "Connected")).isEqualTo(true);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfUpdateEventsSeen")).isEqualTo(1L);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfDeleteEventsSeen")).isEqualTo(1L);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfCreateEventsSeen")).isEqualTo(2L);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(4L);
  }
}
