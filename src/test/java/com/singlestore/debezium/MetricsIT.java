package com.singlestore.debezium;

import static io.debezium.heartbeat.DatabaseHeartbeatImpl.HEARTBEAT_ACTION_QUERY;
import static io.debezium.heartbeat.Heartbeat.HEARTBEAT_INTERVAL;
import static io.debezium.schema.AbstractTopicNamingStrategy.TOPIC_HEARTBEAT_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import java.lang.management.ManagementFactory;
import java.time.Duration;
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
    start(SingleStoreDBConnector.class, config);
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
        "DROP TABLE IF EXISTS " + TEST_DATABASE + ".A;" +
            "CREATE TABLE " + TEST_DATABASE + ".A (pk INT, aa VARCHAR(50), PRIMARY KEY(pk));" +
            "INSERT INTO " + TEST_DATABASE + ".A VALUES(0, 'test0');" +
            "INSERT INTO " + TEST_DATABASE + ".A VALUES(1, 'test1');" +
            "INSERT INTO " + TEST_DATABASE + ".A VALUES(2, 'test2');" +
            "INSERT INTO " + TEST_DATABASE + ".A VALUES(4, 'test4');" +
            "SNAPSHOT DATABASE " + TEST_DATABASE + ";";
    execute(statements);
    final Configuration config = defaultJdbcConfigBuilder().withDefault(
            SingleStoreDBConnectorConfig.DATABASE_NAME, TEST_DATABASE)
        .withDefault(SingleStoreDBConnectorConfig.TABLE_NAME, "A")
        .with(CommonConnectorConfig.CUSTOM_METRIC_TAGS, "env=test,bu=bigdata").build();

    Map<String, String> customMetricTags = new SingleStoreDBConnectorConfig(
        config).getCustomMetricTags();
    start(SingleStoreDBConnector.class, config);
    assertConnectorIsRunning();
    assertSnapshotWithCustomMetrics(customMetricTags);
    assertStreamingWithCustomMetrics(customMetricTags);
  }

  private void assertSnapshotWithCustomMetrics(Map<String, String> customMetricTags)
      throws Exception {
    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName = getSnapshotMetricsObjectName("singlestoredb", "singlestore_topic",
        customMetricTags);
    waitForSnapshotWithCustomMetricsToBeCompleted(customMetricTags);
    assertThat(mBeanServer.getAttribute(objectName, "TotalTableCount")).isEqualTo(1);
    assertThat(mBeanServer.getAttribute(objectName, "CapturedTables")).isEqualTo(
        new String[]{"db.A"});
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
    final ObjectName objectName = getStreamingMetricsObjectName("singlestoredb",
        "singlestore_topic", customMetricTags);
    // Insert for streaming events
    waitForStreamingWithCustomMetricsToStart(customMetricTags);
    String statements =
            "UPDATE " + TEST_DATABASE + ".A SET aa = 'test1updated' WHERE pk = 1;" +
            "DELETE FROM " + TEST_DATABASE + ".A WHERE pk = 2;" +
            "INSERT INTO " + TEST_DATABASE + ".A VALUES(5, 'test1');" +
            "INSERT INTO " + TEST_DATABASE + ".A VALUES(6, 'test2');";
    execute(statements);
    Thread.sleep(Duration.ofSeconds(3).toMillis());
    // Check streaming metrics
    assertThat(mBeanServer.getAttribute(objectName, "Connected")).isEqualTo(true);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfUpdateEventsSeen")).isEqualTo(1L);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfDeleteEventsSeen")).isEqualTo(1L);
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfCreateEventsSeen")).isEqualTo(6L);//todo fix should be 2 PLAT-6970
    assertThat(mBeanServer.getAttribute(objectName, "TotalNumberOfEventsSeen")).isEqualTo(
        8L);//todo fix should be 4 PLAT-6970
  }
}
