package com.singlestore.debezium;

import static org.assertj.core.api.Assertions.assertThat;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.pipeline.notification.Notification;
import io.debezium.pipeline.notification.channels.SinkNotificationChannel;
import io.debezium.pipeline.notification.channels.jmx.JmxNotificationChannelMXBean;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.JMX;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.assertj.core.api.Assertions;
import org.assertj.core.data.Percentage;
import org.awaitility.Awaitility;
import org.junit.Test;

public class NotificationsIT extends SnapshotIT {

  @Test
  public void notificationCorrectlySentOnItsTopic() {
    final Configuration config = defaultJdbcConfigBuilder().withDefault(
            SingleStoreConnectorConfig.DATABASE_NAME, TEST_DATABASE)
        .withDefault(SingleStoreConnectorConfig.TABLE_NAME, "A")
        .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification")
        .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "sink").build();
    start(SingleStoreConnector.class, config);
    assertConnectorIsRunning();
    List<SourceRecord> notifications = new ArrayList<>();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(() -> {
      consumeAvailableRecords(r -> {
        if (r.topic().equals("io.debezium.notification")) {
          notifications.add(r);
        }
      });
      return notifications.size() == 2;
    });
    assertThat(notifications).hasSize(2);
    SourceRecord sourceRecord = notifications.get(0);
    Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
    Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type"))
        .isEqualTo("Initial Snapshot");
    Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("STARTED");
    Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp"))
        .isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
    sourceRecord = notifications.get(notifications.size() - 1);
    Assertions.assertThat(sourceRecord.topic()).isEqualTo("io.debezium.notification");
    Assertions.assertThat(((Struct) sourceRecord.value()).getString("aggregate_type"))
        .isEqualTo("Initial Snapshot");
    Assertions.assertThat(((Struct) sourceRecord.value()).getString("type")).isEqualTo("COMPLETED");
    Assertions.assertThat(((Struct) sourceRecord.value()).getInt64("timestamp"))
        .isCloseTo(Instant.now().toEpochMilli(), Percentage.withPercentage(1));
  }

  @Test
  public void notificationNotSentIfNoChannelIsConfigured() {
    final Configuration config = defaultJdbcConfigBuilder().withDefault(
            SingleStoreConnectorConfig.DATABASE_NAME, TEST_DATABASE)
        .withDefault(SingleStoreConnectorConfig.TABLE_NAME, "A")
        .with(SinkNotificationChannel.NOTIFICATION_TOPIC, "io.debezium.notification").build();
    start(SingleStoreConnector.class, config);
    assertConnectorIsRunning();
    waitForAvailableRecords(1000, TimeUnit.MILLISECONDS);
    List<SourceRecord> notifications = consumedLines.stream()
        .filter(r -> r.topic().equals("io.debezium.notification")).collect(Collectors.toList());
    assertThat(notifications).isEmpty();
  }

  @Test
  public void notificationCorrectlySentOnJmx()
      throws ReflectionException, MalformedObjectNameException, InstanceNotFoundException, IntrospectionException, AttributeNotFoundException, MBeanException {

    final Configuration config = defaultJdbcConfigBuilder().withDefault(
            SingleStoreConnectorConfig.DATABASE_NAME, TEST_DATABASE)
        .withDefault(SingleStoreConnectorConfig.TABLE_NAME, "A")
        .with(CommonConnectorConfig.NOTIFICATION_ENABLED_CHANNELS, "jmx").build();

    start(SingleStoreConnector.class, config);
    assertConnectorIsRunning();

    Awaitility.await().atMost(60, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
        .pollInterval(1, TimeUnit.SECONDS).until(() -> !readNotificationFromJmx().isEmpty());

    final List<Notification> notifications = readNotificationFromJmx();
    assertThat(notifications).hasSize(2);
    assertThat(notifications.get(0)).hasFieldOrPropertyWithValue("aggregateType",
            "Initial Snapshot").hasFieldOrPropertyWithValue("type", "STARTED")
        .hasFieldOrProperty("timestamp");
    assertThat(notifications.get(notifications.size() - 1)).hasFieldOrPropertyWithValue(
            "aggregateType", "Initial Snapshot").hasFieldOrPropertyWithValue("type", "COMPLETED")
        .hasFieldOrProperty("timestamp");
    List<Notification> notificationsAfterReset = readNotificationFromJmx();
    assertThat(notificationsAfterReset).hasSize(2);
  }

  private List<Notification> readNotificationFromJmx()
      throws MalformedObjectNameException, ReflectionException, InstanceNotFoundException, IntrospectionException {
    ObjectName notificationBean = getObjectName();
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    MBeanInfo mBeanInfo = server.getMBeanInfo(notificationBean);
    List<String> attributesNames = Arrays.stream(mBeanInfo.getAttributes())
        .map(MBeanAttributeInfo::getName).collect(Collectors.toList());
    assertThat(attributesNames).contains("Notifications");
    JmxNotificationChannelMXBean proxy = JMX.newMXBeanProxy(server, notificationBean,
        JmxNotificationChannelMXBean.class);

    return proxy.getNotifications();
  }

  private ObjectName getObjectName() throws MalformedObjectNameException {
    return new ObjectName(
        String.format("debezium.%s:type=management,context=notifications,server=%s",
            "singlestore", "singlestore_topic"));
  }
}
