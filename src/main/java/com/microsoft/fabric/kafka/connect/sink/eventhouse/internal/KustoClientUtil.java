package com.microsoft.fabric.kafka.connect.sink.eventhouse.internal;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.QueuedIngestClient;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.EventHouseSinkConfig;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.net.URISyntaxException;

public class KustoClientUtil {
  public static IngestClient createIngestClient(EventHouseSinkConfig eventHouseSinkConfig) throws URISyntaxException {
    return IngestClientFactory
        .createClient(getIngestKcsb(connectionOptions, sourceClass, "ingest"));
  }

  public static QueuedIngestClient createDMClient(KustoConnectionOptions connectionOptions,
      String sourceClass) throws URISyntaxException {
    return IngestClientFactory.createClient(getIngestKcsb(connectionOptions, sourceClass, "dm"));
  }

  public static IngestClient createMangedIngestClient(KustoConnectionOptions connectionOptions,
      String sourceClass) throws URISyntaxException {
    return IngestClientFactory.createManagedStreamingIngestClient(
        getIngestKcsb(connectionOptions, sourceClass, "ingest"));
  }

  @Contract("_, _ -> new")
  public static @NotNull Client createClient(KustoConnectionOptions connectionOptions,
      String sourceClass) throws URISyntaxException {
    return ClientFactory.createClient(getQueryKcsb(connectionOptions, sourceClass));
  }


  public static ConnectionStringBuilder getIngestKcsb(
      @NotNull EventHouseSinkConfig eventHouseSinkConfig, String clusterType) {
    ConnectionStringBuilder kcsb = eventHouseSinkConfig.i()
        ? "system".equalsIgnoreCase(connectionOptions.getManagedIdentityAppId())
            ? ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getIngestUrl())
            : ConnectionStringBuilder.createWithAadManagedIdentity(connectionOptions.getIngestUrl(),
                connectionOptions.getManagedIdentityAppId())
        : connectionOptions.isAzCliAuth()
            ? ConnectionStringBuilder.createWithAzureCli(connectionOptions.getIngestUrl())
            : ConnectionStringBuilder.createWithAadApplicationCredentials(
                connectionOptions.getIngestUrl(), connectionOptions.getAppId(),
                connectionOptions.getAppKey(), connectionOptions.getTenantId());
    Pair<String, String> sinkTag = ImmutablePair.of("sinkType", sourceClass);
    Pair<String, String> clusterTypeTag = ImmutablePair.of("clusterType", clusterType);
    setConnectorDetails(kcsb, sinkTag, clusterTypeTag);
    return kcsb;
  }

  @SafeVarargs
  static private void setConnectorDetails(@NotNull ConnectionStringBuilder kcsb,
      Pair<String, String>... additionalOptions) {
    kcsb.setConnectorDetails(Version.CLIENT_NAME, Version.getVersion(), Version.CLIENT_NAME,
        Version.getVersion(), false, null, additionalOptions);
  }

  private static ConnectionStringBuilder getQueryKcsb(
      @NotNull KustoConnectionOptions connectionOptions, String sourceClass) {
    ConnectionStringBuilder kcsb = connectionOptions.isManagedIdentity()
        ? "system".equalsIgnoreCase(connectionOptions.getManagedIdentityAppId())
            ? ConnectionStringBuilder
                .createWithAadManagedIdentity(connectionOptions.getClusterUrl())
            : ConnectionStringBuilder.createWithAadManagedIdentity(
                connectionOptions.getClusterUrl(), connectionOptions.getManagedIdentityAppId())
        : connectionOptions.isAzCliAuth()
            ? ConnectionStringBuilder.createWithAzureCli(connectionOptions.getClusterUrl())
            : ConnectionStringBuilder.createWithAadApplicationCredentials(
                connectionOptions.getClusterUrl(), connectionOptions.getAppId(),
                connectionOptions.getAppKey(), connectionOptions.getTenantId());
    Pair<String, String> sinkTag = ImmutablePair.of("sinkType", sourceClass);
    Pair<String, String> clusterTypeTag = ImmutablePair.of("clusterType", "queued");
    setConnectorDetails(kcsb, sinkTag, clusterTypeTag);
    return kcsb;
  }
}
