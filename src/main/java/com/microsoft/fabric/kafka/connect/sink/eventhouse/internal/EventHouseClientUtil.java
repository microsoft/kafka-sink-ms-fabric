package com.microsoft.fabric.kafka.connect.sink.eventhouse.internal;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.WorkloadIdentityCredential;
import com.azure.identity.WorkloadIdentityCredentialBuilder;
import com.microsoft.azure.kusto.data.HttpClientProperties;
import com.microsoft.azure.kusto.data.auth.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.fabric.kafka.connect.sink.eventhouse.EventHouseSinkConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpHost;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.util.Collections;

public class EventHouseClientUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(EventHouseClientUtil.class);
    private static IngestClient managedIngestionClient;
    private static IngestClient queuedIngestionClient;

    // make this a singleton and get IngestionClient or QueuedClient
    private EventHouseClientUtil() {
    }

    public static IngestClient createIngestClient(@NotNull EventHouseSinkConfig eventHouseSinkConfig,
                                                  boolean isManagedStreamingClient) throws URISyntaxException {
        if (isManagedStreamingClient) {
            if (managedIngestionClient == null) {
                HttpClientProperties httpClientProperties = null;
                if (StringUtils.isNotEmpty(eventHouseSinkConfig.getConnectionProxyHost()) && eventHouseSinkConfig.getConnectionProxyPort() > -1) {
                    httpClientProperties = HttpClientProperties.builder()
                            .proxy(new HttpHost(eventHouseSinkConfig.getConnectionProxyHost(), eventHouseSinkConfig.getConnectionProxyPort())).build();
                }
                ConnectionStringBuilder ingestConnectionStringBuilder = getIngestKcsb(eventHouseSinkConfig);
                ConnectionStringBuilder queryConnectionStringBuilder = getQueryKcsb(eventHouseSinkConfig);
                managedIngestionClient = httpClientProperties != null ?
                        IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, queryConnectionStringBuilder, httpClientProperties)
                        : IngestClientFactory.createManagedStreamingIngestClient(ingestConnectionStringBuilder, queryConnectionStringBuilder);

            }
            return managedIngestionClient;
        } else {
            if (queuedIngestionClient == null) {
                HttpClientProperties httpClientProperties = null;
                if (StringUtils.isNotEmpty(eventHouseSinkConfig.getConnectionProxyHost()) && eventHouseSinkConfig.getConnectionProxyPort() > -1) {
                    httpClientProperties = HttpClientProperties.builder()
                            .proxy(new HttpHost(eventHouseSinkConfig.getConnectionProxyHost(), eventHouseSinkConfig.getConnectionProxyPort())).build();
                }
                ConnectionStringBuilder queryConnectionStringBuilder = getQueryKcsb(eventHouseSinkConfig);
                queuedIngestionClient = httpClientProperties != null ?
                        IngestClientFactory.createClient(queryConnectionStringBuilder, httpClientProperties)
                        : IngestClientFactory.createClient(queryConnectionStringBuilder);

            }
            return queuedIngestionClient;
        }
    }


    private static @NotNull ConnectionStringBuilder getIngestKcsb(@NotNull EventHouseSinkConfig eventHouseSinkConfig) {
        String ingestConnectionString = eventHouseSinkConfig.getIngestUrl();
        return getKcsb(eventHouseSinkConfig, ingestConnectionString);
    }

    private static @NotNull ConnectionStringBuilder getQueryKcsb(@NotNull EventHouseSinkConfig eventHouseSinkConfig) {
        String connectionString = eventHouseSinkConfig.getEventhouseConnectionString();
        return getKcsb(eventHouseSinkConfig, connectionString);
    }

    @SafeVarargs
    static private void setConnectorDetails(@NotNull ConnectionStringBuilder kcsb,
                                            Pair<String, String>... additionalOptions) {
        kcsb.setConnectorDetails(Version.CLIENT_NAME, Version.getVersion(), Version.CLIENT_NAME,
                Version.getVersion(), false, null, additionalOptions);
    }

    private static ConnectionStringBuilder getKcsb(
            @NotNull EventHouseSinkConfig eventHouseSinkConfig, String clusterUrl) {
        final ConnectionStringBuilder kcsb;
        switch (eventHouseSinkConfig.getAuthStrategy()) {
            case APPLICATION:
                kcsb = new ConnectionStringBuilder(eventHouseSinkConfig.getEventhouseConnectionString());
                break;
            case MANAGED_IDENTITY:
                boolean isSystemIdentity = "system".equalsIgnoreCase(eventHouseSinkConfig.getManagedIdentityId());
                kcsb = isSystemIdentity ? ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl) :
                        ConnectionStringBuilder.createWithAadManagedIdentity(clusterUrl,
                                eventHouseSinkConfig.getManagedIdentityId());
                break;
            case WORKLOAD_IDENTITY:
                String clusterScope = String.format("%s/.default", new ConnectionStringBuilder(clusterUrl).getClusterUrl());
                kcsb = ConnectionStringBuilder.createWithAadTokenProviderAuthentication(
                        clusterUrl,
                        () -> {
                            WorkloadIdentityCredential wic = new WorkloadIdentityCredentialBuilder().build();
                            TokenRequestContext requestContext = new TokenRequestContext();
                            requestContext.setScopes(Collections.singletonList(clusterScope));
                            AccessToken accessToken = wic.getTokenSync(requestContext);
                            if (accessToken != null) {
                                LOGGER.debug("Returned access token that expires at {}", accessToken.getExpiresAt());
                                return accessToken.getToken();
                            } else {
                                LOGGER.error("Obtained empty token during token refresh. Context {}", clusterScope);
                                throw new ConnectException("Failed to retrieve WIF token");
                            }
                        });
                break;
            case AZ_DEV_TOKEN:
                LOGGER.warn("Using DEV-TEST mode, use this for development only. NOT recommended for production scenarios");
                kcsb = ConnectionStringBuilder.createWithAadAccessTokenAuthentication(
                        clusterUrl,
                        eventHouseSinkConfig.getAuthAccessToken());
                break;
            default:
                throw new ConfigException("Failed to initialize KustoIngestClient, please " +
                        "provide valid credentials. Either Kusto managed identity or " +
                        "Kusto appId, appKey, and authority should be configured.");
        }
        Pair<String, String> sinkTag = ImmutablePair.of("sinkType", "FabricConnector");
        setConnectorDetails(kcsb, sinkTag);
        return kcsb;
    }
}
