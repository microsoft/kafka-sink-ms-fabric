package com.microsoft.fabric.kafka.connect.sink.it;

import org.apache.commons.lang3.StringUtils;

class ITCoordinates {
    final String accessToken;
    final String database;
    final String kqlDbConnectionString;
    final String esConnectionString;
    final String eventHub;
    String table;

    ITCoordinates(String accessToken, String database, String table,
            String kqlDbConnectionString, String esConnectionString, String eventHub) {

        this.accessToken = accessToken;
        this.database = database;
        this.table = table;
        this.esConnectionString = esConnectionString.replace("sb:/", "sb://");
        this.kqlDbConnectionString = kqlDbConnectionString.replace("Data Source=https:/", "Data Source=https://");
        this.eventHub = eventHub;
    }

    boolean isValidConfig() {
        return StringUtils.isNotEmpty(kqlDbConnectionString);
    }
}
