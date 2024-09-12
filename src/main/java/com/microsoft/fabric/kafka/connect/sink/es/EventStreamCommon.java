package com.microsoft.fabric.kafka.connect.sink.es;

import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class EventStreamCommon {
    // singleton instance
    private EventStreamCommon() {
    }

    /**
     * Get the secure connection string
     *
     * @param connectionString the connection string
     * @return the secure connection string
     */
    public static @NotNull String getSecureConnectionString(String connectionString) {
        if (StringUtils.isNotEmpty(connectionString)) {
            Map<String, String> connectionParts = Arrays.stream(connectionString.split(";"))
                    .map(entry -> entry.split("\\s*:\\s*"))
                    .collect(Collectors.toMap(
                            pair -> pair[0],
                            pair -> pair[1]));
            return String.format("Endpoint=%s and eventhub %s",
                    connectionParts.get("Endpoint"), connectionParts.get("EntityPath"));
        }
        return connectionString;
    }
}
