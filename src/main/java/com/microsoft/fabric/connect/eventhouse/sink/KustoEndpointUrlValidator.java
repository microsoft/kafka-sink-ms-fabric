package com.microsoft.fabric.connect.eventhouse.sink;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.kafka.common.config.ConfigException;

import com.microsoft.azure.kusto.data.StringUtils;
import com.microsoft.azure.kusto.data.auth.endpoints.KustoTrustedEndpoints;
import com.microsoft.azure.kusto.data.auth.endpoints.WellKnownKustoEndpointsData;
import com.microsoft.azure.kusto.data.exceptions.KustoClientInvalidConnectionStringException;

/**
 * Validates that Eventhouse / Kusto endpoint URLs point to legitimate Azure Data Explorer or Fabric domains.
 * This prevents SSRF attacks where attacker-controlled URLs could be used to exfiltrate
 * Entra ID authentication tokens.
 *
 * <p>Domain matching is delegated to the azure-kusto-java SDK's {@link KustoTrustedEndpoints}, which uses the
 * canonical {@code WellKnownKustoEndpoints.json} as the source of truth for all trusted endpoints
 * (including {@code *.kusto.fabric.microsoft.com}) across all Azure clouds and sovereign regions.
 * Ported from Azure/kafka-sink-azure-kusto (MSRC 110999).
 *
 * <p>If the URL is provided without a scheme, {@code https://} is prepended automatically.
 */
public final class KustoEndpointUrlValidator {
    private static final String HTTPS_SCHEME_PREFIX = "https://";
    // Matches dotted numeric hosts such as 127.0.0.1 or 10.1.2.3 (IPv4 literals).
    private static final Pattern NUMERIC_HOST = Pattern.compile("^[0-9.]+$");

    private KustoEndpointUrlValidator() {
        // Utility class
    }

    /**
     * Trims the URL and prepends {@code https://} when no scheme is given, so that scheme-less values accepted by
     * validation are also usable by the Kusto SDK (which requires a URI authority). Blank values are returned as-is.
     */
    public static String normalizeUrl(String url) {
        if (StringUtils.isBlank(url)) {
            return url;
        }
        String trimmed = url.trim();
        return trimmed.contains("://") ? trimmed : HTTPS_SCHEME_PREFIX + trimmed;
    }

    /**
     * Validates that a URL points to a legitimate Azure Data Explorer / Fabric Eventhouse endpoint.
     *
     * @param url       the URL string to validate
     * @param configKey the configuration key name (used in error messages)
     * @throws ConfigException if the URL does not match any known trusted Kusto endpoint
     */
    public static void validateEndpointUrl(String url, String configKey) {
        if (StringUtils.isBlank(url)) {
            return;
        }

        if (url.trim().regionMatches(true, 0, "http://", 0, 7)) {
            throw new ConfigException(configKey, url,
                    "HTTP is not supported. Only HTTPS endpoints are allowed.");
        }

        url = normalizeUrl(url);

        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new ConfigException(configKey, url,
                    "Invalid URL format: " + e.getMessage());
        }

        String host = uri.getHost();
        if (host == null || isLocalOrIpLiteral(host)) {
            throw new ConfigException(configKey, url,
                    "URL does not point to a known Azure Data Explorer endpoint.");
        }

        WellKnownKustoEndpointsData endpointsData = WellKnownKustoEndpointsData.getInstance();
        for (String loginEndpoint : endpointsData.AllowedEndpointsByLogin.keySet()) {
            try {
                KustoTrustedEndpoints.validateTrustedEndpoint(uri, loginEndpoint);
                return;
            } catch (KustoClientInvalidConnectionStringException e) {
                // Not trusted for this login endpoint, try next cloud
            }
        }

        throw new ConfigException(configKey, url,
                "URL does not point to a known Azure Data Explorer endpoint. "
                        + "The hostname must be a well-known trusted Kusto endpoint "
                        + "(see WellKnownKustoEndpoints.json in azure-kusto-java SDK).");
    }

    /**
     * The SDK treats local hosts (localhost, ::1 and the whole 127.* range) as trusted so that local emulators work.
     * A sink connector must never send tokens there, and real Eventhouse endpoints are never raw IP addresses,
     * so localhost and every IP literal (IPv4 or IPv6) are rejected.
     */
    private static boolean isLocalOrIpLiteral(String host) {
        String h = host.toLowerCase(Locale.ROOT);
        if (h.endsWith(".")) {
            h = h.substring(0, h.length() - 1);
        }
        return h.equals("localhost")
                || h.startsWith("127.")
                || h.startsWith("[")
                || h.contains(":")
                || NUMERIC_HOST.matcher(h).matches();
    }
}
