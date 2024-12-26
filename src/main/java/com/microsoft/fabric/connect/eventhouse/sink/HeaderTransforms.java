package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.List;

public class HeaderTransforms {
    public List<String> getHeadersToFilter() {
        return headersToFilter;
    }

    public void setHeadersToFilter(List<String> headersToFilter) {
        this.headersToFilter = headersToFilter;
    }

    public boolean isEncodeHeadersToB64() {
        return encodeHeadersToB64;
    }

    public void setEncodeHeadersToB64(boolean encodeHeadersToB64) {
        this.encodeHeadersToB64 = encodeHeadersToB64;
    }

    private List<String> headersToFilter;

    public HeaderTransforms(List<String> headersToFilter, boolean encodeHeadersToB64) {
        this.headersToFilter = headersToFilter;
        this.encodeHeadersToB64 = encodeHeadersToB64;
    }

    private boolean encodeHeadersToB64;
}
