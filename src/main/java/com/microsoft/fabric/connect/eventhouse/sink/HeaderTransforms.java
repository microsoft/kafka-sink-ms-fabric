package com.microsoft.fabric.connect.eventhouse.sink;

import java.util.Set;

public class HeaderTransforms {

    private Set<String> headersToProject;
    private Set<String> headersToDrop;

    public Set<String> getHeadersToProject() {
        return headersToProject;
    }

    public void setHeadersToProject(Set<String> headersToProject) {
        this.headersToProject = headersToProject;
    }

    public Set<String> getHeadersToDrop() {
        return headersToDrop;
    }

    public void setHeadersToDrop(Set<String> headersToDrop) {
        this.headersToDrop = headersToDrop;
    }

    public HeaderTransforms(Set<String> headersToDrop, Set<String> headersToProject) {
        this.headersToProject = headersToProject;
        this.headersToDrop = headersToDrop;
    }

    public HeaderTransforms() {
        // No args constructor for serialization
    }

}
