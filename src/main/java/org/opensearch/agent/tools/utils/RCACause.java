/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.agent.tools.utils;

import lombok.Getter;

@Getter
public class RCACause {
    public RCACause() {}

    public String reason;
    public String apiUrl;
    public String expectedResponse;
    public String response;
}
