/*-
 * #%L
 * athena-jdbc
 * %%
 * Copyright (C) 2019 - 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.jdbc.credentials;

import org.apache.commons.lang3.Validate;
import software.amazon.awssdk.regions.Region;

import java.util.Objects;

/**
 * Connection details required to generate an RDS IAM database authentication token.
 */
public class RdsIamAuthConfiguration
{
    private final String hostname;
    private final int port;
    private final String username;
    private final Region region;

    public RdsIamAuthConfiguration(
            final String hostname,
            final int port,
            final String username,
            final Region region)
    {
        this.hostname = Validate.notBlank(hostname, "hostname must not be blank");
        Validate.isTrue(port > 0, "port must be positive");
        this.port = port;
        this.username = Validate.notBlank(username, "username must not be blank");
        this.region = Validate.notNull(region, "region must not be null");
    }

    public String getHostname()
    {
        return hostname;
    }

    public int getPort()
    {
        return port;
    }

    public String getUsername()
    {
        return username;
    }

    public Region getRegion()
    {
        return region;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RdsIamAuthConfiguration that = (RdsIamAuthConfiguration) o;
        return port == that.port
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(username, that.username)
                && Objects.equals(region, that.region);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hostname, port, username, region);
    }
}
