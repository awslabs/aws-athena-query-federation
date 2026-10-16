/*-
 * #%L
 * athena-cloudwatch
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.cloudwatch;

import java.util.Map;

/**
 * Reversibly maps CloudWatch log group / log stream names to identifiers that are accepted as
 * federated Glue Data Catalog database and table names when this connector is used as a Glue managed
 * connector governed by Lake Formation.
 * <p>
 * The catalog federation layer rejects identifiers that contain '/' or ':' or any uppercase letter, so
 * CloudWatch names such as {@code /aws/lambda/MyFunction} cannot be surfaced verbatim. When encoding is
 * enabled this class rewrites only such names, on the way out to the catalog, into a legal form, and
 * decodes them back on the way in before the name is resolved against CloudWatch. Names that are already
 * legal (and do not collide with the reserved prefix) are returned unchanged, so existing behavior and
 * existing grants for legal-named log groups are preserved exactly.
 * <p>
 * Encoding is disabled by default and is controlled by the {@link #ENCODING_ENABLED_CONFIG_KEY}
 * configuration option, which is set only on the managed connector deployment. When disabled every method
 * is an identity function, so classic (non-managed) usage is unaffected.
 */
public class CloudwatchFederatedNameEncoder
{
    /**
     * Config option (Lambda environment variable) that turns encoding on. Absent or any value other than
     * "true" (case-insensitive) leaves encoding off.
     */
    public static final String ENCODING_ENABLED_CONFIG_KEY = "managed_connector_name_encoding";

    // Marks an encoded identifier. Chosen to be legal and to not occur in practical CloudWatch names; a
    // legal name that nonetheless starts with it is still encoded so decoding stays unambiguous.
    private static final String ENCODED_PREFIX = "__cwenc__";

    private final boolean enabled;

    public CloudwatchFederatedNameEncoder(Map<String, String> configOptions)
    {
        this.enabled = configOptions != null
                && Boolean.parseBoolean(configOptions.getOrDefault(ENCODING_ENABLED_CONFIG_KEY, "false"));
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    /**
     * Encodes a single name for presentation to the catalog. Returns the input unchanged when encoding is
     * disabled, or when the name is already catalog-legal and does not start with the reserved prefix.
     */
    public String encode(String name)
    {
        if (!enabled || name == null) {
            return name;
        }
        if (isCatalogLegal(name) && !name.startsWith(ENCODED_PREFIX)) {
            return name;
        }
        StringBuilder sb = new StringBuilder(ENCODED_PREFIX);
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '_') {
                sb.append("__");
            }
            else if (isPassthrough(c)) {
                sb.append(c);
            }
            else {
                sb.append("_x").append(String.format("%02x", (int) c));
            }
        }
        return sb.toString();
    }

    /**
     * Reverses {@link #encode(String)}. Returns the input unchanged when encoding is disabled or when the
     * name is not a previously encoded identifier.
     */
    public String decode(String name)
    {
        if (!enabled || name == null || !name.startsWith(ENCODED_PREFIX)) {
            return name;
        }
        String body = name.substring(ENCODED_PREFIX.length());
        StringBuilder sb = new StringBuilder(body.length());
        int i = 0;
        while (i < body.length()) {
            char c = body.charAt(i);
            if (c == '_') {
                char marker = body.charAt(i + 1);
                if (marker == '_') {
                    sb.append('_');
                    i += 2;
                }
                else {
                    sb.append((char) Integer.parseInt(body.substring(i + 2, i + 4), 16));
                    i += 4;
                }
            }
            else {
                sb.append(c);
                i += 1;
            }
        }
        return sb.toString();
    }

    // A name is catalog-legal when it contains no character the federation layer rejects: '/' , ':' , or
    // any uppercase letter.
    private static boolean isCatalogLegal(String name)
    {
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (c == '/' || c == ':' || (Character.isLetter(c) && Character.isUpperCase(c))) {
                return false;
            }
        }
        return true;
    }

    // Characters copied verbatim into an encoded body: catalog-legal and not the '_' escape marker.
    private static boolean isPassthrough(char c)
    {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '#';
    }
}
