package com.amazonaws.athena.connector.lambda.security;

/*-
 * #%L
 * Amazon Athena Query Federation SDK
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

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Since Athena may call your connector or UDF at a high TPS or concurrency you may want to have a short lived
 * cache in front of SecretsManager to avoid bottlenecking on SecretsManager. This class offers such a cache. This class
 * also has utilities for idetifying and replacing secrets in scripts. For example: MyString${WithSecret} would have
 * ${WithSecret} replaced by the corresponding value of the secret in AWS Secrets Manager with that name.
 */
public class CachableSecretsManager
{
    private static final Logger logger = LoggerFactory.getLogger(CachableSecretsManager.class);

    private static final long MAX_CACHE_AGE_MS = 60_000;
    protected static final int MAX_CACHE_SIZE = 10;

    private static final String SECRET_PATTERN = "(\\$\\{[a-zA-Z0-9-\\/_\\-\\.\\+=@]+\\})";
    private static final String SECRET_NAME_PATTERN = "\\$\\{([a-zA-Z0-9-\\/_\\-\\.\\+=@]+)\\}";
    private static final Pattern PATTERN = Pattern.compile(SECRET_PATTERN);
    private static final Pattern NAME_PATTERN = Pattern.compile(SECRET_NAME_PATTERN);

    private final LinkedHashMap<String, CacheEntry> cache = new LinkedHashMap<>();
    private final AWSSecretsManager secretsManager;

    public CachableSecretsManager(AWSSecretsManager secretsManager)
    {
        this.secretsManager = secretsManager;
    }

    /**
     * Resolves any secrets found in the supplied string, for example: MyString${WithSecret} would have ${WithSecret}
     * repalced by the corresponding value of the secret in AWS Secrets Manager with that name. If no such secret is found
     * the function throws.
     *
     * @param rawString The string in which to find and replace/inject secrets.
     * @return The processed rawString that has had all secrets replaced with their secret value from SecretsManager.
     * Throws if any of the secrets can not be found.
     */
    public String resolveSecrets(String rawString)
    {
        if (rawString == null) {
            return rawString;
        }

        Matcher m = PATTERN.matcher(rawString);
        String result = rawString;
        while (m.find()) {
            String nextSecret = m.group(1);
            Matcher m1 = NAME_PATTERN.matcher(nextSecret);
            m1.find();
            result = result.replace(nextSecret, getSecret(m1.group(1)));
        }
        return result;
    }

    /**
     * Retrieves a secret from SecretsManager, first checking the cache. Newly fetched secrets are added to the cache.
     *
     * @param secretName The name of the secret to retrieve.
     * @return The value of the secret, throws if no such secret is found.
     */
    public String getSecret(String secretName)
    {
        CacheEntry cacheEntry = cache.get(secretName);

        if (cacheEntry == null || cacheEntry.getAge() > MAX_CACHE_AGE_MS) {
            logger.info("getSecret: Resolving secret[{}].", secretName);
            GetSecretValueResult secretValueResult = secretsManager.getSecretValue(new GetSecretValueRequest()
                    .withSecretId(secretName));
            cacheEntry = new CacheEntry(secretName, secretValueResult.getSecretString());
            evictCache(cache.size() >= MAX_CACHE_SIZE);
            cache.put(secretName, cacheEntry);
        }

        return cacheEntry.getValue();
    }

    private void evictCache(boolean force)
    {
        Iterator<Map.Entry<String, CacheEntry>> itr = cache.entrySet().iterator();
        int removed = 0;
        while (itr.hasNext()) {
            CacheEntry entry = itr.next().getValue();
            if (entry.getAge() > MAX_CACHE_AGE_MS) {
                itr.remove();
                removed++;
            }
        }

        if (removed == 0 && force) {
            //Remove the oldest since we found no expired entries
            itr = cache.entrySet().iterator();
            if (itr.hasNext()) {
                itr.next();
                itr.remove();
            }
        }
    }

    @VisibleForTesting
    protected void addCacheEntry(String name, String value, long createTime)
    {
        cache.put(name, new CacheEntry(name, value, createTime));
    }

    private class CacheEntry
    {
        private final String name;
        private final String value;
        private final long createTime;

        public CacheEntry(String name, String value)
        {
            this.value = value;
            this.name = name;
            this.createTime = System.currentTimeMillis();
        }

        public CacheEntry(String name, String value, long createTime)
        {
            this.value = value;
            this.name = name;
            this.createTime = createTime;
        }

        public String getValue()
        {
            return value;
        }

        public long getAge()
        {
            return System.currentTimeMillis() - createTime;
        }
    }
}
