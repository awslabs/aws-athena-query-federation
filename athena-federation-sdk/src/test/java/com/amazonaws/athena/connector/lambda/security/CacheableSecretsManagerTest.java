package com.amazonaws.athena.connector.lambda.security;

import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CacheableSecretsManagerTest
{
    private AWSSecretsManager mockSecretsManager;

    private CachableSecretsManager cachableSecretsManager;

    @Before
    public void setup()
    {
        mockSecretsManager = mock(AWSSecretsManager.class);
        cachableSecretsManager = new CachableSecretsManager(mockSecretsManager);
    }

    @After
    public void after()
    {
        reset(mockSecretsManager);
    }

    @Test
    public void expirationTest()
    {
        cachableSecretsManager.addCacheEntry("test", "value", System.currentTimeMillis());
        assertEquals("value", cachableSecretsManager.getSecret("test"));
        verifyNoMoreInteractions(mockSecretsManager);
        reset(mockSecretsManager);

        when(mockSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    GetSecretValueRequest request = invocation.getArgumentAt(0, GetSecretValueRequest.class);
                    if (request.getSecretId().equalsIgnoreCase("test")) {
                        return new GetSecretValueResult().withSecretString("value2");
                    }
                    throw new RuntimeException();
                });

        cachableSecretsManager.addCacheEntry("test", "value", 0);
        assertEquals("value2", cachableSecretsManager.getSecret("test"));
    }

    @Test
    public void evictionTest()
    {
        for (int i = 0; i < CachableSecretsManager.MAX_CACHE_SIZE; i++) {
            cachableSecretsManager.addCacheEntry("test" + i, "value" + i, System.currentTimeMillis());
        }
        when(mockSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    GetSecretValueRequest request = invocation.getArgumentAt(0, GetSecretValueRequest.class);
                    return new GetSecretValueResult().withSecretString(request.getSecretId() + "_value");
                });

        assertEquals("test_value", cachableSecretsManager.getSecret("test"));
        assertEquals("test0_value", cachableSecretsManager.getSecret("test0"));

        verify(mockSecretsManager, times(2)).getSecretValue(any(GetSecretValueRequest.class));
    }

    @Test
    public void resolveSecrets()
    {
        when(mockSecretsManager.getSecretValue(any(GetSecretValueRequest.class)))
                .thenAnswer((InvocationOnMock invocation) -> {
                    GetSecretValueRequest request = invocation.getArgumentAt(0, GetSecretValueRequest.class);
                    String result = request.getSecretId();
                    if (result.equalsIgnoreCase("unknown")) {
                        throw new RuntimeException("Unknown secret!");
                    }
                    return new GetSecretValueResult().withSecretString(result);
                });

        String oneSecret = "${OneSecret}";
        String oneExpected = "OneSecret";
        assertEquals(oneExpected, cachableSecretsManager.resolveSecrets(oneSecret));

        String twoSecrets = "ThisIsMyStringWith${TwoSecret}SuperSecret${Secrets}";
        String twoExpected = "ThisIsMyStringWithTwoSecretSuperSecretSecrets";
        assertEquals(twoExpected, cachableSecretsManager.resolveSecrets(twoSecrets));

        String noSecrets = "ThisIsMyStringWithTwoSecretSuperSecretSecrets";
        String noSecretsExpected = "ThisIsMyStringWithTwoSecretSuperSecretSecrets";
        assertEquals(noSecretsExpected, cachableSecretsManager.resolveSecrets(noSecrets));

        String commonErrors = "ThisIsM}yStringWi${thTwoSecretS{uperSecretSecrets";
        String commonErrorsExpected = "ThisIsM}yStringWi${thTwoSecretS{uperSecretSecrets";
        assertEquals(commonErrorsExpected, cachableSecretsManager.resolveSecrets(commonErrors));

        String unknownSecret = "This${Unknown}";
        try {
            cachableSecretsManager.resolveSecrets(unknownSecret);
            fail("Should not see this!");
        }
        catch (RuntimeException ex) {}
    }
}