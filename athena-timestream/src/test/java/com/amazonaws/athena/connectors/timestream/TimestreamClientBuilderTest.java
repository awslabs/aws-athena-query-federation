package com.amazonaws.athena.connectors.timestream;

import com.amazonaws.ClientConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimestreamClientBuilderTest {

  @Test
  public void testUserAgentField()
  {
    ClientConfiguration clientConfiguration = TimestreamClientBuilder.buildClientConfiguration("timestream");
    assertEquals("aws-athena-timestream-connector", clientConfiguration.getUserAgentPrefix());
  }
}
