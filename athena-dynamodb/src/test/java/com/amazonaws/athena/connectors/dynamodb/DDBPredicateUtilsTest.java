package com.amazonaws.athena.connectors.dynamodb;

import com.amazonaws.athena.connectors.dynamodb.util.DDBPredicateUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Tests DynamoDB utility methods relating to predicate handling.
 */
@RunWith(MockitoJUnitRunner.class)
public class DDBPredicateUtilsTest
{
    private static final Logger logger = LoggerFactory.getLogger(DDBPredicateUtilsTest.class);

    @Test
    public void aliasColumnTest()
    {
        logger.info("aliasColumnTest - enter");

        assertEquals("Unexpected alias column value!", "#column_1",
                DDBPredicateUtils.aliasColumn("column_1"));
        assertEquals("Unexpected alias column value!", "#column__1",
                DDBPredicateUtils.aliasColumn("column__1"));
        assertEquals("Unexpected alias column value!", "#column_1",
                DDBPredicateUtils.aliasColumn("column-1"));
        assertEquals("Unexpected alias column value!", "#column_1_f3F",
                DDBPredicateUtils.aliasColumn("column-$1`~!@#$%^&*()-=+[]{}\\|;:'\",.<>/?f3F"));

        logger.info("aliasColumnTest - exit");
    }
}
