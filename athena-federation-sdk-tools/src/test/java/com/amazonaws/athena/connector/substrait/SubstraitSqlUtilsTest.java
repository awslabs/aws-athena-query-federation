/*-
 * #%L
 * Amazon Athena Query Federation SDK Tools
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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
package com.amazonaws.athena.connector.substrait;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.junit.Assert;
import org.junit.Test;

/*
Projection happens at Athena end so here the sqlNode will be `SELECT *` instead of `SELECT field1, field2, ...`
 */
public class SubstraitSqlUtilsTest {
    @Test
    public void deserializeSubstraitPlanTestSnowflakeSqlLowercaseWithSchema()
    {
        String expectedSnowflakeSql1 =
            "SELECT *\n" +
            "FROM \"car_schema_lower\".\"carsid\"\n" +
            "LIMIT 10";
        SqlNode sqlNode = SubstraitSqlUtils.deserializeSubstraitPlan("GqoBEqcBCqQBGqEBCgIKABKWAQqTAQoCCgAScQoFY2FyaWQKBW1vZGVsCgR5ZWFyCgZzdGF0dXMKB21pbGVhZ2UKBXByaWNlCgRtc3ByCglwYXJ0aXRpb24SMgoEOgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQARgCOhoKEGNhcl9zY2hlbWFfbG93ZXIKBmNhcnNpZBgAIAo=", SnowflakeSqlDialect.DEFAULT);
        Assert.assertEquals(expectedSnowflakeSql1, sqlNode.toSqlString(SnowflakeSqlDialect.DEFAULT).getSql());
    }

    @Test
    public void deserializeSubstraitPlanTestSnowflakeSqlAdvanced() {
        String expectedSnowflakeSql1 =
            "SELECT *\n" +
            "FROM \"vehicles\"\n" +
            "WHERE \"PRICE\" < 12345678.12 AND \"ID\" < 30000 AND (\"MODEL_NAME\" = 'test' AND \"MILEAGE\" > 4.000123456789E3) AND (\"PURCHASE_DATE\" < DATE '2025-01-10' AND \"IS_AVAILABLE\" AND (\"EVENT_BYTE\" = X'04D2' AND \"EVENT_TIMESTAMP\" > TIMESTAMP '2025-01-10 10:10:10'))\n" +
            "ORDER BY \"ID\"\n" +
            "LIMIT 10";
        SqlNode sqlNode = SubstraitSqlUtils.deserializeSubstraitPlan("ChsIARIXL2Z1bmN0aW9uc19ib29sZWFuLnlhbWwKHAgDEhgvZnVuY3Rpb25zX2RhdGV0aW1lLnlhbWwKHggCEhovZnVuY3Rpb25zX2NvbXBhcmlzb24ueWFtbBIOGgwIARoIYW5kOmJvb2wSEhoQCAIQARoKbHQ6YW55X2FueRIVGhMIAhACGg1lcXVhbDphbnlfYW55EhIaEAgCEAMaCmd0OmFueV9hbnkSFBoSCAMQBBoMbHQ6ZGF0ZV9kYXRlEhIaEAgDEAUaCmd0OnB0c19wdHMa1QcS0gcK6AYa5QYKAgoAEtoGKtcGCgIKABLCBjq/BgoNEgsKCQkKCwwNDg8QERLDBRLABQoCCgASyAEKxQEKAgoAErIBCgJJRAoKTU9ERUxfTkFNRQoFUFJJQ0UKB01JTEVBR0UKDVBVUkNIQVNFX0RBVEUKDElTX0FWQUlMQUJMRQoLREVTQ1JJUFRJT04KD0VWRU5UX1RJTUVTVEFNUAoKRVZFTlRfQllURRJJCgQ6AhABCgiyAQUI6AcYAQoJwgEGCAIQCiABCgRaAhABCgWCAQIQAQoECgIQAQoIsgEFCOgHGAEKB4oCBAgDGAEKBGoCEAEYAToKCgh2ZWhpY2xlcxruAxrrAxoECgIQASI5GjcaNQgBGgQKAhABIgwaChIICgQSAggCIgAiHRobChnCARYKEIQClkkAAAAAAAAAAAAAAAAQChgCIi4aLBoqCAEaBAoCEAEiChoIEgYKAhIAIgAiFBoSWhAKBDoCEAESBgoEKLDqARgCIiwaKhooCAIaBAoCEAEiDBoKEggKBBICCAEiACIQGg4KDLIBCQoEdGVzdBDoByJFGkMaQQgDGgQKAhABIgwaChIICgQSAggDIgAiKRonWiUKBFoCEAESGwoZwgEWChAVDfBZowMAAAAAAAAAAAAAEA0YCRgCIjoaOBo2CAQaBAoCEAEiDBoKEggKBBICCAQiACIeGhxaGgoFggECEAISDwoNqgEKMjAyNS0wMS0xMBgCIiIaIBoeCAIaBAoCEAEiDBoKEggKBBICCAUiACIGGgQKAggBIkUaQxpBCAUaBAoCEAEiDBoKEggKBBICCAciACIpGidaJQoHigIECAMYAhIYChaqARMyMDI1LTAxLTEwIDEwOjEwOjEwGAIiJBoiGiAIAhoECgIQASIMGgoSCAoEEgIICCIAIggaBgoEagIE0iI2GjQaMggFGgQKAhABIgwaChIICgQSAggHIgAiGhoYWhYKBYoCAhgCEgsKCXCAifzl9OqKAxgCGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABoKEggKBBICCAMiABoKEggKBBICCAQiABoKEggKBBICCAUiABoKEggKBBICCAYiABoKEggKBBICCAciABoKEggKBBICCAgiABoMCggSBgoCEgAiABACGAAgChICSUQSCk1PREVMX05BTUUSBVBSSUNFEgdNSUxFQUdFEg1QVVJDSEFTRV9EQVRFEgxJU19BVkFJTEFCTEUSC0RFU0NSSVBUSU9OEg9FVkVOVF9USU1FU1RBTVASCkVWRU5UX0JZVEU=", SnowflakeSqlDialect.DEFAULT);
        Assert.assertEquals(expectedSnowflakeSql1, sqlNode.toSqlString(SnowflakeSqlDialect.DEFAULT).getSql());
    }

    @Test
    public void deserializeSubstraitPlanTestSnowflakeSqlBasicLowercase() {
        String expectedSnowflakeSql1 =
            "SELECT *\n" +
            "FROM \"carsid\"";
        SqlNode sqlNode = SubstraitSqlUtils.deserializeSubstraitPlan("GpABEo0BCnc6dQoHEgUKAwMEBRJICkYKAgoAEjYKBWNhcmlkCgVtb2RlbAoEeWVhchIgCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBGAE6CAoGY2Fyc2lkGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABIFY2FyaWQSBW1vZGVsEgR5ZWFy", SnowflakeSqlDialect.DEFAULT);
        Assert.assertEquals(expectedSnowflakeSql1, sqlNode.toSqlString(SnowflakeSqlDialect.DEFAULT).getSql());
    }

    @Test
    public void deserializeSubstraitPlanTestSnowflakeSqlBasicUppercase() {
        String expectedSnowflakeSql1 =
            "SELECT *\n" +
            "FROM \"CARSID\"";
        SqlNode sqlNode = SubstraitSqlUtils.deserializeSubstraitPlan("GpABEo0BCnc6dQoHEgUKAwMEBRJICkYKAgoAEjYKBWNhcmlkCgVtb2RlbAoEeWVhchIgCgiyAQUI6AcYAQoIsgEFCOgHGAEKCLIBBQjoBxgBGAE6CAoGQ0FSU0lEGggSBgoCEgAiABoKEggKBBICCAEiABoKEggKBBICCAIiABIFQ0FSSUQSBU1PREVMEgRZRUFS", SnowflakeSqlDialect.DEFAULT);
        Assert.assertEquals(expectedSnowflakeSql1, sqlNode.toSqlString(SnowflakeSqlDialect.DEFAULT).getSql());
    }

    @Test
    public void test() {
        Schema schema = SubstraitSqlUtils.getTableSchemaFromSubstraitPlan("GtsFEtgFCtUFGtIFCgIKABLHBSrEBQoCCgASrwUKrAUKAgoAEosFChFjY19jYWxsX2NlbnRlcl9zawoRY2NfY2FsbF9jZW50ZXJfaWQKEWNjX3JlY19zdGFydF9kYXRlCg9jY19yZWNfZW5kX2RhdGUKEWNjX2Nsb3NlZF9kYXRlX3NrCg9jY19vcGVuX2RhdGVfc2sKB2NjX25hbWUKCGNjX2NsYXNzCgxjY19lbXBsb3llZXMKCGNjX3NxX2Z0CghjY19ob3VycwoKY2NfbWFuYWdlcgoJY2NfbWt0X2lkCgxjY19ta3RfY2xhc3MKC2NjX21rdF9kZXNjChFjY19tYXJrZXRfbWFuYWdlcgoLY2NfZGl2aXNpb24KEGNjX2RpdmlzaW9uX25hbWUKCmNjX2NvbXBhbnkKD2NjX2NvbXBhbnlfbmFtZQoQY2Nfc3RyZWV0X251bWJlcgoOY2Nfc3RyZWV0X25hbWUKDmNjX3N0cmVldF90eXBlCg9jY19zdWl0ZV9udW1iZXIKB2NjX2NpdHkKCWNjX2NvdW50eQoIY2Nfc3RhdGUKBmNjX3ppcAoKY2NfY291bnRyeQoNY2NfZ210X29mZnNldAoRY2NfdGF4X3BlcmNlbnRhZ2UKCXBhcnRpdGlvbhLCAQoEOgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEWgIQAQoEOgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEOgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEOgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQAQoEYgIQARgCOhgKEGNhcl9zY2hlbWFfbG93ZXIKBHRlc3QaDAoIEgYKAhIAIgAQBBgAIAM=", SnowflakeSqlDialect.DEFAULT);
        System.out.println(schema.toJson());
    }
}
