package com.amazonaws.athena.connectors.android;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;

public class AndroidDeviceTable
{
    private final TableName tableName;
    private final Schema schema;

    public AndroidDeviceTable()
    {
        //Table name must match the firebase push subscription topic used on the devices
        this.tableName = new TableName("android", "live_query");
        schema = new SchemaBuilder().newBuilder()
                .addStringField("device_id")
                .addStringField("name")
                .addStringField("echo_value")
                .addStringField("result_field")
                .addField("last_updated", Types.MinorType.DATEMILLI.getType())
                .addIntField("score")
                .addBigIntField("query_timeout")
                .addBigIntField("query_min_results")
                .addMetadata("device_id", "Android device id of the responding device.")
                .addMetadata("name", "Name of the simulated device owner.")
                .addMetadata("last_updated", "Last time this data was fetched")
                .addMetadata("echo_value", "The value requested by the search.")
                .addMetadata("result_field", "Flattened copy of the first value from the values field.")
                .addMetadata("score", "Randomly generated score")
                .addMetadata("query_timeout", "used to configure the number of milli-seconds the query waits for the min_results")
                .addMetadata("query_min_results", "The min number of results to wait for.")
                .build();
    }

    public TableName getTableName()
    {
        return tableName;
    }

    public Schema getSchema()
    {
        return schema;
    }

    public String getQueryMinResultsField()
    {
        return "query_min_results";
    }

    public String getQueryTimeout()
    {
        return "query_timeout";
    }

    public String getDeviceIdField()
    {
        return "device_id";
    }

    public String getLastUpdatedField()
    {
        return "last_updated";
    }

    public String getNameField()
    {
        return "name";
    }

    public String getEchoValueField()
    {
        return "echo_value";
    }

    public String getResultField()
    {
        return "result_field";
    }

    public String getScoreField()
    {
        return "score";
    }

    public FieldVector getQueryMinResultsField(Block block)
    {
        return block.getFieldVector("query_min_results");
    }

    public FieldVector getQueryTimeout(Block block)
    {
        return block.getFieldVector("query_timeout");
    }

    public FieldVector getDeviceIdField(Block block)
    {
        return block.getFieldVector("device_id");
    }

    public FieldVector getNameField(Block block)
    {
        return block.getFieldVector("name");
    }

    public FieldVector getLastUpdatedField(Block block)
    {
        return block.getFieldVector("last_updated");
    }

    public FieldVector getEchoValueField(Block block)
    {
        return block.getFieldVector("echo_value");
    }

    public FieldVector getResultField(Block block)
    {
        return block.getFieldVector("result_field");
    }

    public FieldVector getScoreField(Block block)
    {
        return block.getFieldVector("score");
    }
}
