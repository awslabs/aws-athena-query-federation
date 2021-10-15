const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const AWS = require('aws-sdk');
const { cwd } = require('process');

AWS.config.update({
    region: 'us-west-2'
})

var glue = new AWS.Glue();
var dbidentifier = uuidv4();
var databaseName = `graph-database-${dbidentifier}`;

function addDatabase(addTablesAfterDatabase) {
    var params = {
        DatabaseInput: { /* required */
            Name: databaseName, /* required */
            Description: 'glue database to store external tables for neptune database',
        }
    };

    glue.createDatabase(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else {
            console.log(data);           // successful response
            addTablesAfterDatabase();
        }
    });
}

function addTable(tableType, tableName, tableColumns) {
    var params = {
        DatabaseName: databaseName, /* required */
        TableInput: { /* required */
            Name: tableName, /* required */
            Parameters: {
                "separatorChar": ",",
                "componenttype": tableType
            },
            StorageDescriptor: {
                Columns: [],
                Location: 's3://dummy-bucket/'
            }
        }
    };

    if (tableType === 'vertex') {
        params.TableInput.StorageDescriptor.Columns.push(
            {
                Name: 'id', /* required */
                Type: 'string'
            });
    } else {
        params.TableInput.StorageDescriptor.Columns.push(
            {
                Name: 'id', /* required */
                Type: 'string'
            });

        params.TableInput.StorageDescriptor.Columns.push(
            {
                Name: 'in', /* required */
                Type: 'string'
            });

        params.TableInput.StorageDescriptor.Columns.push(
            {
                Name: 'out', /* required */
                Type: 'string'
            });
    }

    tableColumns.forEach(column => {
        switch (column.dataType) {
            case "String":
                params.TableInput.StorageDescriptor.Columns.push(
                    {
                        Name: column.property, /* required */
                        Type: 'string'
                    });
                break;

            case "Double":
                params.TableInput.StorageDescriptor.Columns.push(
                    {
                        Name: column.property, /* required */
                        Type: 'double'
                    });
                break;

            case "Integer":
                params.TableInput.StorageDescriptor.Columns.push(
                    {
                        Name: column.property, /* required */
                        Type: 'int'
                    });
                break;
        }
    });

    glue.createTable(params, function (err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data);           // successful response
    });
}

//addDatabase();
function addTables(data) {
    data.nodes.forEach(node => {
        addTable('vertex', node.label, node.properties);
    });

    data.edges.forEach(edge => {
        addTable('edge', edge.label, edge.properties);
    });
}

fs.readFile(`${cwd()}/config.json`, 'utf8', (err, data) => {
    if (err) {
        console.error(err)
        return
    }

    addDatabase(function () {
        addTables(JSON.parse(data));
    });
})