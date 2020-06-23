const configHandler = require("./ConfigHandler");
const arrayChunk = require("array-chunk");
const LEVEL_NOT_FOUND_VALUE = -1;
const LEVEL_NOT_FOUND_MESSAGE = "Invalid confidentiality level";
const PRINCIPAL_CONFIDENTIALITY_NOT_FOUND_MESSAGE = "Confidentiality level for principal not found.";
const CONFIDENTIALITY_LEVEL_PARAM_KEY = "confidentiality_level";
const TABLE_NOT_FOUND_MESSAGE = "Table not found";
const NOTHING_TO_SYNC_MESSAGE = "Nothing to sync";
const GLUE_API_PAGINATION = 1000;
const MAX_GRANT_PER_REQUEST = 20;
const { v4: uuid4 } = require('uuid');

module.exports = {
    setConfidentialityLevel: async function(ddbClient, principalArn, level) {
        if (!await this.isConfidentialityLevelValid(level)) {
            throw LEVEL_NOT_FOUND_MESSAGE;
        }

        var config = await configHandler.loadConfigFile();

        var params = {
            Item: {
                "principal_arn": {
                    "S": principalArn
                },
                "confidentiality_level": {
                    "S": level
                }
            },
            TableName: config.ddb_table_name
        }

        await ddbClient.putItem(params).promise();
    },
    syncWithLakeFormation: async function(ddbClient, lfClient, glueClient, principalArn, dbName, tableName) {
        var level = await this.getConfidentialLevelOfPrincipal(ddbClient, principalArn);
        var levels = await this.getConfidentialityLevels();
        var principalLevelValue = levels[level];

        var tables = [];

        if (tableName != null && tableName.length > 0) {
            try {
                var tablePayload = await glueClient.getTable({"DatabaseName": dbName, "Name": tableName}).promise();
                tables.push(tablePayload.Table);
            } catch (e) {
                throw TABLE_NOT_FOUND_MESSAGE;
            }
        } else {
            tables = await this.getAllTables(glueClient, dbName);
        }
        
        if (tables.length > 0) {
            var batchGrant = [];
            var batchRevoke = [];

            for (var i=0;i<tables.length>0;i++) {
                var table = tables[i];

                var permissionDiff = await this.createPermissionDiff(lfClient, principalArn, level, levels, table);

                if (permissionDiff.grant != null) {
                    batchGrant.push(permissionDiff.grant);
                }

                if (permissionDiff.revoke != null) {
                    batchRevoke.push(permissionDiff.revoke);
                }
            }

            if (batchGrant.length > 0) {
                var chunkBatchGrant = arrayChunk(batchGrant, MAX_GRANT_PER_REQUEST);

                for (var i=0;i<chunkBatchGrant.length;i++) {
                    var chunk = chunkBatchGrant[i];
                    var params = {
                        Entries: chunk
                    }
    
                    await lfClient.batchGrantPermissions(params).promise();
                }
            }

            if (batchRevoke.length > 0) {
                var chunkBatchRevoke = arrayChunk(batchRevoke, MAX_GRANT_PER_REQUEST);

                for (var i=0;i<chunkBatchRevoke.length;i++) {
                    var chunk = chunkBatchRevoke[i];
                    var params = {
                        Entries: chunk
                    }
    
                    await lfClient.batchRevokePermissions(params).promise();
                }
            }

        } else {
            throw NOTHING_TO_SYNC_MESSAGE;
        }
    },
    createPermissionDiff: async function(lfClient, principalArn, principalLevel, levels, tableMetadata) {
        var resp = {
            grant: null,
            revoke: null
        }

        var principalLevelValue = levels[principalLevel];

        var principalPermissions = await this.getPrincipalTablePermission(lfClient, principalArn, tableMetadata.DatabaseName, tableMetadata.Name);

        var columns = tableMetadata.StorageDescriptor.Columns;

        var grantExcludedColumnNames = [];
        var revokeExcludedColumnNames = [];

        for (var j=0;j<columns.length;j++) {
            var col = columns[j];

            var excluded = false;

            if ("Parameters" in col && CONFIDENTIALITY_LEVEL_PARAM_KEY in col["Parameters"]) {
                var colLevel = col["Parameters"][CONFIDENTIALITY_LEVEL_PARAM_KEY];

                if (colLevel in levels) {
                    var colLevelValue = levels[colLevel];

                    if (colLevelValue > principalLevelValue) {
                        excluded = true;
                    }
                }
            }

            if (excluded && !principalPermissions.ExcludedColumnNames.includes(col.Name)) {
                grantExcludedColumnNames.push(col.Name);
            } else if (!excluded && principalPermissions.ExcludedColumnNames.includes(col.Name)) {
                revokeExcludedColumnNames.push(col.Name);
            }
        }

        if (grantExcludedColumnNames.length > 0) {
            resp.grant = {
                Id: uuid4(),
                Permissions: [
                    "SELECT"
                ],
                Principal: {
                    DataLakePrincipalIdentifier: principalArn
                },
                Resource: {
                    TableWithColumns: {
                        ColumnWildcard: {
                            ExcludedColumnNames: grantExcludedColumnNames
                        },
                        DatabaseName: tableMetadata.DatabaseName,
                        Name: tableMetadata.Name
                    }
                }
            }
        }

        if (revokeExcludedColumnNames.length > 0) {
            resp.revoke = {
                Id: uuid4(),
                Permissions: [
                    "SELECT"
                ],
                Principal: {
                    DataLakePrincipalIdentifier: principalArn
                },
                Resource: {
                    TableWithColumns: {
                        ColumnWildcard: {
                            ExcludedColumnNames: revokeExcludedColumnNames
                        },
                        DatabaseName: tableMetadata.DatabaseName,
                        Name: tableMetadata.Name
                    }
                }
            }
        }

        return resp;
    },
    getPrincipalTablePermission: async function(lfClient, principalArn, dbName, tableName) {
        var params = {
            Principal: {
                DataLakePrincipalIdentifier: principalArn
            },
            ResourceType: "TABLE",
            Resource: {
                Table: {
                    DatabaseName: dbName,
                    Name: tableName
                }
            }
        }        

        var permission = await lfClient.listPermissions(params).promise();

        var resp = {
            ColumnNames: [],
            ExcludedColumnNames: []
        }

        if (permission.PrincipalResourcePermissions.length > 0) {
            var tablePermission = permission.PrincipalResourcePermissions[0];

            if ("TableWithColumns" in tablePermission.Resource) {
                var tableWithColumns = tablePermission.Resource.TableWithColumns;

                if ("ColumnNames" in tableWithColumns) {
                    resp.ColumnNames = tableWithColumns.ColumnNames;
                }

                if ("ColumnWildcard" in tableWithColumns) {
                    resp.ExcludedColumnNames = tableWithColumns.ColumnWildcard.ExcludedColumnNames;
                }
            }
        }

        return resp;
    },
    getAllTables: async function(glueClient, dbName) {
        var nextToken = null;
        var tables = [];
        do {
            var params = {
                DatabaseName: dbName,
                MaxResults: GLUE_API_PAGINATION
            }

            if (nextToken != null) {
                params["NextToken"] = nextToken;
            }

            var temp = await glueClient.getTables(params).promise();
            nextToken = null;
            if (temp["TableList"].length > 0) {
                tables = tables.concat(temp["TableList"]);
                if ("NextToken" in temp) {
                    nextToken = temp["NextToken"];
                }
            }
        } while(nextToken != null);
        
        return tables;
    },
    getConfidentialLevelOfPrincipal: async function(ddbClient, principalArn) {
        var config = await configHandler.loadConfigFile();

        var params = {
            TableName: config.ddb_table_name,
            Key: {
                "principal_arn": {
                    "S": principalArn
                }
            }
        }

        var row = await ddbClient.getItem(params).promise();

        if (row == null || row['Item'] == null) {
            throw PRINCIPAL_CONFIDENTIALITY_NOT_FOUND_MESSAGE;
        }

        return row['Item'].confidentiality_level.S;
        
    },
    isConfidentialityLevelValid: async function(level) {
        var levels = await this.getConfidentialityLevels();
        return level in levels;
    },
    getLevelValue: async function(level) {
        var levels = await this.getConfidentialityLevels();

        if (level in levels) {
            return levels[level];
        }

        return LEVEL_NOT_FOUND_VALUE;
    },
    getConfidentialityLevels: async function() {
        var config = await configHandler.loadConfigFile();

        return config.confidentiality_levels;
    }
}