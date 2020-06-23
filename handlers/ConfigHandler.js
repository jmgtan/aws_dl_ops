const { exception } = require("console");

const fs = require("fs");
const CONFIG_FOLDER_NAME = ".aws_dl_ops";
const CONFIG_FILE_NAME = "config";
const CONFIG_DDB_TABLE_NAME = "aws_dl_ops_principal_access";

module.exports =  {
    config: null,
    init: async function(ddbClient) {
        var config = await this.loadConfigFile();
        var create = false;
        if (!await this.isDDBTableExists(ddbClient, CONFIG_DDB_TABLE_NAME)) {
            var params = {
                TableName: CONFIG_DDB_TABLE_NAME,
                AttributeDefinitions: [
                    {
                        AttributeName: "principal_arn",
                        AttributeType: "S"
                    }
                ],
                KeySchema: [
                    {
                        AttributeName: "principal_arn",
                        KeyType: "HASH"
                    }
                ],
                BillingMode: "PAY_PER_REQUEST"
            };
            await ddbClient.createTable(params).promise();
            create = true;
        }

        await this.generateConfigFile(CONFIG_DDB_TABLE_NAME, config.confidentiality_levels);

        return create;
    },
    isDDBTableExists: async function(ddbClient, tableName) {
        var exists = false;

        try {
            var info = await ddbClient.describeTable({TableName: tableName}).promise();

            exists = info != null;
        } catch (e) {}

        return exists;
    },
    generateConfigFile: async function(ddbTableName, confidentialityLevels) {
        var payload = {
            "ddb_table_name": ddbTableName,
            "confidentiality_levels": confidentialityLevels
        }

        var fileLoc = await this.getLocation();
        await fs.promises.writeFile(fileLoc, JSON.stringify(payload));
    },
    loadConfigFile: async function() {
        if (!this.config) {
            var fileLoc = await this.getLocation();
            var jsonPayload = await fs.promises.readFile(fileLoc, {encoding: 'utf8'});
            this.config = JSON.parse(jsonPayload);
        }

        return this.config;
    },
    getLocation: async function() {
        const homeDir = require('os').homedir();
        const configFolder = homeDir + "/" + CONFIG_FOLDER_NAME;
        const configFile = homeDir + "/" + CONFIG_FOLDER_NAME + "/" + CONFIG_FILE_NAME;

        if (!fs.existsSync(configFolder)) {
            await fs.promises.mkdir(configFolder);
        }

        if (!fs.existsSync(configFile)) {
            var payload = {
                "ddb_table_name": CONFIG_DDB_TABLE_NAME,
                "confidentiality_levels": {
                    "public": 0,
                    "internal": 1,
                    "confidential": 2,
                    "highly-confidential": 3,
                    "critical": 4
                }
            }
            await fs.promises.writeFile(configFile, JSON.stringify(payload));
        }

        return configFile;
    }
}