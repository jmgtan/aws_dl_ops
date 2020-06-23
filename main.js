#!/usr/bin/env node

const { program } = require('commander');
const AWS = require("aws-sdk");
const configHandler = require("./handlers/ConfigHandler");
const principalHandler = require("./handlers/PrincipalHandler");
const AsciiTable = require("ascii-table");

program.version('0.0.1');

program
    .option('-i, --init', 'Initializes backend, this is a required step before using the CLI for the first time.')
    .option('-pr, --principal <arn>', 'The principal to attach/query.')
    .option('-c, --confidentiality <level>', 'The data confidentiality level to associate with the principal, this would override the previous setting.')
    .option('-s, --sync', 'Apply Lake Formation permissions in relation to principal\'s confidentiality level.')
    .option('-d, --database <dbname>', 'The database to sync.')
    .option('-t, --table <tableName>', 'The table to sync, this is optional. If table is not specified, all tables inside the database will be synchronized.')
    .option('-pc, --printconfidentiality', 'Print confidentiality levels.')
    .option('-p, --profile <profileName>', 'AWS profile to use.')
    .option('-r, --region <region>', 'Region override');

program.parse(process.argv);

var awsClientParams = {};

if (program.profile) {
    awsClientParams['credentials'] = new AWS.SharedIniFileCredentials({profile: program.profile});
}

if (program.region) {
    awsClientParams['region'] = program.region;
}

const ddbClient = new AWS.DynamoDB(awsClientParams);
const lfClient = new AWS.LakeFormation(awsClientParams);
const glueClient = new AWS.Glue(awsClientParams);

(async () => {
    try {
        if (program.init) {
            var result = await configHandler.init(ddbClient);
            if (result) {
                console.log("Created new DynamoDB table: DONE");
            } else {
                console.log("DynamoDB table already exists");
            }
        } else if (program.principal) {
            if (program.confidentiality) {
                await principalHandler.setConfidentialityLevel(ddbClient, program.principal, program.confidentiality);
                console.log("Confidential level set for principal.")
            } else if (program.sync) {
                if (program.database) {
                    await principalHandler.syncWithLakeFormation(ddbClient, lfClient, glueClient, program.principal, program.database, program.table);
                    console.log("Permissions synchronized");
                } else {
                    throw "Database is a required parameter.";
                }
            }
        } else if (program.printconfidentiality) {
            var config = await configHandler.loadConfigFile();
            var table = new AsciiTable('Confidentiality Levels');
            table.setHeading('Name', 'Score');

            var levels = config.confidentiality_levels;
            for (l in levels) {
                table.addRow(l, levels[l]);
            }

            console.log(table.toString());
        }
    } catch(e) {
        console.error(e);
    }
})();
