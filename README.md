# AWS Data Lake Operations tool

This CLI tool enables automated Lake Formation permission grant/revokes based on column level `confidentiality_level` in relation to the principal's confidentiality_level. By default, this tool uses the following levels:

| Level | Score |
| ----- | ----- |
| `public` | 0 |
| `internal` | 1 |
| `confidential` | 2 |
| `highly-confidential` | 3 |
| `critical` | 4 |

The higher the score, the higher the confidentiality of a column of data.

## Requirements

- The IAM user or [role](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-role.html) that must be used in conjunction with this tool must have [LakeFormation administrator privileges](https://docs.aws.amazon.com/lake-formation/latest/dg/getting-started-setup.html).
- This tool uses node js, so you must have that installed to use this.

## Getting Started

To get started, clone the repository, run `npm install` and make sure that `main.js` has execute privileges by executing the following:

```
chmod +x main.js
```

You can see the options that are available by executing the following:

```
./main.js --help
```

### Initialize
First thing we need to do is initialize by executing the following:

```
./main.js -i -p <awsCLIprofile> -r <region>
```

This will create a DynamoDB table in the AWS account and region indicated by the profile and region parameter respectively.

### Define Principal Confidentiality Level
Next is to create an entry using the following:

```
./main.js -i -pr <principalArn> -c <level> -p <awsCLIprofile> -r <region>
```

The Principal ARN can be an IAM user or a role, and the level is based on the table above. This data would be compared to the column level confidentiality based on the tag `confidentiality_level` of the table column. Any level that is greater than the principal's configured level, the CLI will create a column level exclude permission for that principal.

### Sync

No Lake Formation permission has been granted/revoked at this stage, to synchronized execute the following command:

```
./main.js -pr <principalArn> -s -d <dbName> -t <tableName> -p <awsCLIprofile> -r <region>
```

This will compare confidentiality levels between table and principal and then a series of grant/revoke permissions would be executed. The `tableName` parameter is optional, if that is ommitted, the tool would execute the synchronize command across all the tables in the database.

### Automation
This tool can be used in conjunction with [Glue Cloudwatch Events](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/EventTypes.html#glue-event-types) in response to schema/table updates, or DynamoDB stream events in response to confidentiality changes to a specific principal arn. The Lambda execution role must also have LakeFormation administrator privileges to be able to grant/revoke permissions accordingly.