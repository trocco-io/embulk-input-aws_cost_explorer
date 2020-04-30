# Aws Cost Explorer input plugin for Embulk

AWS Cost Explorer Cost and Usage Report

## Overview

- **Plugin type**: input
- **Resume supported**: yes
- **Cleanup supported**: yes
- **Guess supported**: no

## Configuration

- **access_key_id**: AWS Access Key Id (integer, required)
- **secret_access_key**: AWS Secrete Access Key (string, default: `myvalue`)
- **metrics**: metrics of the report (`AmortizedCost` `BlendedCost` `NetAmortizedCost` `NetUnblendedCost` `NormalizedUsageAmount` `UnblendedCost` `UsageQuantity`, default: `UnblendedCost`)
- **start_date**: start date of the report (string, required, format: `2020-01-01`)
- **end_date**: end date of the report (string, required, format: `2020-01-01`)

## Example

```yaml
in:
  type: aws_cost_explorer
  access_key_id: xxx
  secret_access_key: yyy
  metrics: UnblendedCost
  start_date: "2020-04-01"
  end_date: "2020-04-20"
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
