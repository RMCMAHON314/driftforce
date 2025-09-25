# DriftForce
Snowflake drift detection that works.
One file. Set environment variables and run.
Slack webhook support added - use --webhook parameter

## Configuration

Set these environment variables:
- SNOWFLAKE_USER: Your username
- SNOWFLAKE_PASSWORD: Your password  
- SNOWFLAKE_ACCOUNT: Your account (include region like XXX.us-east-1 for trial accounts)
- SNOWFLAKE_WAREHOUSE: Your warehouse (default: COMPUTE_WH)
- SNOWFLAKE_ROLE: Your role (default: SYSADMIN)

Test with: ./driftforce.py snapshot --database YOUR_DB --schema PUBLIC --save test.json

