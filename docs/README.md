# Documentation

These guides describe the checked-in AWS-oriented pipeline. The production data
boundary uses S3 and the supported Iceberg catalog is AWS Glue; MinIO and its
Hadoop catalog exist only as an optional local test harness. Start with the
document that fits the task instead of reading the entire repository README.

| If you need to… | Read |
| --- | --- |
| Set up the project, run the AWS workflow, use the local test harness, or execute a command | [Development](DEVELOPMENT.md) |
| Understand pipeline stages, DAG assets, and orchestration | [Architecture](ARCHITECTURE.md) |
| Inspect raw data, contracts, table grains, or snapshots | [Data Model](DATA_MODEL.md) |
| Understand retries, failures, and intentional boundaries | [Edge Cases](EDGE_CASES.md) |
| Publish or operate the Evidence site and supported runtime paths | [Deployment](DEPLOY.md) |
| Change the Evidence application or its snapshots | [Evidence reports](../reports/README.md) |

For a visual overview, open the deployed [interactive PipeFlow architecture
whiteboard](https://ukiah-heasley.github.io/sf-urban-health/architecture/sf-urban-health.pipeflow.html).
