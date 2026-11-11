# Infrastructure as Code (IaC)

This directory is reserved for Infrastructure as Code (Terraform, OpenTofu, AWS CDK, or CloudFormation) used to provision cloud resources corresponding to this data engineering platform.

---

## 📁 Directory Structure

```text
infra/
├── environments/      # Environment-specific deployments (local, dev, stage, prod)
│   └── .gitkeep
├── modules/           # Reusable IaC modules (S3, Glue, IAM, Networking)
│   └── .gitkeep
└── README.md          # Infrastructure documentation and instructions
```

---

## 🎯 Target Cloud Resources

When deploying to real AWS environments, the resources provisioned here mirror the local Docker/LocalStack environment:

1. **Amazon S3**:
   - `s3://<project>-raw-data-bucket`
   - `s3://<project>-processed-data-bucket`
   - `s3://<project>-curated-data-bucket`

2. **AWS Glue**:
   - Glue Data Catalog databases and tables.
   - Glue PySpark 4.0 Job definitions matching `glue_jobs/sample_etl_job.py`.
   - IAM Service Roles with appropriate S3 read/write and CloudWatch logging permissions.

3. **Apache Airflow / Amazon MWAA**:
   - IAM execution role for MWAA or Airflow worker task execution.
   - S3 bucket synchronization for DAGs (`dags/`).

