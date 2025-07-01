### Updated Document (v2.1)

Here is the revised document with your requested changes.

# Project: Real-Time Data Ingestion and Aggregation Pipeline

**Document Version:** 2.1
**Date:** `[Insert Date]`
**Author(s):** `[Your Name]`
**Stakeholder(s):** `[Your Boss's Name]`

## 1. Executive Summary

This document outlines the requirements and proposed technical design for a new data pipeline. The primary goal is to reliably ingest a high volume of small data files from an external third party, process them in near real-time, and securely deliver aggregated, compressed data batches to an on-premise MinIO storage instance.

The projected data volume is **~864,000 objects per day**, assuming an average rate of 10 files/second. The proposed solution leverages a modern, serverless, event-driven architecture on AWS, designed to be **resilient**, **scalable**, and **cost-effective**. The entire infrastructure will be defined as code (IaC) using Terraform for automated, repeatable, and auditable management.

**Primary Success Metric:** ≥ 99.9% of incoming files will be successfully processed and delivered to the on-premise MinIO instance **within 3 minutes** of their arrival in the S3 landing zone under normal operating conditions.

## 2. Business & Functional Requirements

| ID | Requirement | Details |
| :--- | :--- | :--- |
| **REQ-01** | **Data Ingestion** | The system must provide a secure S3 bucket as a landing zone for an external party to upload data files. |
| **REQ-02** | **Data Aggregation** | The system must collect and aggregate incoming data files over a **1-minute time window**. |
| **REQ-03** | **Data Compression** | The aggregated data batch must be compressed (Gzip) to reduce its size for efficient storage and transfer. |
| **REQ-04** | **Secure Delivery** | The final compressed data batch must be securely delivered to the on-premise MinIO storage instance. |
| **REQ-05** | **Data Integrity** | The system should perform checksum validation on data batches before and after transfer to ensure no data corruption has occurred. |

## 3. Non-Functional Requirements

| ID | Category | Requirement & Rationale |
| :--- | :--- | :--- |
| **NFR-01**| **Availability** | The ingestion endpoint (S3) must achieve **≥ 99.99% availability**. The end-to-end pipeline should be resilient to transient failures of individual components. |
| **NFR-02**| **Latency (SLO)** | **95%** of files should be delivered to MinIO within **2 minutes** of arrival. **99.9%** should be delivered within **3 minutes**. |
| **NFR-03**| **Durability / Retention**| Raw files in the S3 landing zone will be retained in a hot, accessible tier for **7 days** for immediate audit/replay. After 7 days, they will be automatically transitioned to **S3 Glacier Deep Archive** for long-term, low-cost archival (write once, read never). |
| **NFR-04**| **Resilience & Backlog** | In the event of an outage of the on-premise endpoint, the system must buffer incoming data without loss. The SQS queue will be configured with a **4-day message retention** period. Alerts will trigger if the backlog exceeds 1 hour's worth of data. |
| **NFR-05**| **Scalability** | The system must handle the baseline load of **10 files/sec** and be able to automatically scale to handle bursts of up to **100 files/sec** without performance degradation. |
| **NFR-06**| **Security** | Communication must be encrypted-in-transit (TLS 1.2+) at all stages. Data must be encrypted-at-rest in S3 and SQS. Access credentials for MinIO will be managed by AWS Secrets Manager with a defined rotation policy. |
| **NFR-07**| **Observability**| The system must provide key health metrics, including queue depth, processing errors, and processing latency. Critical failures (e.g., failed batches, connectivity loss) must trigger automated alerts. |

## 4. Proposed Architecture

### 4.1. High-Level Design

The architecture remains a decoupled, event-driven pipeline. The introduction of a Dead-Letter Queue (DLQ) and placement of the Lambda within a VPC are key enhancements for production readiness. **The exact network connectivity solution between the AWS VPC and the on-premise data center is To Be Determined (TBD)**, but will be a secure, private connection.

### 4.2. Architectural Diagram (v2.1)

```mermaid
graph TD;
    subgraph "On-Premise Data Center"
        MinIO(fa:fa-hdd MinIO Instance)
    end

    subgraph "AWS Cloud (eu-west-2)"
        subgraph "VPC"
            Lambda(fa:fa-microchip Aggregator Lambda);
            style Lambda fill:#FF9900,stroke:#333,stroke-width:2px
        end

        ExternalParty[External Party] -- "1. Uploads files (HTTPS)" --> S3(fa:fa-database S3 Bucket);
        S3 -- "2. Event Notification" --> SQS(fa:fa-list-alt SQS Queue);
        SQS -- "Persistent Failure" --> DLQ(fa:fa-exclamation-triangle Dead-Letter Queue);

        EventBridge("fa:fa-clock EventBridge Rule <br> rate(1 minute)") -- "4. Triggers" --> Lambda;
        Lambda -- "5. Polls messages" --> SQS;
        Lambda -- "6. Downloads files" --> S3;
        SecretsManager(fa:fa-key Secrets Manager) -- "7. Provides credentials" --> Lambda;
    end

    Lambda -- "8. Pushes batch via<br>Private Network (TBD)" --> MinIO;

    style S3 fill:#f90,stroke:#333,stroke-width:2px
    style SQS fill:#FF4F8B,stroke:#333,stroke-width:2px
    style DLQ fill:#CC0000,stroke:#333,stroke-width:2px

### 4.3. Design Considerations & Risk Mitigation

*   **Idempotency & State Management:** To prevent duplicate processing from rare, repeated S3 events, the Lambda must be idempotent. **DynamoDB** is the recommended choice for this. It is a fully managed, serverless NoSQL database that requires zero maintenance ("no reboot to fix"). A table with a Time-to-Live (TTL) attribute will be used to track recently processed file keys, offering the simplest and most cost-effective solution for this use case compared to ElastiCache.
*   **Lambda Execution:** The processing logic will be written in **Python**.
    *   **Memory/Temp Storage:** The function's memory will be sized based on the maximum expected batch size. If aggregated batches risk exceeding the 512 MB `/tmp` storage, the Lambda will stream data directly or use multipart uploads to MinIO.
    *   **Timeout:** The SQS visibility timeout will be set to **6x the average Lambda execution time** to provide a robust buffer for retries.
*   **Secrets Management:** Credentials for MinIO will be retrieved from Secrets Manager. The Lambda will cache these credentials in-memory for a short period (e.g., 5 minutes) to improve performance and reduce API costs. A rotation policy for these secrets will be established.
*   **Third-Party Uploads:** The design assumes the third party uses single `PutObject` operations. If they use S3 Multipart Uploads, the S3 event trigger will be changed to `s3:ObjectCreated:CompleteMultipartUpload`.

## 5. Implementation & Operations Plan

| Phase | Activity | Key Deliverables / Actions |
| :--- | :--- | :--- |
| **1. Infra Setup** | Core Infrastructure Provisioning | Terraform modules for core components (VPC, IAM roles) and application components (S3, SQS, DynamoDB, Lambda). S3 Block Public Access enabled; bucket policy enforces TLS. |
| **2. Dev & Test**| Lambda Logic & Unit Testing | Develop idempotent **Python** code for the Lambda. Unit test aggregation and compression logic locally using AWS SAM or LocalStack. |
| **3. Integration**| End-to-End & Load Testing | Deploy to a staging environment. Conduct load tests simulating baseline and burst loads. **Crucially, test the outage scenario** by blocking connectivity to a mock endpoint and verifying that SQS buffers correctly and the system recovers. |
| **4. Deployment** | Production Rollout | Use a blue/green deployment strategy for the Lambda function (via aliases and traffic shifting) to enable zero-downtime updates and instant rollbacks. |
| **5. Operations**| Monitoring & Alerting | Configure CloudWatch Alarms for: 1) SQS queue depth (`ApproximateAgeOfOldestMessage` > 1 hour), 2) High Lambda error/throttle rates, 3) Messages in the DLQ. Integrate alerts with your chosen monitoring service. |

## 6. High-Level Cost Estimate

*This is a preliminary estimate and will be refined. Assumes `eu-west-2` region.*

| Service | Dimension | Estimated Monthly Cost | Notes |
| :--- | :--- | :--- | :--- |
| S3 | 26M PUTs, 165 GB-Mo (Hot), 2TB-Mo (Deep Archive) | ~$145 | Assumes 100KB/file avg, 7-day hot tier. |
| SQS | 26M Requests | ~$10 | Standard Queue pricing. |
| Lambda | 44,000 invocations, 1M GB-seconds | ~$20 | Assumes 512MB RAM, 10s avg duration. |
| DynamoDB | On-Demand Capacity, low usage | ~$1-5 | For idempotency tracking. Minimal cost. |
| Data Transfer| ~650 GB Egress to On-Prem | ~$60 | Cost depends on final network solution (TBD).|
| **Total (Est.)** | | **~$240 / month** | *Excludes fixed costs of the private network solution (TBD).* |