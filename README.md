```mermaid
graph TD
    subgraph Local_Environment [Lokale Umgebung / VM]
        subgraph Docker_Network [Docker Compose Environment]
            A[Python Ingestion Engine] -->|Uses| B(Playwright Browser)
            C[Apache Airflow Scheduler]:::infra -->|Orchestrates & Triggers| A
            C -->|Triggers| D[dbt Core Engine]:::infra
        end
    end

    subgraph AWS_Cloud [AWS Cloud]
        E[(AWS S3 Bucket <br/> jobmarket-analyzer-data-lake)]:::storage
    end

    subgraph Snowflake_Cloud [Snowflake Data Cloud]
        F[(Staging Layer <br/> v_stg_jobs)]:::dwh
        G[(Silver Layer <br/> stg_jobs)]:::dwh
        H[(Gold Layer <br/> Star Schema)]:::dwh
    end

    subgraph Analytics [Reporting]
        I[Streamlit App / UI Dashboard]:::bi
    end

    %% Data Pipeline Connections
    A -->|Uploads JSON| E
    E -->|External Stage Load| F
    D -->|Executes SQL Logic| G
    G -->|Materializes Tables| H
    H -->|Cached Queries| I

    %% Custom Styles
    classDef infra fill:#7952b3,color:#fff,stroke:#333,stroke-width:2px;
    classDef storage fill:#ff9900,color:#fff,stroke:#333,stroke-width:2px;
    classDef dwh fill:#29b5e8,color:#fff,stroke:#333,stroke-width:2px;
    classDef bi fill:#00dbc2,color:#333,stroke:#333,stroke-width:2px;
