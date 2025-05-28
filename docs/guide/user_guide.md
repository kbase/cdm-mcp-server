# CDM MCP Server User Guide

## Table of Contents
1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [Creating Sample Delta Tables](#creating-sample-delta-tables)
4. [Using the API](#using-the-api)
5. [AI Assistant Integration](#ai-assistant-integration)
   - [MCP Configuration](#mcp-configuration)
   - [MCP Host Setup](#mcp-host-setup)
   - [Example Prompts](#example-prompts)

## Introduction

The CDM MCP Server is a FastAPI-based service that enables AI assistants to interact with Delta Lake tables stored in MinIO through Spark. It implements the Model Context Protocol (MCP) to provide LLM-accessible tools for data operations.

> **⚠️ Important Warning:** 
> 
> This service allows arbitrary `read-oriented` queries to be executed against Delta Lake tables. Query results will be sent to the model host server, unless you are hosting your model locally.

> **❌** Additionally, this service is **NOT** approved for deployment to any production environment, including CI, until explicit approval is granted by KBase leadership. Use strictly for local development or evaluation purposes only.

### Key Features
- List and explore Delta Lake tables and databases
- Read table data and schemas
- Execute SQL queries with safety constraints
- Secure access control with KBase authentication
- Integration with AI assistants through MCP

## Quick Start

1. Clone the repository:
   ```bash
   git clone https://github.com/kbase/cdm-mcp-server.git
   cd cdm-mcp-server
   ```

2. Create required directories:
   ```bash
   mkdir -p cdr/cdm/jupyter/cdm_shared_workspace
   ```

3. Create Docker network:
   ```bash
   docker network create cdm-jupyterhub-network
   ```

4. Start the services:
   ```bash
   docker-compose up -d --build
   ```

5. Access the services:
   - MCP Server: http://localhost:8000/docs
   - MinIO Console: http://localhost:9003
   - Spark Master UI: http://localhost:8090
   - JupyterHub: http://localhost:4043

## Creating Sample Delta Tables

### Option 1: Using JupyterHub

1. Open your browser and go to `http://localhost:4043`.
2. Add your CI token as a cookie:
   - Open Developer Tools (e.g. F12) and go to the **Application** (or **Storage**) tab.
   - Under **Cookies**, select `localhost` and add a new entry:
     - **Name:** `kbase_session`
     - **Value:** your CI token
3. Refresh the page to apply the cookie.
4. Create a notebook.
5. Paste and run the following code to generate your sample Delta tables:
```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import random

spark = spark = get_spark_session(yarn=False)

# Create test namespace
namespace = 'test'
create_namespace_if_not_exists(spark, namespace)

# Create employees data
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", IntegerType(), True)
])

departments = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
data = []
for i in range(1, 101):
    data.append((
        i, 
        f"Employee {i}", 
        departments[random.randint(0, len(departments)-1)], 
        random.randint(50000, 150000)
    ))

employees_df = spark.createDataFrame(data, schema)
employees_df.show(5)

# Save employees table to S3 and Hive metastore
(
employees_df.write
    .mode("overwrite")
    .option("path", "s3a://cdm-lake/employees")
    .format("delta")
    .saveAsTable(f"{namespace}.employees")
)

# Create products data
product_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("in_stock", IntegerType(), True)
])

categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]
products = []
for i in range(1, 51):
    products.append((
        i, 
        f"Product {i}", 
        categories[random.randint(0, len(categories)-1)], 
        random.randint(10, 500),
        random.randint(0, 100)
    ))

product_df = spark.createDataFrame(products, product_schema)
product_df.show(5)

# Save products table to S3 and Hive metastore
(
product_df.write
    .mode("overwrite")
    .option("path", "s3a://cdm-lake/products")
    .format("delta")
    .saveAsTable(f"{namespace}.products")
)
```
### Option 2: Check and Create Bucket Manually

1. Open MinIO Console at `http://localhost:9003`
2. Login with user `minio` and password `minio123`
3. Create a new bucket named `cdm-lake`
4. Use this bucket for your Delta tables

## Using the API

#### List Databases
```bash
curl -X 'POST' \
  'http://localhost:8000/delta/databases/list' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-kbase-token-here' \
  -d '{"use_postgres": true}'
```

#### List Tables
```bash
curl -X 'POST' \
  'http://localhost:8000/delta/databases/tables/list' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-kbase-token-here' \
  -d '{"database": "default", "use_postgres": true}'
```

#### Get Table Schema
```bash
curl -X 'POST' \
  'http://localhost:8000/delta/databases/tables/schema' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-kbase-token-here' \
  -d '{"database": "default", "table": "products"}'
```

#### Get Sample Data
```bash
curl -X 'POST' \
  'http://localhost:8000/delta/tables/sample' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-kbase-token-here' \
  -d '{"database": "default", "table": "products", "limit": 10}'
```

#### Count Table Rows
```bash
curl -X 'POST' \
  'http://localhost:8000/delta/tables/count' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-kbase-token-here' \
  -d '{"database": "default", "table": "products"}'
```

#### Query Table
```bash
curl -X 'POST' \
  'http://localhost:8000/delta/tables/query' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer your-kbase-token-here' \
  -d '{"query": "SELECT * FROM test.products WHERE price > 100 LIMIT 5"}'
```

## AI Assistant Integration

The CDM MCP Server implements the Model Context Protocol (MCP), allowing AI assistants to interact with your Delta Lake tables using natural language. This section explains how to configure and use the server with various MCP-compatible clients.

> **⚠️ Reminder** 
> 
> Any data returned by AI generated query will be sent to the AI model host server (e.g. ChatGPT, Anthropic, etc.)—unless you're running the model locally. Only create or import test datasets that are safe to share publicly when using this service.  


### MCP Configuration

1. Create or update your MCP configuration file at `~/.mcp/mcp.json`:
   ```json
   {
     "mcpServers": {
       "delta-lake-mcp": {
         "url": "http://localhost:8000/mcp",
         "enabled": true,
         "headers": {
           "Authorization": "Bearer YOUR_KBASE_TOKEN"
         }
       }
     }
   }
   ```

2. Replace `YOUR_KBASE_TOKEN` with your actual KBase authentication token.

3. Run `chmod 600` on the `mcp.json` file to prevent it from being read by unauthorized parties/tools.

### MCP Host Setup
Most MCP‑enabled tools offer two ways to configure a host:

- **Direct JSON import**  
  Look for an "Import MCP config" or "Load from file" option and point it to your `~/.mcp/mcp.json`.

- **Manual entry**  
  Open the MCP or API settings, then copy the URL, headers, and other fields from your `mcp.json` into the corresponding inputs.  


#### Cherry Studio

1. Open Cherry Studio settings
2. Go to "Integrations" → "MCP"
3. Click "Add Server"
4. Configure with:
   - Name: delta-lake-mcp
   - URL: http://localhost:8000/mcp
   - Headers:
     ```json
     {
       "Authorization": "Bearer YOUR_KBASE_TOKEN"
     }
     ```
5. Save and restart Cherry Studio

#### Cursor

1. Open Cursor settings (⌘,)
2. Navigate to "Model Context Protocol" in the sidebar
3. Click "Add MCP Server"
4. Enter the following details:
   - Name: delta-lake-mcp
   - URL: http://localhost:8000/mcp
   - Headers: 
     ```json
     {
       "Authorization": "Bearer YOUR_KBASE_TOKEN"
     }
     ```
5. Click "Save" and restart Cursor

### Example Prompts

Once configured, you can interact with your Delta tables using natural language. Here are some example prompts:

> **⚠️ Final Warning Before We Phone Home!**  
> 
> Any output from an AI generated query will be sent to the remote AI model host—unless you've configured your MCP Host to use a locally deployed model.

#### Database Exploration
```markdown
1. "List all databases in the system"
2. "Show me the tables in the test database"
3. "What's the schema of the products table?"
```

#### Data Analysis
```markdown
1. "Count the total number of employees"
2. "Show me 5 random employees from the Engineering department"
3. "What's the average salary by department?"
4. "List all products that are out of stock"
5. "Find the top 3 most expensive products in each category"
```

#### Complex Queries
```markdown
1. "Compare the salary distribution between Engineering and Marketing departments"
2. "Find products with price above average for their category"
3. "Show me employees who earn more than their department's average salary"
```

#### Tips for Effective Prompts
- Be specific about the data you want to see
- Mention the table names if you're working with multiple tables
- Specify any filters or conditions clearly
- Use natural language to describe aggregations or calculations

## Using the CDM MCP Server Deployed in Rancher2
Use the steps below to access the CDM MCP Server deployed in Rancher 2 Kubernetes cluster from your local machine.

> **⚠️ Warning**  
> 
> AI query results are sent to a remote host unless using a local model. Ensure your data is public and you have permission from the original author to use it.

### Prerequisites
- Access to Rancher2 (rancher2.berkeley.kbase.us)
- KBase CI auth token
- Rancher CLI tool installed (e.g. `brew install rancher-cli`)

### Step 1: Generate Rancher2 API Keys
If your browser isn't already set up to use the proxy, please refer to the [CDM JupyterHub User Guide](https://github.com/kbase/cdm-jupyterhub/blob/main/docs/user_guide.md#1-create-ssh-tunnel) for instructions on creating an SSH tunnel and configuring your browser to use a SOCKS5 proxy

1. Log into Rancher2 at `https://rancher2.berkeley.kbase.us`
2. Navigate to **Account & API Keys**
3. Click **Create API Key** → **Create**
4. Copy the complete Bearer Token (format: `token-xxxxx:xxxxxxxxxxxxxxxxxxx`)
    ```bash
    # Copy the token
    RANCHER2_TOKEN="token-xxxxx:xxxxxxxxxxxxxxxxxxx"
    
    # Optional - save the token to a file
    echo "$RANCHER2_TOKEN" > ~/.rancher-token
    chmod 600 ~/.rancher-token
    ```

### Step 2: Set Up Environment and Login
1. Create SSH tunnel and configure proxy settings in your terminal:
    ```bash
    # Create SSH tunnel (You might already have this running from Step 1)
    ssh -f -D 1338 <ac.anl_username>@login1.berkeley.kbase.us "/bin/sleep infinity"
    
    # Configure proxy settings
    export HTTP_PROXY="socks5://127.0.0.1:1338"
    export HTTPS_PROXY="socks5://127.0.0.1:1338"
    export NO_PROXY="localhost,127.0.0.1"
    ```

2. Login to Rancher using your API token:
    ```bash
    # Option 1: Direct token login
    rancher login https://rancher2.berkeley.kbase.us/v3 \
      --token "$RANCHER2_TOKEN"

    # Option 2: More secure - token from file created in Step 1
    rancher login https://rancher2.berkeley.kbase.us/v3 \
      --token-file ~/.rancher-token
    ```

3. Verify the connection:
    ```bash
    # List clusters
    rancher clusters ls

    # List pods in the cdm-jupyterhub namespace
    rancher kubectl get pods -n cdm-jupyterhub
    ```

### Step 3: Forward Port to Local Machine
  ```bash
  rancher kubectl port-forward service/cdm-mcp-server 8088:8000 -n cdm-jupyterhub
  ```

This forwards the service port 8000 to your local port 8088.

### Step 4: Update MCP Configuration
Create or update your MCP configuration file at `~/.mcp/mcp.json`:

> **🔑 Authentication Note**  
> The currently deployed CDM MCP Server is configured to use **CI KBase auth server**. Please ensure you are using your **CI KBase Auth token** (not production tokens) for authorization.

```json
{
  "mcpServers": {
    "delta-lake-mcp": {
      "url": "http://localhost:8088/mcp",
      "enabled": true,
      "headers": {
        "Authorization": "Bearer YOUR_CI_KBASE_AUTH_TOKEN"
      }
    }
  }
}
```
