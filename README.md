# CDM MCP Server

A FastAPI-based service that enables AI assistants to interact with Delta Lake tables stored in MinIO through Spark, implementing the Model Context Protocol (MCP) for natural language data operations.

> **⚠️ Important Warning:** 
> 
> This service allows arbitrary `read-oriented` queries to be executed against Delta Lake tables. Query results will be sent to the model host server, unless you are hosting your model locally.

> **❌** Additionally, this service is **NOT** approved for deployment to any production environment, including CI, until explicit approval is granted by KBase leadership. Use strictly for local development or evaluation purposes only.

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

## API Documentation

The MCP Server provides the following endpoints:

**(WIP)**
- `GET /delta/tables/list` - List available Delta tables
- `GET /delta/tables/read` - Read data from a Delta table
- `GET /delta/tables/schema` - Get schema for a Delta table

For detailed API documentation, visit http://localhost:8000/docs

## AI Assistant Integration

### MCP Client Integration

The MCP Server is compatible with any client that supports the Model Context Protocol. Popular MCP-compatible clients include:

- [Cursor](https://cursor.sh/) - AI-first code editor
- [Cherry Studio](https://github.com/CherryHQ/cherry-studio/) - AI-powered development environment
- [LangChain MCP](https://github.com/langchain-ai/langchain-mcp-adapters) - LangChain integration

To connect an MCP-compatible client:

1. Configure your MCP client with the server URL: http://localhost:8000/mcp
2. Ensure the client has proper authentication if required
3. Test the connection using the MCP discovery endpoint

#### Example: Cursor Integration
As an example, to connect with Cursor:
1. Open Cursor settings
2. Navigate to MCP section
3. Add MCP server URL: http://localhost:8000/mcp
4. Test connection

### Example Prompts

MCP-compatible clients can use natural language to interact with Delta tables. Here are some example prompts:

```markdown
1. "List all Delta tables in my lakehouse tenant"
2. "Show me the schema of the products table"
3. "Read the first 10 rows from the employees table"
4. "Find products with price > 100"
```

### Testing

```bash
# Run tests
pytest

# Run with coverage
pytest --cov=src tests/
```
