# Apache Airflow with MCP Server

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/badge/Docker-20.10%2B-blue.svg)](https://www.docker.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10.4-017CEE.svg)](https://airflow.apache.org/)
[![Python](https://img.shields.io/badge/Python-3.13-3776AB.svg)](https://www.python.org/)
[![MCP](https://img.shields.io/badge/MCP-Enabled-green.svg)](https://modelcontextprotocol.io/)

A complete Apache Airflow deployment with Docker Compose and integrated MCP (Model Context Protocol) Server for AI-powered workflow management through Warp terminal.

## 🚀 Quick Start

```bash
# Navigate to project directory
cd airflow-deploy-mcp

# Start all services
./start.sh

# Access Airflow UI at http://localhost:8080
# Username: admin
# Password: admin123
```

## ✨ Features

### Airflow Components
- **PostgreSQL**: Metadata database
- **Redis**: Message broker for Celery
- **Airflow Webserver**: Web UI (port 8080)
- **Airflow Scheduler**: DAG scheduling and orchestration
- **Airflow Worker**: Celery executor for task execution
- **Airflow Triggerer**: Support for deferrable operators

### MCP Server Integration
- 🤖 **AI Integration**: Control Airflow through AI assistants in Warp terminal
- 📊 **DAG Management**: List, trigger, and monitor DAGs
- 📈 **Real-time Monitoring**: Check DAG run status and task logs
- 🔧 **Configuration**: Manage Airflow variables and connections
- 🔐 **Security**: Authenticated access via Airflow REST API

### Available MCP Tools
- \`list_dags\` - List all available DAGs
- \`get_dag\` - Get detailed DAG information
- \`trigger_dag\` - Trigger DAG runs with configuration
- \`get_dag_runs\` - View DAG run history
- \`get_dag_run_status\` - Check specific run status
- \`get_task_instances\` - List tasks in a DAG run
- \`get_task_logs\` - Retrieve task execution logs
- \`list_variables\` - List Airflow variables
- \`get_variable\` - Get variable value
- \`set_variable\` - Set or update variables
- \`delete_variable\` - Delete variables
- \`list_connections\` - List Airflow connections

## 📋 Prerequisites

- **Docker Desktop**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Memory**: Minimum 4GB RAM recommended
- **Disk Space**: At least 5GB free space
- **Warp Terminal**: For MCP integration (optional)

## 🛠️ Installation

### 1. Clone or Navigate to Project

```bash
cd airflow-deploy-mcp
```

### 2. Configure Environment Variables

Review and customize the \`.env\` file:

```bash
cat .env
```

**Important**: Change default passwords for production environments!

### 3. Start Services

Using the provided script:
```bash
./start.sh
```

Or manually with Docker Compose:
```bash
docker-compose up -d
```

First-time startup takes 2-3 minutes to:
- Download Docker images
- Initialize Airflow database
- Create admin user
- Start all services

### 4. Verify Status

```bash
docker-compose ps
```

All services should show as "healthy" or "running".

## 📖 Usage

### Accessing Airflow UI

1. Open browser to: **http://localhost:8080**
2. Login with:
   - **Username**: \`admin\`
   - **Password**: \`admin123\`

### Example DAGs

The project includes two example DAGs:

#### 1. \`example_hello_world\`
- **Purpose**: Simple DAG for testing basic functionality
- **Tasks**: bash_hello → python_hello → print_context → goodbye
- **Schedule**: Manual trigger only
- **Features**: Demonstrates basic bash and Python operators

**How to test**:
1. Navigate to DAGs page in Airflow UI
2. Find \`example_hello_world\`
3. Click the Play button (▶️) to trigger
4. View logs in Graph view

#### 2. \`example_etl_pipeline\`
- **Purpose**: Complete ETL workflow demonstration
- **Tasks**: start → extract → transform → validate → load → notify → end
- **Schedule**: Daily (or manual trigger)
- **Features**: Shows XCom usage for inter-task data passing

**How to test**:
1. Go to DAGs page
2. Find \`example_etl_pipeline\`
3. Toggle the On/Off switch to "On" (for auto-scheduling)
4. Click Play button for manual trigger
5. Monitor execution in Graph or Grid view

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f mcp-server
docker-compose logs -f airflow-scheduler
```

### Stopping Services

Using the provided script:
```bash
./stop.sh
```

Or manually:
```bash
# Keep data
docker-compose down

# Remove all data and volumes
docker-compose down -v
```

## 🤖 MCP Server Configuration

### Setting up Warp Terminal

#### Option 1: Using mcp-config.json (Recommended)

1. **View MCP configuration**:
```bash
cat mcp-config.json
```

2. **Add to Warp**:
   - Open Warp Settings
   - Navigate to "Features" → "MCP Servers"
   - Add the configuration from \`mcp-config.json\`

3. **Restart Warp terminal**

#### Option 2: Manual Configuration

Add this configuration to Warp MCP settings:

\`\`\`json
{
  "mcpServers": {
    "airflow": {
      "command": "docker",
      "args": [
        "exec",
        "-i",
        "mcp-server",
        "python",
        "/app/server.py"
      ],
      "env": {
        "AIRFLOW_BASE_URL": "http://localhost:8080",
        "AIRFLOW_API_USERNAME": "admin",
        "AIRFLOW_API_PASSWORD": "admin123"
      }
    }
  }
}
\`\`\`

### Testing MCP Server

After configuration, you can use AI commands in Warp:

```
# List all DAGs
"Show me all Airflow DAGs"

# Trigger a DAG
"Trigger the example_hello_world DAG"

# Check DAG status
"What's the status of the latest run of example_etl_pipeline?"

# Get task logs
"Show me logs for the transform task in example_etl_pipeline"

# Manage variables
"Set Airflow variable API_KEY to test123"
"What's the value of API_KEY variable?"
```

### Example AI Interactions

**Scenario 1: Trigger DAG with Configuration**
```
AI: "Trigger example_hello_world with config message='Hello from AI'"
```

**Scenario 2: Monitor DAG Run**
```
AI: "Show me the last 5 runs of example_etl_pipeline and their status"
```

**Scenario 3: Debug Failed Task**
```
AI: "The ETL pipeline failed, show me logs from the transform task"
```

**Scenario 4: Manage Configuration**
```
AI: "List all Airflow variables and their values"
```

## 📁 Project Structure

```
airflow-deploy-mcp/
├── docker-compose.yaml      # Docker Compose configuration
├── .env                      # Environment variables (gitignored)
├── .env.example             # Environment template
├── .gitignore               # Git ignore patterns
├── mcp-config.json          # MCP Server configuration for Warp
├── README.md                # This file
├── LICENSE                  # MIT License
├── start.sh                 # Startup script
├── stop.sh                  # Shutdown script
│
├── dags/                    # Airflow DAGs directory
│   ├── example_hello_world.py
│   └── example_etl_pipeline.py
│
├── logs/                    # Airflow logs (auto-generated)
├── plugins/                 # Airflow plugins (optional)
├── config/                  # Airflow configuration (optional)
│
└── mcp-server/             # MCP Server implementation
    ├── Dockerfile          # MCP Server Docker image
    ├── requirements.txt    # Python dependencies
    └── server.py          # MCP Server implementation
```

## 🔧 Troubleshooting

### Services Won't Start

**Check Docker**:
```bash
docker info
```

**Check Memory Allocation**:
- Docker Desktop → Settings → Resources
- Recommended: Memory >= 4GB

**Check Initialization Logs**:
```bash
docker-compose logs airflow-init
docker-compose logs airflow-webserver
```

### Airflow UI Not Accessible

**Wait for initialization to complete**:
```bash
docker-compose logs -f airflow-webserver
```

Look for: \`"Running the Gunicorn Server"\`

**Check for port conflicts**:
```bash
lsof -i :8080
```

### DAGs Not Appearing in UI

**Verify DAG files**:
```bash
ls -la dags/
```

**Check scheduler logs**:
```bash
docker-compose logs -f airflow-scheduler
```

**Refresh DAGs**:
- Airflow UI → Admin → Refresh all

### MCP Server Issues

**Check MCP container status**:
```bash
docker-compose ps mcp-server
docker-compose logs mcp-server
```

**Test MCP server connection**:
```bash
docker exec -it mcp-server python -c "import requests; print(requests.get('http://localhost:3000/health').status_code)"
```

**Verify Warp configuration**:
- Check \`mcp-config.json\` syntax
- Restart Warp terminal
- Review Warp logs

### Database Issues

**Reset database** (using script):
```bash
./stop.sh
# Select "Yes" when prompted to remove volumes
./start.sh
```

**Manual database reset**:
```bash
docker-compose down -v
docker volume rm airflow-deploy-mcp_postgres-db-volume
./start.sh
```

### Permission Errors

**Fix folder permissions**:
```bash
mkdir -p logs dags plugins
chmod -R 755 logs dags plugins
```

**Verify AIRFLOW_UID**:
```bash
grep AIRFLOW_UID .env
# Should match your user ID
echo $(id -u)
```

## 🔒 Security Notes

**⚠️ Important for Production Deployments**:

1. **Change Default Passwords**:
   - Admin password in \`.env\`
   - PostgreSQL credentials
   - Fernet key and secret key

2. **Enable HTTPS**:
   - Use reverse proxy (nginx, traefik)
   - Configure SSL certificates

3. **Network Security**:
   - Use Docker network isolation
   - Configure firewall rules
   - Restrict port access

4. **Secrets Management**:
   - Use Airflow Secrets Backend
   - Implement Docker secrets or environment encryption
   - Never store sensitive data in DAG code

## 📚 Documentation

### Apache Airflow
- [Official Documentation](https://airflow.apache.org/docs/)
- [REST API Reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

### MCP (Model Context Protocol)
- [MCP Specification](https://modelcontextprotocol.io/)
- [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)

### Docker
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

If you encounter issues or need help:

1. Check the [Troubleshooting](#troubleshooting) section
2. Review logs with \`docker-compose logs\`
3. Check Airflow UI → Browse → Task Instance Logs

---

**Happy Orchestrating! 🎉**
