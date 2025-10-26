#!/bin/bash

echo "🚀 Starting Airflow + MCP Server..."
echo "=================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running"
    echo "Please start Docker Desktop and try again"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "Please create .env file first"
    exit 1
fi

# Set AIRFLOW_UID if not set
if ! grep -q "AIRFLOW_UID" .env; then
    echo "Setting AIRFLOW_UID=$(id -u)"
    echo "AIRFLOW_UID=$(id -u)" >> .env
fi

echo "📦 Building MCP Server image..."
docker-compose build mcp-server

echo "🔄 Starting services..."
docker-compose up -d

echo ""
echo "⏳ Waiting for services to be ready..."
echo "This may take 2-3 minutes on first run..."

# Wait for webserver
MAX_RETRIES=60
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    if docker-compose ps | grep airflow-webserver | grep -q "healthy\|running"; then
        echo "✅ Airflow Webserver is ready!"
        break
    fi
    echo -n "."
    sleep 5
    RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo ""
    echo "⚠️  Timeout waiting for services. Check logs with: docker-compose logs"
    exit 1
fi

echo ""
echo "=================================="
echo "✅ All services are running!"
echo ""
echo "📊 Access Points:"
echo "   Airflow UI: http://localhost:8080"
echo "   Username:   admin"
echo "   Password:   admin123"
echo ""
echo "   PostgreSQL: localhost:5432"
echo "   MCP Server: Docker container 'mcp-server'"
echo ""
echo "📝 Useful Commands:"
echo "   View logs:    docker-compose logs -f"
echo "   Stop:         ./stop.sh or docker-compose down"
echo "   Restart:      docker-compose restart"
echo ""
echo "🔧 To configure MCP in Warp, see README.md"
echo "=================================="
