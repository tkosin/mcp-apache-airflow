#!/bin/bash

echo "🛑 Stopping Airflow + MCP Server..."
echo "=================================="

# Ask for confirmation if volumes should be removed
read -p "Do you want to remove volumes (database data)? (y/N): " -n 1 -r
echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🗑️  Stopping and removing containers, networks, and volumes..."
    docker-compose down -v
    echo "✅ All services stopped and data removed"
else
    echo "🔄 Stopping services (keeping data)..."
    docker-compose down
    echo "✅ All services stopped (data preserved)"
fi

echo ""
echo "📝 To start again: ./start.sh"
echo "=================================="
