#!/usr/bin/env python3
"""
MCP Server for Apache Airflow Integration
Provides AI-accessible tools for managing Airflow workflows
"""

import os
import json
import logging
import base64
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime

import httpx
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from pydantic import BaseModel, Field

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration from environment
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver:8080")
AIRFLOW_API_USERNAME = os.getenv("AIRFLOW_API_USERNAME", "admin")
AIRFLOW_API_PASSWORD = os.getenv("AIRFLOW_API_PASSWORD", "admin")
MCP_SERVER_HOST = os.getenv("MCP_SERVER_HOST", "0.0.0.0")
MCP_SERVER_PORT = int(os.getenv("MCP_SERVER_PORT", "3000"))

# DAG files directory (mounted volume)
DAGS_FOLDER = os.getenv("DAGS_FOLDER", "/opt/airflow/dags")


class AirflowClient:
    """HTTP client for Airflow REST API"""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip('/')
        self.auth = (username, password)
        self.client = httpx.AsyncClient(
            base_url=f"{self.base_url}/api/v1",
            auth=self.auth,
            timeout=30.0
        )
    
    async def list_dags(self, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        """List all DAGs"""
        response = await self.client.get(
            "/dags",
            params={"limit": limit, "offset": offset}
        )
        response.raise_for_status()
        return response.json()
    
    async def get_dag(self, dag_id: str) -> Dict[str, Any]:
        """Get details of a specific DAG"""
        response = await self.client.get(f"/dags/{dag_id}")
        response.raise_for_status()
        return response.json()
    
    async def trigger_dag(
        self, 
        dag_id: str, 
        conf: Optional[Dict[str, Any]] = None,
        logical_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Trigger a DAG run"""
        data = {
            "conf": conf or {},
        }
        if logical_date:
            data["logical_date"] = logical_date
        
        response = await self.client.post(
            f"/dags/{dag_id}/dagRuns",
            json=data
        )
        response.raise_for_status()
        return response.json()
    
    async def get_dag_runs(
        self, 
        dag_id: str, 
        limit: int = 25,
        state: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get DAG runs for a specific DAG"""
        params = {"limit": limit}
        if state:
            params["state"] = state
        
        response = await self.client.get(
            f"/dags/{dag_id}/dagRuns",
            params=params
        )
        response.raise_for_status()
        return response.json()
    
    async def get_dag_run(self, dag_id: str, dag_run_id: str) -> Dict[str, Any]:
        """Get details of a specific DAG run"""
        response = await self.client.get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}"
        )
        response.raise_for_status()
        return response.json()
    
    async def get_task_instances(
        self, 
        dag_id: str, 
        dag_run_id: str
    ) -> Dict[str, Any]:
        """Get task instances for a DAG run"""
        response = await self.client.get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        )
        response.raise_for_status()
        return response.json()
    
    async def get_task_logs(
        self, 
        dag_id: str, 
        dag_run_id: str, 
        task_id: str,
        task_try_number: int = 1
    ) -> str:
        """Get logs for a specific task instance"""
        response = await self.client.get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{task_try_number}"
        )
        response.raise_for_status()
        return response.text
    
    async def get_variables(self, limit: int = 100) -> Dict[str, Any]:
        """List Airflow variables"""
        response = await self.client.get(
            "/variables",
            params={"limit": limit}
        )
        response.raise_for_status()
        return response.json()
    
    async def get_variable(self, variable_key: str) -> Dict[str, Any]:
        """Get a specific variable"""
        response = await self.client.get(f"/variables/{variable_key}")
        response.raise_for_status()
        return response.json()
    
    async def set_variable(self, key: str, value: str) -> Dict[str, Any]:
        """Set or update a variable"""
        response = await self.client.patch(
            f"/variables/{key}",
            json={"key": key, "value": value}
        )
        if response.status_code == 404:
            # Variable doesn't exist, create it
            response = await self.client.post(
                "/variables",
                json={"key": key, "value": value}
            )
        response.raise_for_status()
        return response.json()
    
    async def delete_variable(self, variable_key: str) -> None:
        """Delete a variable"""
        response = await self.client.delete(f"/variables/{variable_key}")
        response.raise_for_status()
    
    async def get_connections(self, limit: int = 100) -> Dict[str, Any]:
        """List Airflow connections"""
        response = await self.client.get(
            "/connections",
            params={"limit": limit}
        )
        response.raise_for_status()
        return response.json()
    
    async def pause_dag(self, dag_id: str) -> Dict[str, Any]:
        """Pause a DAG"""
        response = await self.client.patch(
            f"/dags/{dag_id}",
            json={"is_paused": True}
        )
        response.raise_for_status()
        return response.json()
    
    async def unpause_dag(self, dag_id: str) -> Dict[str, Any]:
        """Unpause a DAG"""
        response = await self.client.patch(
            f"/dags/{dag_id}",
            json={"is_paused": False}
        )
        response.raise_for_status()
        return response.json()


class DAGFileManager:
    """Manager for DAG file operations"""
    
    def __init__(self, dags_folder: str):
        self.dags_folder = Path(dags_folder)
        self.dags_folder.mkdir(parents=True, exist_ok=True)
    
    def list_dag_files(self) -> List[Dict[str, Any]]:
        """List all Python files in the DAGs folder"""
        files = []
        for file_path in self.dags_folder.glob("*.py"):
            if file_path.name.startswith("__"):
                continue
            stat = file_path.stat()
            files.append({
                "filename": file_path.name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "path": str(file_path.relative_to(self.dags_folder))
            })
        return sorted(files, key=lambda x: x["filename"])
    
    def read_dag_file(self, filename: str) -> str:
        """Read content of a DAG file"""
        file_path = self.dags_folder / filename
        if not file_path.exists():
            raise FileNotFoundError(f"DAG file '{filename}' not found")
        if not file_path.suffix == ".py":
            raise ValueError("Only Python files (.py) are allowed")
        return file_path.read_text(encoding="utf-8")
    
    def write_dag_file(self, filename: str, content: str, overwrite: bool = False) -> Dict[str, Any]:
        """Write content to a DAG file"""
        if not filename.endswith(".py"):
            raise ValueError("DAG filename must end with .py")
        if filename.startswith("__"):
            raise ValueError("DAG filename cannot start with __")
        
        file_path = self.dags_folder / filename
        
        if file_path.exists() and not overwrite:
            raise FileExistsError(f"DAG file '{filename}' already exists. Use overwrite=True to replace")
        
        # Validate Python syntax
        try:
            compile(content, filename, 'exec')
        except SyntaxError as e:
            raise ValueError(f"Invalid Python syntax: {str(e)}")
        
        file_path.write_text(content, encoding="utf-8")
        stat = file_path.stat()
        
        return {
            "filename": filename,
            "size": stat.st_size,
            "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "message": f"DAG file '{filename}' {'updated' if overwrite else 'created'} successfully"
        }
    
    def delete_dag_file(self, filename: str) -> Dict[str, str]:
        """Delete a DAG file"""
        file_path = self.dags_folder / filename
        if not file_path.exists():
            raise FileNotFoundError(f"DAG file '{filename}' not found")
        if not file_path.suffix == ".py":
            raise ValueError("Only Python files (.py) can be deleted")
        
        file_path.unlink()
        return {
            "filename": filename,
            "message": f"DAG file '{filename}' deleted successfully"
        }
    
    def validate_dag_file(self, content: str) -> Dict[str, Any]:
        """Validate DAG file content"""
        issues = []
        
        # Check Python syntax
        try:
            compile(content, '<string>', 'exec')
        except SyntaxError as e:
            issues.append({"type": "syntax_error", "message": str(e), "line": e.lineno})
        
        # Check for required imports
        required_patterns = [
            (r'from airflow import DAG', 'Missing Airflow DAG import'),
            (r'from airflow.operators', 'Missing operator imports'),
        ]
        
        for pattern, message in required_patterns:
            if pattern not in content:
                issues.append({"type": "warning", "message": message})
        
        # Check for DAG definition
        if 'with DAG(' not in content and 'DAG(' not in content:
            issues.append({"type": "error", "message": "No DAG definition found"})
        
        return {
            "valid": len([i for i in issues if i["type"] == "error"]) == 0,
            "issues": issues
        }


# Initialize Airflow client
airflow_client = AirflowClient(
    AIRFLOW_BASE_URL, 
    AIRFLOW_API_USERNAME, 
    AIRFLOW_API_PASSWORD
)

# Initialize DAG file manager
dag_file_manager = DAGFileManager(DAGS_FOLDER)

# Initialize MCP Server
mcp_server = Server("airflow-mcp-server")


@mcp_server.list_tools()
async def list_tools() -> List[Tool]:
    """List available MCP tools"""
    return [
        Tool(
            name="list_dags",
            description="List all available Airflow DAGs with their status and configuration",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of DAGs to return (default: 100)",
                        "default": 100
                    },
                    "offset": {
                        "type": "integer",
                        "description": "Offset for pagination (default: 0)",
                        "default": 0
                    }
                }
            }
        ),
        Tool(
            name="get_dag",
            description="Get detailed information about a specific DAG",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG to retrieve"
                    }
                },
                "required": ["dag_id"]
            }
        ),
        Tool(
            name="trigger_dag",
            description="Trigger a DAG run with optional configuration parameters",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG to trigger"
                    },
                    "conf": {
                        "type": "object",
                        "description": "Optional configuration dictionary to pass to the DAG",
                        "default": {}
                    },
                    "logical_date": {
                        "type": "string",
                        "description": "Optional logical date in ISO format (e.g., 2024-01-01T00:00:00Z)"
                    }
                },
                "required": ["dag_id"]
            }
        ),
        Tool(
            name="get_dag_runs",
            description="Get the history of DAG runs for a specific DAG",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of runs to return (default: 25)",
                        "default": 25
                    },
                    "state": {
                        "type": "string",
                        "description": "Filter by state (success, running, failed, etc.)",
                        "enum": ["success", "running", "failed", "queued"]
                    }
                },
                "required": ["dag_id"]
            }
        ),
        Tool(
            name="get_dag_run_status",
            description="Get the status and details of a specific DAG run",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG"
                    },
                    "dag_run_id": {
                        "type": "string",
                        "description": "The ID of the DAG run"
                    }
                },
                "required": ["dag_id", "dag_run_id"]
            }
        ),
        Tool(
            name="get_task_instances",
            description="Get all task instances for a specific DAG run",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG"
                    },
                    "dag_run_id": {
                        "type": "string",
                        "description": "The ID of the DAG run"
                    }
                },
                "required": ["dag_id", "dag_run_id"]
            }
        ),
        Tool(
            name="get_task_logs",
            description="Get logs for a specific task instance",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG"
                    },
                    "dag_run_id": {
                        "type": "string",
                        "description": "The ID of the DAG run"
                    },
                    "task_id": {
                        "type": "string",
                        "description": "The ID of the task"
                    },
                    "task_try_number": {
                        "type": "integer",
                        "description": "The try number (default: 1)",
                        "default": 1
                    }
                },
                "required": ["dag_id", "dag_run_id", "task_id"]
            }
        ),
        Tool(
            name="list_variables",
            description="List all Airflow variables",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of variables to return (default: 100)",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="get_variable",
            description="Get the value of a specific Airflow variable",
            inputSchema={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "The key of the variable to retrieve"
                    }
                },
                "required": ["key"]
            }
        ),
        Tool(
            name="set_variable",
            description="Set or update an Airflow variable",
            inputSchema={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "The key of the variable"
                    },
                    "value": {
                        "type": "string",
                        "description": "The value to set"
                    }
                },
                "required": ["key", "value"]
            }
        ),
        Tool(
            name="delete_variable",
            description="Delete an Airflow variable",
            inputSchema={
                "type": "object",
                "properties": {
                    "key": {
                        "type": "string",
                        "description": "The key of the variable to delete"
                    }
                },
                "required": ["key"]
            }
        ),
        Tool(
            name="list_connections",
            description="List all Airflow connections (credentials are masked)",
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of connections to return (default: 100)",
                        "default": 100
                    }
                }
            }
        ),
        Tool(
            name="pause_dag",
            description="Pause a DAG to prevent it from scheduling new runs",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG to pause"
                    }
                },
                "required": ["dag_id"]
            }
        ),
        Tool(
            name="unpause_dag",
            description="Unpause a DAG to allow it to schedule new runs",
            inputSchema={
                "type": "object",
                "properties": {
                    "dag_id": {
                        "type": "string",
                        "description": "The ID of the DAG to unpause"
                    }
                },
                "required": ["dag_id"]
            }
        ),
        Tool(
            name="list_dag_files",
            description="List all DAG Python files in the dags folder",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="read_dag_file",
            description="Read the content of a specific DAG file",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "The filename of the DAG file (must end with .py)"
                    }
                },
                "required": ["filename"]
            }
        ),
        Tool(
            name="upload_dag_file",
            description="Upload a new DAG file or update an existing one",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "The filename for the DAG (must end with .py)"
                    },
                    "content": {
                        "type": "string",
                        "description": "The Python code content of the DAG file"
                    },
                    "overwrite": {
                        "type": "boolean",
                        "description": "Whether to overwrite if file exists (default: false)",
                        "default": False
                    }
                },
                "required": ["filename", "content"]
            }
        ),
        Tool(
            name="delete_dag_file",
            description="Delete a DAG file from the dags folder",
            inputSchema={
                "type": "object",
                "properties": {
                    "filename": {
                        "type": "string",
                        "description": "The filename of the DAG file to delete"
                    }
                },
                "required": ["filename"]
            }
        ),
        Tool(
            name="validate_dag_file",
            description="Validate DAG file content for syntax and structure errors",
            inputSchema={
                "type": "object",
                "properties": {
                    "content": {
                        "type": "string",
                        "description": "The Python code content to validate"
                    }
                },
                "required": ["content"]
            }
        )
    ]


@mcp_server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls"""
    try:
        logger.info(f"Tool called: {name} with arguments: {arguments}")
        
        if name == "list_dags":
            result = await airflow_client.list_dags(
                limit=arguments.get("limit", 100),
                offset=arguments.get("offset", 0)
            )
            
        elif name == "get_dag":
            result = await airflow_client.get_dag(arguments["dag_id"])
            
        elif name == "trigger_dag":
            result = await airflow_client.trigger_dag(
                dag_id=arguments["dag_id"],
                conf=arguments.get("conf"),
                logical_date=arguments.get("logical_date")
            )
            
        elif name == "get_dag_runs":
            result = await airflow_client.get_dag_runs(
                dag_id=arguments["dag_id"],
                limit=arguments.get("limit", 25),
                state=arguments.get("state")
            )
            
        elif name == "get_dag_run_status":
            result = await airflow_client.get_dag_run(
                dag_id=arguments["dag_id"],
                dag_run_id=arguments["dag_run_id"]
            )
            
        elif name == "get_task_instances":
            result = await airflow_client.get_task_instances(
                dag_id=arguments["dag_id"],
                dag_run_id=arguments["dag_run_id"]
            )
            
        elif name == "get_task_logs":
            result = await airflow_client.get_task_logs(
                dag_id=arguments["dag_id"],
                dag_run_id=arguments["dag_run_id"],
                task_id=arguments["task_id"],
                task_try_number=arguments.get("task_try_number", 1)
            )
            
        elif name == "list_variables":
            result = await airflow_client.get_variables(
                limit=arguments.get("limit", 100)
            )
            
        elif name == "get_variable":
            result = await airflow_client.get_variable(arguments["key"])
            
        elif name == "set_variable":
            result = await airflow_client.set_variable(
                key=arguments["key"],
                value=arguments["value"]
            )
            
        elif name == "delete_variable":
            await airflow_client.delete_variable(arguments["key"])
            result = {"status": "deleted", "key": arguments["key"]}
            
        elif name == "list_connections":
            result = await airflow_client.get_connections(
                limit=arguments.get("limit", 100)
            )
        
        elif name == "pause_dag":
            result = await airflow_client.pause_dag(arguments["dag_id"])
        
        elif name == "unpause_dag":
            result = await airflow_client.unpause_dag(arguments["dag_id"])
        
        elif name == "list_dag_files":
            result = dag_file_manager.list_dag_files()
        
        elif name == "read_dag_file":
            content = dag_file_manager.read_dag_file(arguments["filename"])
            result = {
                "filename": arguments["filename"],
                "content": content,
                "size": len(content)
            }
        
        elif name == "upload_dag_file":
            result = dag_file_manager.write_dag_file(
                filename=arguments["filename"],
                content=arguments["content"],
                overwrite=arguments.get("overwrite", False)
            )
        
        elif name == "delete_dag_file":
            result = dag_file_manager.delete_dag_file(arguments["filename"])
        
        elif name == "validate_dag_file":
            result = dag_file_manager.validate_dag_file(arguments["content"])
            
        else:
            raise ValueError(f"Unknown tool: {name}")
        
        # Format response
        if isinstance(result, str):
            response_text = result
        else:
            response_text = json.dumps(result, indent=2, ensure_ascii=False)
        
        logger.info(f"Tool {name} executed successfully")
        return [TextContent(type="text", text=response_text)]
        
    except httpx.HTTPStatusError as e:
        error_msg = f"Airflow API error: {e.response.status_code} - {e.response.text}"
        logger.error(error_msg)
        return [TextContent(type="text", text=error_msg)]
        
    except Exception as e:
        error_msg = f"Error executing tool {name}: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return [TextContent(type="text", text=error_msg)]


async def main():
    """Main entry point for MCP server"""
    logger.info(f"Starting Airflow MCP Server")
    logger.info(f"Airflow URL: {AIRFLOW_BASE_URL}")
    logger.info(f"Server listening on stdio")
    logger.info(f"Waiting for MCP client connection via docker exec...")
    
    try:
        # Run the server - this will block until stdin is closed
        async with stdio_server() as (read_stream, write_stream):
            logger.info("MCP client connected")
            await mcp_server.run(
                read_stream,
                write_stream,
                mcp_server.create_initialization_options()
            )
            logger.info("Client session ended normally")
    except BrokenPipeError:
        logger.info("Client disconnected (broken pipe)")
    except EOFError:
        logger.info("Client disconnected (EOF)")
    except Exception as e:
        logger.error(f"Server error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    import asyncio
    import sys
    
    # Ensure unbuffered output
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)
    
    logger.info("MCP Server starting...")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Stdin is a TTY: {sys.stdin.isatty()}")
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except EOFError:
        logger.info("Server stopped: EOF received")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
