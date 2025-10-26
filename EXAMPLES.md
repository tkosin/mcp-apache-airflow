# MCP Server Usage Examples

This document provides practical examples of using the MCP Server to manage Airflow workflows through AI assistants in Warp terminal.

## Table of Contents

- [DAG Management](#dag-management)
- [DAG File Management](#dag-file-management)
- [Task Monitoring](#task-monitoring)
- [Configuration Management](#configuration-management)

## DAG Management

### List All DAGs

```
"Show me all available Airflow DAGs"
```

Response includes:
- DAG IDs
- Is paused status
- Last run date
- Schedule interval

### Get DAG Details

```
"Get detailed information about the example_etl_pipeline DAG"
```

### Trigger DAG Run

```
"Trigger the example_hello_world DAG"
```

With configuration:
```
"Trigger example_etl_pipeline with config: {'source': 'api', 'limit': 1000}"
```

### Pause/Unpause DAG

```
"Pause the example_etl_pipeline DAG"
"Unpause the example_hello_world DAG"
```

### View DAG Run History

```
"Show me the last 10 runs of example_etl_pipeline"
"Show failed runs of example_hello_world"
```

## DAG File Management

### List DAG Files

```
"List all DAG files in the dags folder"
```

Response includes:
- Filename
- File size
- Last modified date
- Relative path

### Read DAG File

```
"Show me the content of example_hello_world.py"
"Read the DAG file scalable_data_pipeline.py"
```

### Upload New DAG

Example: Creating a simple DAG

```
"Create a new DAG file called my_daily_task.py with the following code:

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_message():
    print('Hello from my daily task!')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='my_daily_task',
    default_args=default_args,
    description='A simple daily task',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['custom', 'daily'],
) as dag:
    
    task = PythonOperator(
        task_id='print_message',
        python_callable=print_message,
    )
"
```

### Update Existing DAG

```
"Update example_hello_world.py to add a new task that sends an email notification"
```

Note: Use `overwrite=true` parameter to replace existing files.

### Validate DAG Code

Before uploading, validate your DAG:

```
"Validate this DAG code for syntax errors:
[paste your DAG code here]
"
```

The validation checks for:
- Python syntax errors
- Required Airflow imports
- DAG definition presence
- Common issues

### Delete DAG File

```
"Delete the DAG file my_old_pipeline.py"
```

**Warning**: This permanently deletes the file. Make sure to backup if needed.

## Task Monitoring

### Get Task Instances

```
"Show me all tasks in the latest run of example_etl_pipeline"
```

### View Task Logs

```
"Show logs for the transform task in example_etl_pipeline"
"Get logs for task extract in DAG run scheduled__2024-01-01T00:00:00+00:00"
```

### Debug Failed Tasks

```
"The ETL pipeline failed on the validate task, show me the logs"
"What went wrong with the last run of example_hello_world?"
```

## Configuration Management

### List Variables

```
"List all Airflow variables"
"Show me variables that start with 'API'"
```

### Get Variable Value

```
"What is the value of API_KEY variable?"
"Get the DATABASE_URL variable"
```

### Set Variable

```
"Set Airflow variable API_ENDPOINT to https://api.example.com"
"Create a variable named MAX_RETRIES with value 3"
```

### Delete Variable

```
"Delete the OLD_API_KEY variable"
```

### List Connections

```
"Show me all configured Airflow connections"
```

Note: Credentials are masked for security.

## Advanced Workflows

### Create and Test a New DAG

1. **Validate the DAG code first:**
   ```
   "Validate this DAG code: [paste code]"
   ```

2. **Upload the DAG:**
   ```
   "Upload this as new_pipeline.py: [paste validated code]"
   ```

3. **Check if it's detected:**
   ```
   "List all DAGs"
   ```

4. **Trigger a test run:**
   ```
   "Trigger new_pipeline DAG"
   ```

5. **Monitor the run:**
   ```
   "What's the status of the latest run of new_pipeline?"
   ```

### Bulk DAG Management

```
"Pause all DAGs that have 'test' in their name"
"Show me all DAG files that were modified in the last 7 days"
"List all failed DAG runs from today"
```

### Debugging Workflow

```
"Show me the latest failed runs across all DAGs"
"For the failed example_etl_pipeline run, show me:
 1. All task statuses
 2. Logs for failed tasks
 3. The DAG configuration used"
```

## Best Practices

### File Naming

- Use descriptive names: `customer_data_pipeline.py`
- Follow Python naming conventions: lowercase with underscores
- Avoid special characters except underscores
- Don't use `__init__.py` or files starting with `__`

### DAG Validation

Always validate DAG code before uploading:
- Check Python syntax
- Verify imports
- Ensure DAG definition exists
- Test locally if possible

### Version Control

- Keep backups of DAG files
- Read existing DAG before modifying
- Document changes in DAG description
- Use git for production DAGs

### Security

- Don't hardcode credentials in DAG files
- Use Airflow Variables for configuration
- Use Connections for external services
- Review DAG code before uploading

## Troubleshooting

### DAG Not Appearing in UI

After uploading a new DAG:
1. Wait 30-60 seconds for Airflow scheduler to detect it
2. Check DAG validation: `"Validate the content of my_dag.py"`
3. Look for syntax errors in scheduler logs
4. Refresh Airflow UI

### Upload Fails

If upload fails with "File exists":
```
"Upload my_dag.py with overwrite=true"
```

### Validation Errors

Fix validation errors before uploading:
- **Syntax Error**: Check Python indentation and syntax
- **Missing Imports**: Add required Airflow imports
- **No DAG Definition**: Ensure `with DAG(...)` or `DAG(...)` exists

## Example Sessions

### Session 1: Daily Report DAG

```
User: "I need to create a DAG that runs a daily report"
AI: [Creates basic DAG structure]

User: "Add the code to my_daily_report.py"
AI: [Uploads DAG file]

User: "Trigger a test run"
AI: [Triggers the DAG]

User: "Show me the status"
AI: [Shows DAG run status and task progress]
```

### Session 2: Debugging Failed Pipeline

```
User: "The ETL pipeline failed, what happened?"
AI: [Shows latest run status]

User: "Show logs for the failed task"
AI: [Displays task logs with error]

User: "Read the pipeline DAG file"
AI: [Shows DAG code]

User: "I see the issue, update the DAG with this fix: [paste code]"
AI: [Updates DAG file]

User: "Trigger another run"
AI: [Triggers new run]
```

## Tips

- Be specific with DAG names and task IDs
- Use natural language - the AI understands context
- You can ask for multiple operations in sequence
- The AI can help generate DAG code from requirements
- Always validate before uploading to production

---

For more information, see the [README](README.md) and [Apache Airflow documentation](https://airflow.apache.org/docs/).
