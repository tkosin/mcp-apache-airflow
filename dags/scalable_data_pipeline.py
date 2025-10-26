"""
Scalable Data Pipeline DAG
Demonstrates best practices for large-scale data ingestion and ETL processing

Features:
- Dynamic task generation based on data partitions
- Parallel processing using dynamic task mapping
- Data chunking and partitioning
- TaskGroups for better organization
- Comprehensive error handling and monitoring
- XCom for data passing between tasks
"""

from datetime import datetime, timedelta
import random
import time
from typing import Dict, List, Any
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# DAG configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

# Pipeline configuration
NUM_PARTITIONS = 10  # Number of data partitions for parallel processing
RECORDS_PER_PARTITION = 1000  # Records to process per partition
BATCH_SIZE = 100  # Batch size for chunked processing


@task
def generate_partitions() -> List[Dict[str, Any]]:
    """
    Generate partition metadata for parallel processing
    This simulates identifying data sources/files to process
    """
    print(f"🔍 Generating {NUM_PARTITIONS} data partitions...")
    
    partitions = []
    for i in range(NUM_PARTITIONS):
        partition = {
            'partition_id': i,
            'partition_name': f'partition_{i:03d}',
            'record_count': RECORDS_PER_PARTITION,
            'source_path': f's3://data-lake/raw/partition_{i:03d}.parquet',
            'start_offset': i * RECORDS_PER_PARTITION,
            'end_offset': (i + 1) * RECORDS_PER_PARTITION,
        }
        partitions.append(partition)
    
    print(f"✅ Generated {len(partitions)} partitions")
    print(f"📊 Total records to process: {NUM_PARTITIONS * RECORDS_PER_PARTITION:,}")
    
    return partitions


@task
def extract_partition_data(partition: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract data from a single partition
    Simulates reading from data source (database, file, API)
    """
    partition_id = partition['partition_id']
    print(f"📥 Extracting partition {partition_id}: {partition['partition_name']}")
    print(f"   Source: {partition['source_path']}")
    print(f"   Records: {partition['record_count']:,}")
    
    # Simulate data extraction with some processing time
    time.sleep(random.uniform(0.1, 0.3))
    
    # Simulate extracted raw data
    extracted_data = {
        'partition_id': partition_id,
        'partition_name': partition['partition_name'],
        'records': [
            {
                'id': partition['start_offset'] + j,
                'value': random.randint(100, 1000),
                'category': random.choice(['A', 'B', 'C', 'D']),
                'timestamp': datetime.now().isoformat(),
            }
            for j in range(min(10, partition['record_count']))  # Sample for demo
        ],
        'total_records': partition['record_count'],
        'extraction_time': datetime.now().isoformat(),
    }
    
    print(f"✅ Extracted {extracted_data['total_records']:,} records from partition {partition_id}")
    
    return extracted_data


@task
def transform_partition_data(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform data from a single partition
    Applies business logic, data cleaning, enrichment
    """
    partition_id = extracted_data['partition_id']
    print(f"⚙️  Transforming partition {partition_id}: {extracted_data['partition_name']}")
    
    # Simulate data transformation with batching
    total_records = extracted_data['total_records']
    num_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
    
    transformed_records = []
    for i in range(num_batches):
        # Simulate batch processing
        time.sleep(random.uniform(0.05, 0.15))
        batch_records = min(BATCH_SIZE, total_records - i * BATCH_SIZE)
        transformed_records.extend([
            {
                'id': record['id'],
                'value': record['value'] * 1.1,  # Apply transformation
                'category': record['category'],
                'is_high_value': record['value'] > 500,
                'processed_at': datetime.now().isoformat(),
            }
            for record in extracted_data['records'][:batch_records]
        ])
    
    # Calculate statistics
    values = [r['value'] for r in extracted_data['records']]
    stats = {
        'avg_value': sum(values) / len(values) if values else 0,
        'max_value': max(values) if values else 0,
        'min_value': min(values) if values else 0,
        'high_value_count': sum(1 for r in extracted_data['records'] if r['value'] > 500),
    }
    
    transformed_data = {
        'partition_id': partition_id,
        'partition_name': extracted_data['partition_name'],
        'records': transformed_records[:10],  # Sample for demo
        'total_records': total_records,
        'batches_processed': num_batches,
        'statistics': stats,
        'transformation_time': datetime.now().isoformat(),
    }
    
    print(f"✅ Transformed {total_records:,} records in {num_batches} batches")
    print(f"   Statistics: avg={stats['avg_value']:.2f}, max={stats['max_value']}, min={stats['min_value']}")
    print(f"   High-value records: {stats['high_value_count']}")
    
    return transformed_data


@task
def validate_partition_data(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate transformed data quality
    Checks data integrity, completeness, business rules
    """
    partition_id = transformed_data['partition_id']
    print(f"✓ Validating partition {partition_id}: {transformed_data['partition_name']}")
    
    # Simulate validation checks
    validation_results = {
        'partition_id': partition_id,
        'partition_name': transformed_data['partition_name'],
        'total_records': transformed_data['total_records'],
        'checks': {
            'completeness': True,
            'data_types': True,
            'business_rules': True,
            'null_check': True,
        },
        'errors': [],
        'warnings': [],
        'validation_time': datetime.now().isoformat(),
    }
    
    # Simulate some validation warnings
    if random.random() < 0.2:  # 20% chance of warning
        validation_results['warnings'].append(f"Partition {partition_id}: Some records have edge-case values")
    
    if validation_results['errors']:
        print(f"❌ Validation failed for partition {partition_id}")
        raise ValueError(f"Validation errors: {validation_results['errors']}")
    
    print(f"✅ Validation passed for partition {partition_id}")
    if validation_results['warnings']:
        print(f"⚠️  Warnings: {len(validation_results['warnings'])}")
    
    return validation_results


@task
def load_partition_data(transformed_data: Dict[str, Any], validation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load transformed data to destination
    Simulates writing to data warehouse, database, or data lake
    """
    partition_id = transformed_data['partition_id']
    print(f"💾 Loading partition {partition_id}: {transformed_data['partition_name']}")
    
    # Simulate loading with batching
    total_records = transformed_data['total_records']
    num_batches = transformed_data['batches_processed']
    
    for i in range(num_batches):
        # Simulate batch loading
        time.sleep(random.uniform(0.05, 0.1))
    
    load_result = {
        'partition_id': partition_id,
        'partition_name': transformed_data['partition_name'],
        'records_loaded': total_records,
        'batches_loaded': num_batches,
        'destination': f'warehouse://analytics/data/partition_{partition_id:03d}',
        'load_time': datetime.now().isoformat(),
        'statistics': transformed_data['statistics'],
    }
    
    print(f"✅ Loaded {total_records:,} records to {load_result['destination']}")
    
    return load_result


@task
def aggregate_results(load_results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate results from all partitions
    Calculates overall statistics and metrics
    """
    print("📊 Aggregating results from all partitions...")
    
    total_records = sum(r['records_loaded'] for r in load_results)
    total_batches = sum(r['batches_loaded'] for r in load_results)
    
    # Aggregate statistics
    all_stats = [r['statistics'] for r in load_results]
    aggregated_stats = {
        'total_records_processed': total_records,
        'total_partitions': len(load_results),
        'total_batches': total_batches,
        'avg_records_per_partition': total_records / len(load_results) if load_results else 0,
        'overall_avg_value': sum(s['avg_value'] for s in all_stats) / len(all_stats) if all_stats else 0,
        'overall_max_value': max(s['max_value'] for s in all_stats) if all_stats else 0,
        'overall_min_value': min(s['min_value'] for s in all_stats) if all_stats else 0,
        'total_high_value_records': sum(s['high_value_count'] for s in all_stats),
    }
    
    print(f"✅ Pipeline completed successfully!")
    print(f"   Total records processed: {total_records:,}")
    print(f"   Total partitions: {len(load_results)}")
    print(f"   Total batches: {total_batches}")
    print(f"   Average value: {aggregated_stats['overall_avg_value']:.2f}")
    print(f"   High-value records: {aggregated_stats['total_high_value_records']:,}")
    
    return aggregated_stats


@task
def send_completion_notification(aggregated_stats: Dict[str, Any], **context) -> str:
    """
    Send notification about pipeline completion
    """
    dag_run_id = context['dag_run'].run_id
    execution_date = context['execution_date']
    
    message = f"""
    🎉 Scalable Data Pipeline Completed!
    
    📅 Execution Date: {execution_date}
    🆔 DAG Run ID: {dag_run_id}
    
    📊 Processing Summary:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    ✓ Total Records: {aggregated_stats['total_records_processed']:,}
    ✓ Partitions Processed: {aggregated_stats['total_partitions']}
    ✓ Batches Processed: {aggregated_stats['total_batches']}
    ✓ Avg Records/Partition: {aggregated_stats['avg_records_per_partition']:.0f}
    
    📈 Data Statistics:
    ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    • Average Value: {aggregated_stats['overall_avg_value']:.2f}
    • Max Value: {aggregated_stats['overall_max_value']:.2f}
    • Min Value: {aggregated_stats['overall_min_value']:.2f}
    • High-Value Records: {aggregated_stats['total_high_value_records']:,}
    
    🚀 Pipeline Pattern: Dynamic Task Mapping + Parallel Processing
    """
    
    print(message)
    return message


# Create the DAG
with DAG(
    dag_id='scalable_data_pipeline',
    default_args=default_args,
    description='Scalable data pipeline with dynamic task generation and parallel processing',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scalable', 'etl', 'parallel', 'production'],
    max_active_runs=1,
) as dag:
    
    # Start marker
    start = BashOperator(
        task_id='start',
        bash_command='echo "🚀 Starting Scalable Data Pipeline..."',
    )
    
    # Generate partitions
    partitions = generate_partitions()
    
    # Process each partition in parallel using dynamic task mapping
    # Extract → Transform → Validate → Load (all happen in parallel per partition)
    extracted = extract_partition_data.expand(partition=partitions)
    transformed = transform_partition_data.expand(extracted_data=extracted)
    validated = validate_partition_data.expand(transformed_data=transformed)
    loaded = load_partition_data.expand(
        transformed_data=transformed,
        validation=validated
    )
    
    # Aggregate all results
    aggregated = aggregate_results(loaded)
    
    # Send notification
    notification = send_completion_notification(aggregated)
    
    # End marker
    end = BashOperator(
        task_id='end',
        bash_command='echo "✅ Scalable Data Pipeline Completed!"',
    )
    
    # Define task dependencies
    start >> partitions >> extracted >> transformed >> validated >> loaded >> aggregated >> notification >> end
