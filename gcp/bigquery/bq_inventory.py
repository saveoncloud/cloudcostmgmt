scriptDescription = '''
    This script enumerates specified projects and the BigQuery Datasets within those projects to collect inventory data.
    Account must have permissions: TBD 
    '''

# TODO: Specify permissions needed for script to execute sucessfully.

# Runs in 2 modes:
# 1) Command Line
#    - Optional list of projects, datasets, or tables to inventory
#    - JSON output
# TODO: 2) Cloud Function
#    - PubSub driven

# Requires:
# export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/my-key.json"
# pip install --upgrade google-cloud-bigquery
# https://googleapis.dev/python/bigquery/latest/index.html

# Load json output to bigQuery table via bq - see bq_table_inventory_load.sh for example.
# TODO: Stream findings into BigQuery table

# Provide Projects to inventory via command-line.
# TODO: Pull list of projects w/BigQuery API enabled dynamically, eliminate staticProjectIDs list

# Interesting BigQuery info to produce:
# ***TABLES***
# - Data Size
# - Data Rows
# - Created Date
# - Last Modified Date
# - Partition Column (If any)
# - Cluster Columns (If any)
# - Last Queried (from log sink audit data)
# - require_partition_filter
# - Table Description
# - Expiration Date (If any)
# - Data Location
# - InventoredTime  
# - Column Count
# - # of Data Partitions - https://cloud.google.com/bigquery/docs/creating-integer-range-partitions#getting_information_about_partitioned_tables
# - TODO: Table and/or Dataset Lable K/V pairs.  Define SPINS standard schema/keys?
# - TODO: Idenfity Materialized Views
# - TODO: Identify/group and/or handle Sharded tables...?
# - TODO: Identify table "Data Owner" User(s)/ServiceAccount(s) -> Most inherit, can determine if inherited from Dataset?
# ***DATASETS***
# TODO: Dataset inventory:  description, labels, default expirations, size, etc.
# ***VIEWS***
# TODO: View inventory...

debugOutput = False 

from google.cloud import bigquery
import json
import datetime
import pytz
import argparse

# Setup arguments for script CLI execution
parser = argparse.ArgumentParser(description=scriptDescription)
parser.add_argument('-p','--project', type=str, nargs='+',
                    help='GCP Project(s) to inventory, comma separated if multiple.')
parser.add_argument('-a','--auditDataset', type=str, nargs='+',
                    help='BigQuery View exposing BiqQuery Table Audit/Usage Events')
parser.add_argument('-v','--verbose', action='store_true',
                    help='Display progress throughout the script execution')

args = parser.parse_args()
if args.project:
    staticProjectIDs = args.project[0].split(',')
else:
    staticProjectIDs = None

if args.auditDataset:
    bqQueryAuditTable = args.auditDataset[0]
    
if args.verbose:
    debug = True
else:
    debug = False


client = bigquery.Client()

class DateTimeEncoder(json.JSONEncoder):
    def default(self, z):
        if isinstance(z, datetime.datetime):
            return (str(z))
        else:
            return super().default(z)

def get_table_access_log():
    # Pull Last Query Time for all BQ Tables from BQ Query Audit dataset
    if debugOutput: print("Retrieving Table Last Access")
    tableAccessQuery = f"""
        SELECT tables.projectId as p, tables.datasetId as d, tables.tableId as t, max(startTime) as st
        FROM `{bqQueryAuditTable}`
        GROUP BY p,d,t
        ORDER BY st DESC
    """
    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    tableAccessQueryJob = client.query(tableAccessQuery) 
    tableLastAccess = {} 

    for row in tableAccessQueryJob:
        tableLastAccess[f"{row['p']}:{row['d']}.{row['t']}"] = row['st']

    return tableLastAccess

def get_dataset_access_log():
    # Pull Last Query Time for all BQ Datasets from BQ Query Audit dataset
    if debugOutput: print("Retrieving Dataset Last Access")
    datasetAccessQuery = f"""
        SELECT tables.projectId as p, tables.datasetId as d, max(startTime) as st
        FROM `{bqQueryAuditTable}`
        GROUP BY p,d
        ORDER BY st DESC
    """
    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    datasetAccessQueryJob = client.query(datasetAccessQuery) 
    datasetLastAccess = {} 

    for row in datasetAccessQueryJob:
        datasetLastAccess[f"{row['p']}:{row['d']}"] = row['st']

    return datasetLastAccess

def get_gcp_projects():
    # Pull list of all projects we have access to
    if debugOutput: print("Retrieving GCP Projects")
    return staticProjectIDs

def get_datasets_from_project(projectID):
    # Pull list of all datasets in a project, and inventory their tables.
    if debugOutput: print(f"Getting datasets for project: {projectID}")
    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    datasetList = client.list_datasets(project=projectID) # API request

    datasets = []

    for dataset in datasetList:
        datasets.append(dataset)

    return datasets

def get_tables_from_dataset(projectID, datasetID):
    # Pull a list of all tables in the designated dataset, and inventory it.
    if debugOutput: print(f"Getting tables for project:dataset: {projectID}:{datasetID}")
    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference(projectID, datasetID)
    tableList = client.list_tables(dataset_ref) # API request

    tables = []

    for table in tableList:
        tables.append(table)

    return tables

def get_views_from_dataset(projectID, datasetID):
    return

def get_table_partition_count(projectId, datasetID, tableID):
    # Pull partition details for a given table 
    if debugOutput: print("Retrieving Table Partition Details")
    partitionQuery = f"""
        #legacySQL
        SELECT
          count(partition_id) as count
        FROM
          [{projectId}:{datasetID}.{tableID}$__PARTITIONS_SUMMARY__]
    """

    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(use_legacy_sql=True)
    partitionQueryJob = client.query(partitionQuery, job_config=job_config) 

    if partitionQueryJob.errors:
        return 0 
    else:
        for row in partitionQueryJob:
            partitionCount = row['count']

    return partitionCount

def inventory_table(projectID, datasetID, tableID):
    # Pull metadata for a specified Table, and return inventory record for it.
    if debugOutput: print(f"Getting tables metadata for project:dataset.table: {projectID}:{datasetID}.{tableID}")
    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    if 'tableLastAccess' in globals():
        global tableLastAccess
    else:
        global tableLastAccess
        tableLastAccess = get_table_access_log() 

    dataset_ref = bigquery.DatasetReference(projectID, datasetID)
    table_ref = dataset_ref.table(tableID)
    table = client.get_table(table_ref)  # API request

    tableTuple = f"{projectID}:{datasetID}.{table.table_id}"

    if table.time_partitioning or table.range_partitioning:
        tablePartitionCount = get_table_partition_count(projectID,datasetID,table.table_id)
    else:
        tablePartitionCount = None

    tableInventoryEntry = {
        'projectId':projectID,
        'datasetId':datasetID,
        'tableId':table.table_id,
        'creationTime':table.created,
        'lastModifiedTime':table.modified,
        'rowCount':table.num_rows,
        'columnCount':table.schema.__len__(),
        'sizeBytes':table.num_bytes,
        'timePartitioningField':table.time_partitioning.field if not table.time_partitioning == None else None,
        'timePartitioningType':table.time_partitioning.type_ if not table.time_partitioning == None else None,
        'rangePartitioningField':table.range_partitioning.field if not table.range_partitioning == None else None,
        'require_partition_filter':table.require_partition_filter,
        'clusterColumns':','.join(table.clustering_fields) if not table.clustering_fields == None else None,
        'tableLastAccess':tableLastAccess[tableTuple] if (tableTuple in tableLastAccess) else None,
        'dataLocation':table.location,
        'inventoriedTime':datetime.datetime.now(),
        'tableDescription':table.description,
        'tableExpires':table.expires,
        'partitionsExpire':table.partition_expiration,
        'partitionCount':tablePartitionCount,
        'tableLink':f'https://console.cloud.google.com/bigquery?project={projectID}&p={projectID}&d={datasetID}&t={table.table_id}&page=table'
    }

    # Remove "None" values
    tableInventoryEntry = dict(filter(lambda item: item[1] is not None, tableInventoryEntry.items()))

    print(json.dumps(tableInventoryEntry,cls=DateTimeEncoder))
    return

def inventory_dataset(projectID, datasetID):
    # Pull metadata for a specified Table, and return inventory record for it.
    if debugOutput: print(f"Getting dataset metadata for project:dataset: {projectID}:{datasetID}")
    if 'client' in globals():
        global client
    else:
        client = bigquery.Client()

    if 'datasetLastAccess' in globals():
        global datasetLastAccess
    else:
        global datasetLastAccess
        datasetLastAccess = get_dataset_access_log() 

    dataset_ref = bigquery.DatasetReference(projectID, datasetID)
    dataset = client.get_dataset(dataset_ref)  # API request

    datasetPair = f"{projectID}:{datasetID}"

    owner = []
    for ace in dataset.access_entries:
        if ace.entity_type == 'userByEmail' and ace.role == 'OWNER':
            owner.append(ace.entity_id)
    owner.sort()

    datasetInventoryEntry = {
        'projectId':projectID,
        'datasetId':dataset.dataset_id,
        'creationTime':dataset.created,
        'lastModifiedTime':dataset.modified,
        'datasetLastAccess':datasetLastAccess[datasetPair] if (datasetPair in datasetLastAccess) else None,
        'dataLocation':dataset.location,
        'inventoriedTime':datetime.datetime.now(),
        'datasetDescription':dataset.description,
        'datasetDefaultTableExpiration':dataset.default_table_expiration_ms,
        'datasetDefaultPartitionExpiration':dataset.default_partition_expiration_ms,
        'owner':",".join(owner),
        'costCenter':dataset.labels['cost-center'] if 'cost-center' in dataset.labels else None,
        'datasetLink':f'https://console.cloud.google.com/bigquery?project={projectID}&p={projectID}&d={datasetID}&page=dataset'
    }

    # Remove "None" values
    datasetInventoryEntry = dict(filter(lambda item: item[1] is not None, datasetInventoryEntry.items()))
    print(dataset.labels)
    print(json.dumps(datasetInventoryEntry,cls=DateTimeEncoder))
    return

def inventory_view(projectID, datasetID, viewID):
    return

### Workflow Functions

def inventory_project_views(projectID):
    projDatasets = get_datasets_from_project(projectID)
    for dataset in projDatasets:
        inventory_dataset_tables(projectID, dataset.dataset_id)

    return

def inventory_project_datasets(projectID):
    projDatasets = get_datasets_from_project(projectID)
    for dataset in projDatasets:
        inventory_dataset(projectID, dataset.dataset_id)

    return

def inventory_project_tables(projectID):
    projDatasets = get_datasets_from_project(projectID)
    for dataset in projDatasets:
        inventory_dataset_tables(projectID, dataset.dataset_id)

    return

def inventory_dataset_tables(projectID, datasetID):
    datasetTables = get_tables_from_dataset(projectID, datasetID)
    for table in datasetTables:
        inventory_table(projectID, datasetID, table.table_id)

    return
    
def inventory_dataset_views(projectID, datasetID):
    datasetTables = get_views_from_dataset(projectID, datasetID)
    for views in datasetTables:
        inventory_view(projectID, dataset.dataset_id, view.table_id)

    return

if __name__ == '__main__':
    # Get List of GCP Projects to inventory
    projectIDs = get_gcp_projects() 

    # Enumerate projects, pull dataset and table metadata
    for project in projectIDs:
        #inventory_project_tables(project)
        inventory_project_datasets(project)