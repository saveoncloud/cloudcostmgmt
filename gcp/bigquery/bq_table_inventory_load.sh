# Load BQ Table inventory into BigQuery

# Use:

# bq_table_inventory_load gcp_project:dataset_id.table_id gs://gcp_project/file.json

# $1 = BigQuery Table e.g.: {gcp_project}:{dataset_id}.{table_id}
# $2 = GCS URI to Inventory data e.g.: gs://{gcs_bucket}/{file}

bq load \
 --clustering_fields projectId,datasetId,tableId \
 --source_format=NEWLINE_DELIMITED_JSON \
 $1 \
 $2 \
 projectId:STRING,datasetId:STRING,tableId:STRING,creationTime:TIMESTAMP,lastModifiedTime:TIMESTAMP,rowCount:INTEGER,columnCount:INTEGER,sizeBytes:INTEGER,timePartitioningField:STRING,timePartitioningType:STRING,rangePartitioningField:STRING,require_partition_filter:STRING,clusterColumns:STRING,tableLastAccess:TIMESTAMP,dataLocation:STRING,inventoriedTime:TIMESTAMP,tableDescription:STRING,tableExpires:TIMESTAMP,partitionsExpire:TIMESTAMP,tableLink:STRING,,partitionCount:INTEGER