# Load BQ Table inventory into BigQuery

# Use:

# bq_table_inventory_load gcp_project:dataset_id.table_id gs://gcp_project/file.json

# $1 = BigQuery Table e.g.: {gcp_project}:{dataset_id}.{table_id}
# $2 = GCS URI to Inventory data e.g.: gs://{gcs_bucket}/{file}

bq load \
 --clustering_fields projectId,datasetId \
 --source_format=NEWLINE_DELIMITED_JSON \
 $1 \
 $2 \
 projectId:STRING,datasetId:STRING,creationTime:TIMESTAMP, \
 lastModifiedTime:TIMESTAMP,datasetLastAccess:TIMESTAMP, \
 dataLocation:STRING,inventoriedTime:TIMESTAMP, \
 datasetDescription:STRING,datasetDefaultTableExpiration:TIMESTAMP, \
 datasetDefaultPartitionExpiration:TIMESTAMP,
 owner:STRING,datasetLink:STRING