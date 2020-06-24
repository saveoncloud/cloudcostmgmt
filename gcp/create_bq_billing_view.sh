#!/bin/bash

if [ -z "${PROJECT}" -o -z "${DATASET}" ]; then
  echo "Please set \$PROJECT and \$DATASET."
  exit 1
fi

IFS='' read -d '' Q <<EOF
#standardSQL
/*
 * Script: GCP Billing Export 
 * Author: jeremyrcampb
 * Source: https://github.com/jeremyrcampb/cloudcostmgmt/
 * Description: 
 * 
 * Creates a user friendly view for querying the
 * Google Cloud Platform Billing export from BigQuery.
 */
Select 
  CASE
     WHEN (SELECT value FROM UNNEST(labels) WHERE key = 'cost-center') != '' THEN (SELECT value FROM UNNEST(labels) WHERE key = 'cost-center')
     ELSE (SELECT value FROM UNNEST(project.labels) WHERE key = 'cost-center')
  END AS cost_center,
  (SELECT SUM(amount) FROM UNNEST(credits)) as total_credits,
  *
FROM 
  \`$PROJECT.$DATASET.gcp_billing_export_*\`;
EOF

bq mk --project_id $PROJECT --view="${Q}" $DATASET.gcp_billing