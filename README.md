# Cloud Cost Management Toolkit

This repo contains a collection of tools used for managing costs of Google Cloud Platform (GCP) account.

For broader conversation on use of these tools and on managing cloud costs see:

https://saveon.cloud

## Pre-requisites

Install required Python packages:

1) `pip install -U google-api-python-client`
2) `pip install -U oauth2client`
3) `pip install -U google-cloud-resource-manager`

Authenticate to gcloud with desired credentials:

4) `gcloud auth application-default login`

## Unattached Disks

Retrieves list of all GCP Persistent Disks that are not attached to any GCE instance.

View Help / Arguments:

`python3 gcp/unattached-disk-list.py -h`

Return CSV output of all unattached disks from all projects visible by authenticated account:

`python3 gcp/unattached-disk-list.py`
