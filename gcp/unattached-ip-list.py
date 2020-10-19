
scriptDescription = '''\
    This script connects using Google API to obtain a list of GCE IP addresses that are unused / not attached to a GCE instance.
    Requires Google Application credentials set, e.g.:
    gcloud auth application-default login
    Account must have Project Viewer access.
    '''
# TODO: Define more restrictive permission needs to script execution

from oauth2client.client import GoogleCredentials
from gcp_shared.utils import get_gcp_ips, get_gcp_projects
import sys
import csv
import argparse

# TODO: Parameterize Cost Center label 
projectCostCenterLabel = 'cost-center'
resourceCostCenterLabel = projectCostCenterLabel 

# Obtain credentials from GOOGLE_APPLICATION_CREDENTIALS set via gcloud auth application-default login
# TODO: Revert to interactive user login when not set
gcp_credentials = GoogleCredentials.get_application_default()

# Setup arguments for script CLI execution
parser = argparse.ArgumentParser(description=scriptDescription)
parser.add_argument('-p','--project', type=str, nargs='+',
                    help='Specific GCP Project to retrieve IPs from')
parser.add_argument('-v','--verbose', action='store_true',
                    help='Display progress throughout the script execution')

args = parser.parse_args()
if args.project:
    gcpSearchProject = args.project
else:
    gcpSearchProject = None
    
if args.verbose:
    debug = True
else:
    debug = False


def filter_unused_external_gcp_addresses(gcpIPs, gcpProjects):
    detachedIPs = []
    for project in gcpIPs: 
        for location in gcpIPs[project]: 
            for ip in gcpIPs[project][location][0]: 
                if not 'users' in ip and ip['addressType'] == "EXTERNAL": 
                    detachedIPs.append({
                        'projectId':project,
                        'location':location,
                        'id':ip['id'],
                        'description':ip['description'],
                        'status':ip['status'],
                        'address':ip['address'],
                        'name':ip['name'],
                        'creationTimestamp':ip['creationTimestamp'],
                        'projectCostCenter':gcpProjects[project]['labels'][projectCostCenterLabel] 
                            if ('labels' in gcpProjects[project] 
                                and projectCostCenterLabel in gcpProjects[project]['labels'])
                                else None,
                        'networkTier':ip['networkTier'],
                    })

                    if (debug): print("Found detatched ip:", ip['name'])
    return detachedIPs
                    
# Query list of all GCP projects current user can see.
gcpProjects = get_gcp_projects(gcp_credentials, gcpSearchProject, debug)

# Get list of IPs in all projects
gcpIPs = get_gcp_ips(gcp_credentials, gcpProjects, debug)

# Filter and format view of only detached IPs 
detachedGcpIPs = filter_unused_external_gcp_addresses(gcpIPs, gcpProjects)

# TODO:  Get GCP Cost Info
# https://cloud.google.com/billing/v1/how-tos/catalog-api#getting_the_list_of_skus_for_a_service
# "type": "https://www.googleapis.com/compute/v1/projects/spins-itops-testbox/zones/us-central1-a/diskTypes/pd-standard",
# Currently no way to associate a IP resource to a SKU for billing/pricing purposes.

# Write to stdout as CSV
# TODO:  Parameterize output format
if (len(detachedGcpIPs) > 0):
    outputWriter = csv.DictWriter(sys.stdout, detachedGcpIPs[0].keys())
    outputWriter.writeheader()
    outputWriter.writerows(detachedGcpIPs)
else:
    print("No Detached IPs Detected or Error has Occurred")