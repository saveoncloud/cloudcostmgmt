
scriptDescription = '''\
    This script connects using Google API to obtain a list of GCE disks that are unused / not attached to a GCE instance.
    Requires Google Application credentials set, e.g.:
    gcloud auth application-default login
    Account must have Project Viewer access.
    '''
# TODO: Define more restrictive permission needs to script execution

from oauth2client.client import GoogleCredentials
#from google.cloud import resource_manager
from gcp_shared.utils import get_gcp_disks, get_gcp_projects
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
                    help='Specific GCP Project to retrieve disks from')
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


def filter_detached_gcp_disks(gcpDisks, gcpProjects):
    detachedDisks = []
    for project in gcpDisks: 
        for location in gcpDisks[project]: 
            for disk in gcpDisks[project][location][0]: 
                if not 'users' in disk: 
                    detachedDisks.append({
                        'projectId':project,
                        'location':location,
                        'name':disk['name'],
                        'creationTimestamp':disk['creationTimestamp'],
                        'projectCostCenter':gcpProjects[project]['labels'][projectCostCenterLabel] 
                            if ('labels' in gcpProjects[project] 
                                and projectCostCenterLabel in gcpProjects[project]['labels'])
                                else None,
                        'diskCostCenter':disk['labels'][resourceCostCenterLabel]
                            if ('labels' in disk
                                and resourceCostCenterLabel in disk['labels'])
                                else None,
                        'sizeGb':disk['sizeGb'],
                        'lastDetachTimestamp':disk['lastDetachTimestamp'] if ('lastDetachTimestamp' in disk) else None,
                        'type':disk['type'].split("/")[len(disk['type'].split("/"))-1],
                        'consoleUrl':"https://console.cloud.google.com/compute/disksDetail/{0}/disks/{1}?project={2}".format(location,disk['name'],project),
                        #'selfLink':disk['selfLink']
                    })

                    if (debug): print("Found detatched disk:", disk['name'])
    return detachedDisks
                    
# Query list of all GCP projects current user can see.
gcpProjects = get_gcp_projects(gcp_credentials, gcpSearchProject, debug)

# Get list of disks in all projects
gcpDisks = get_gcp_disks(gcp_credentials, gcpProjects, debug)

# Filter and format view of only detached disks
detachedGcpDisks = filter_detached_gcp_disks(gcpDisks, gcpProjects)

# TODO:  Get GCP Cost Info
# https://cloud.google.com/billing/v1/how-tos/catalog-api#getting_the_list_of_skus_for_a_service
# "type": "https://www.googleapis.com/compute/v1/projects/spins-itops-testbox/zones/us-central1-a/diskTypes/pd-standard",
# Currently no way to associate a Disk resource to a SKU for billing/pricing purposes.

# Write to stdout as CSV
# TODO:  Parameterize output format
if (len(detachedGcpDisks) > 0):
    diskWriter = csv.DictWriter(sys.stdout, detachedGcpDisks[0].keys())
    diskWriter.writeheader()
    diskWriter.writerows(detachedGcpDisks)
else:
    print("No Detached Disks Detected or Error has Occurred")