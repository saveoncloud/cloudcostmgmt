scriptDescription = '''\
    This script connects using Google API to obtain a list of GCP projects, along with helpful metadata.
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

# Query list of all GCP projects current user can see.
gcpProjects = get_gcp_projects(gcp_credentials, gcpSearchProject, debug)

projectOutput = []

for project in gcpProjects:
    #print(project)
    projectOutput.append({
        'projectName':gcpProjects[project]['name'],
        'projectId':gcpProjects[project]['projectId'],
        'projectNumber':gcpProjects[project]['projectNumber'],
        'createTime':gcpProjects[project]['createTime'],
        'lifecycleState':gcpProjects[project]['lifecycleState'],
        'projectCostCenter':gcpProjects[project]['labels'][projectCostCenterLabel] 
            if ('labels' in gcpProjects[project] 
                and projectCostCenterLabel in gcpProjects[project]['labels'])
                else None
    }) 

# Write to stdout as CSV
# TODO:  Parameterize output format
if (len(gcpProjects) > 0):
    diskWriter = csv.DictWriter(sys.stdout, projectOutput[0].keys())
    diskWriter.writeheader()
    diskWriter.writerows(projectOutput)
else:
    print("No Detached Disks Detected or Error has Occurred")