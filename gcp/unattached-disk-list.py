
scriptDescription = '''\
    This script connects using Google API to obtain a list of GCE disks that are unused / not attached to a GCE instance.
    Requires Google Application credentials set, e.g.:
    gcloud auth application-default login
    Account must have Project Viewer access.
    '''
# TODO: Define more restrictive permission needs to script execution

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from google.cloud import resource_manager
import sys
import csv
import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
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


def get_gcp_projects(projId=None):
    if (debug): print("Getting GCP Projects")
    gcpProjects = {} 

    if projId:
        projFilter = f'id: {projId}'
    else:
        projFilter = None

    # Method #1
    service = discovery.build('cloudresourcemanager', 'v1', credentials=gcp_credentials)
    # TODO: Accept parameter for full filter per https://googleapis.dev/python/cloudresourcemanager/0.30.0/client.html
    request = service.projects().list(filter=projFilter)
    while request is not None:
        response = request.execute()
        for project in response.get('projects', []):
            gcpProjects.update({project['projectId']: project})
        request = service.projects().list_next(previous_request=request, previous_response=response)

    #if (len(gcpProjects) < 1):
    # Method #2
    # TODO: Accept parameter for full filter per https://googleapis.dev/python/cloudresourcemanager/0.30.0/client.html
    #client = resource_manager.Client()
    #for project in client.list_projects(projFilter or None):
    #    gcpProjects.update({project['projectId']: project})

    return gcpProjects

def get_gcp_disks(gcpProject):
    # gcpProject = dict containing Project metadata from get_gcp_projects function
    gcpDisks = {}

    for project in gcpProject:
        if (debug): print("Getting Disks for GCP Project: ", project)
        service = discovery.build('compute', 'v1', credentials=gcp_credentials)
        request = service.disks().aggregatedList(project=project)
        projectDisks = {}
        while request is not None:
            try:
                response = request.execute()
            except:
                if sys.exc_info()[1].args[0].status != 403 and sys.exc_info()[1].args[0].status != 404:
                    print("Error: ", sys.exc_info()[1])
                request = None
            else:
                for location in response['items']:
                    if 'disks' in response['items'][location]:
                        projectDisks.update({location: [response['items'][location]['disks']]})
                        if (debug): print("Found ", len(response['items'][location]['disks'])," disks in ", location)

                request = service.disks().list_next(previous_request=request, previous_response=response)

        if (len(projectDisks)>0):
            gcpDisks.update({project:projectDisks})

    return(gcpDisks)

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
gcpProjects = get_gcp_projects(gcpSearchProject)

# Get list of disks in all projects
gcpDisks = get_gcp_disks(gcpProjects)

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