from googleapiclient import discovery
import sys
import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


def get_gcp_projects(gcp_credentials, projId=None, debug=False):
    #if (debug): print("Getting GCP Projects")
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

def get_gcp_disks(gcp_credentials, gcpProject, debug=False):
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

def get_gcp_ips(gcp_credentials, gcpProject, debug=False):
    # gcpProject = dict containing Project metadata from get_gcp_projects function
    gcpIPs = {}

    for project in gcpProject:
        if (debug): print("Getting IPs for GCP Project: ", project)
        service = discovery.build('compute', 'v1', credentials=gcp_credentials)
        request = service.addresses().aggregatedList(project=project)
        projectIPs = {}
        while request is not None:
            try:
                response = request.execute()
            except:
                if sys.exc_info()[1].args[0].status != 403 and sys.exc_info()[1].args[0].status != 404:
                    print("Error: ", sys.exc_info()[1])
                request = None
            else:
                for location in response['items']:
                    if 'addresses' in response['items'][location]:
                        projectIPs.update({location: [response['items'][location]['addresses']]})
                        if (debug): print("Found ", len(response['items'][location]['addresses'])," IPs in ", location)

                request = service.addresses().list_next(previous_request=request, previous_response=response)

        if (len(projectIPs)>0):
            gcpIPs.update({project:projectIPs})

    return(gcpIPs)

