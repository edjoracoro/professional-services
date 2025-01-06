# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# from googleapiclient import discovery
from google.cloud import bigquery
from google.cloud import billing
from google.api_core import client_info as http_client_info
from google.api_core.exceptions import PermissionDenied
from google.cloud.exceptions import NotFound
import argparse
import sys
from colorama import Back
from colorama import Style

base_url = "https://lookerstudio.google.com/reporting/create?"
report_part_url = base_url + "c.reportId=deaeece7-b8dc-402f-b91d-439f98391323"
report_base_url = report_part_url + "&r.reportName=MyBillboard"

std_proj_url = "&ds.ds1.connector=bigQuery&ds.ds1.projectId={}"
std_table_url = "&ds.ds1.type=TABLE&ds.ds1.datasetId={}&ds.ds1.tableId={}"
std_proj_url2 = "&ds.ds2.connector=bigQuery&ds.ds2.projectId={}"
std_table_url2 = "&ds.ds2.type=TABLE&ds.ds2.datasetId={}&ds.ds2.tableId={}"
std_proj_url3 = "&ds.ds3.connector=bigQuery&ds.ds3.projectId={}"
std_table_url3 = "&ds.ds3.type=TABLE&ds.ds3.datasetId={}&ds.ds3.tableId={}"
std_proj_url4 = "&ds.ds4.connector=bigQuery&ds.ds4.projectId={}"
std_table_url4 = "&ds.ds4.type=TABLE&ds.ds4.datasetId={}&ds.ds4.tableId={}"

standard_view_url = std_proj_url + std_table_url
standard_view_url2 = std_proj_url2 + std_table_url2
standard_view_url3 = std_proj_url3 + std_table_url3
standard_view_url4 = std_proj_url4 + std_table_url4

final_url = standard_view_url + standard_view_url2 + standard_view_url3 + standard_view_url4 


output_url = ""
isDetailedExportDifferentLocation = False
detailedBBDataset = ""

app_version = "3.0"

APPLICATION_NAME = "professional-services/billboard"
USER_AGENT = "{}/{}".format(APPLICATION_NAME, app_version)


# This is find code usage
def get_http_client_info():
    return http_client_info.ClientInfo(user_agent=USER_AGENT)

bq_client = bigquery.Client(project="test-pgs1")


# This function checks if billboard dataset already exists or not
# so that we are not recreating it
def check_billboard_dataset_exists(dataset_id):
    try:
        bq_client.get_dataset(dataset_id)  # Make an API request.
        print("Dataset {} already exists.".format(dataset_id))
        return True
    except NotFound:
        print("Dataset {} is not found.".format(dataset_id))
        return False


def generate_datastudio_url(args):
    table_ids = []
    projectId = args.PROJECT_ID
    dataset_id = "{}.{}".format(args.PROJECT_ID,
                                args.DATASET_NAME)
    tables = bq_client.list_tables(dataset_id)
    print("Tables contained in '{}':".format(dataset_id))
    for table in tables:
        table_id = "{}".format(table.table_id)
        print(table_id)

        table_ids.append(table_id.lower())
    
    bucket = ''
    events = ''
    error = ''
    objecta = ''
    project = ''
    for i, item in enumerate(table_ids):
        if "bucket" in item:
            bucket = item # Remove and return the item
        elif "events" in item:
            events = item
        #elif "project" in item:
        #    project = item
        elif "error" in item:
            error = item
        elif "object" in item:
            objecta = item
    
    output_url = report_base_url + final_url.format(projectId, dataset_id, events,
    projectId, dataset_id, bucket, projectId, dataset_id, error, projectId, dataset_id, objecta)

    print("\n" + "To launch datastudio report, please click the following link:" + "\n" +  Back.GREEN +
          output_url + "\n")
    print(Style.RESET_ALL)


def main(argv):

    global detailedBBDataset
    parser = argparse.ArgumentParser(
        description='Billing Export information, Version=' + app_version)
    parser.add_argument('-v',
                        action='version',
                        version='Version of %(prog)s ' + app_version)

    parser.add_argument('-project',
                        dest='PROJECT_ID',
                        type=str,
                        help='Project Id',
                        required=True)
    parser.add_argument('-dataset',
                        dest='DATASET_NAME',
                        type=str,
                        required=True)
    #parser.add_argument('-table',
    #                    dest='TABLE_NAME',
    #                    type=str,
    #                    required=True)

    args = parser.parse_args()
    print('Version of GCS_Insight.py  ' + app_version + "\n")

    project_id_temp = "projects/{}".format(args.PROJECT_ID)

    try:
        project_billing_info = billing.CloudBillingClient(
        ).get_project_billing_info(name=project_id_temp)
    except PermissionDenied as pde:
        print("Permission Denied, check project level permission.")
        print(pde.message)
        return sys.exit(1)

    args.standard_table = "bucket_detail"
    output_url = report_base_url 
    
   
    # This is creates the views ofr now we will focus on just dashboard
    #create_view(args)
    
    #list_tables(args)
    generate_datastudio_url(args)  # to create urls
    

# Main entry point
if __name__ == "__main__":
    main(sys.argv[1:])
