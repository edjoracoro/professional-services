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
# test Andrew is awesome

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
report_part_url = base_url + "c.deaeece7-b8dc-402f-b91d-439f98391323"
report_base_url = report_part_url + "&r.reportName=MyBillboard"

std_proj_url = "&ds.ds93.connector=bigQuery&ds.ds93.projectId={}"
std_table_url = "&ds.ds93.type=TABLE&ds.ds93.datasetId={}&ds.ds93.tableId={}"
standard_view_url = std_proj_url + std_table_url

output_url = ""
isDetailedExportDifferentLocation = False
detailedBBDataset = ""

app_version = "3.0"

APPLICATION_NAME = "professional-services/billboard"
USER_AGENT = "{}/{}".format(APPLICATION_NAME, app_version)


# This is find code usage
#def get_http_client_info():
#    return http_client_info.ClientInfo(user_agent=USER_AGENT)


#bq_client = bigquery.Client(client_info=get_http_client_info())


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
    print(
        "To view dataset, please click " + Back.GREEN +
        "https://console.cloud.google.com/bigquery", "\n")

    print(Style.RESET_ALL)

    print("To launch datastudio report, please click " + Back.GREEN +
          output_url + "\n")
    print(Style.RESET_ALL)


def main(argv):

    global detailedBBDataset
    parser = argparse.ArgumentParser(
        description='Billing Export information, Version=' + app_version)
    parser.add_argument('-v',
                        action='version',
                        version='Version of %(prog)s ' + app_version)

    parser.add_argument('-pr',
                        dest='PROJECT_ID',
                        type=str,
                        help='Project Id',
                        required=True)
    parser.add_argument('-se',
                        dest='STANDARD_BILLING_EXPORT_DATASET_NAME',
                        type=str,
                        required=True)

    args = parser.parse_args()
    print('Version of billboard.py  ' + app_version + "\n")

    project_id_temp = "projects/{}".format(args.PROJECT_ID)

    # Check if billing api is enabled.
    # service = discovery.build('serviceusage', 'v1')
    # request = service.services().get(
    #     name=f"{project_id_temp}/services/cloudbilling.googleapis.com")
    # response = request.execute()
    # if response.get('state') == 'DISABLED':
    #     print("Cloud Billing API is not enabled.")
    #     return sys.exit(1)

    try:
        project_billing_info = billing.CloudBillingClient(
        ).get_project_billing_info(name=project_id_temp)
    except PermissionDenied as pde:
        print("Permission Denied, check project level permission.")
        print(pde.message)
        return sys.exit(1)

    args.standard_table = "bucket_detail"

    generate_datastudio_url(args)  # to create urls

# Main entry point
if __name__ == "__main__":
    main(sys.argv[1:])


#URL to test report directly
#https://lookerstudio.google.com/reporting/create?
# c.reportId=deaeece7-b8dc-402f-b91d-439f98391323
#  &ds.ds93.connector=bigQuery
#  &ds.ds93.type=TABLE
#  &ds.ds93.projectId=test-pgs1
#  &ds.ds93.datasetId=gcs_insight
#  &ds.ds93.tableId=event_detail
#  &ds.ds20.connector=bigQuery
#  &ds.ds20.type=TABLE
#  &ds.ds20.projectId=test-pgs1
#  &ds.ds20.datasetId=gcs_insight
#  &ds.ds20.tableId=bucket_detail