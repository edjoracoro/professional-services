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
# from google.cloud import billing
from google.api_core import client_info as http_client_info
from google.api_core.exceptions import PermissionDenied
from google.cloud.exceptions import NotFound
import argparse
import sys
from colorama import Back
from colorama import Style

base_url = "https://lookerstudio.google.com/reporting/create?"
report_part_url = base_url + "c.reportId=9099eb08-edaf-4107-b0dd-b4a08cd19df6"
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

bq_client = bigquery.Client(client_info=get_http_client_info())


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

def create_bq_views(args):
    
    table_ids = []
    projectId = args.PROJECT_ID
    dataset = "{}".format(args.DATASET_NAME)
    dataset_id = "{}.{}".format(args.PROJECT_ID,
                                args.DATASET_NAME)
    tables = bq_client.list_tables(dataset_id)
    dataset_simple = "{}".format(args.DATASET_NAME)
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
            bucket = item 
        elif "events" in item:
            events = item
        elif "project" in item:
            project = item
        elif "error" in item:
            error = item
        elif "object" in item:
            objecta = item

    # Configure and Create views
    # Create View for Events
    view_id_events = "{}.{}.event_view_looker".format(args.PROJECT_ID, args.DATASET_NAME)
    view_events = bigquery.Table(view_id_events)

    # The SQL query to be executed for events.
    table_name = "{}.{}".format(dataset, events)
    view_events.view_query = r"""
        WITH events_view AS (
            SELECT
                *
            FROM
                {}
        ), regions_information AS (
                WITH location_set AS (SELECT 'africa-south1' AS location, 'Africa' AS geographic_area, 'Region' AS location_type, 'South Africa' AS countries, 'Gauteng' AS state, 'Johannesburg' AS city, -26.206619 AS latitude, 28.031437 AS longitude
            UNION ALL
            SELECT 'asia', 'Asia', 'Multi-Region', 'Taiwan, Japan, South Korea, Singapore', 'Taiwan Province, Kanto, Kansai, Seoul Capital, West Region', 'Changhua County, Tokyo, Osaka, Seoul, Jurong West', 27.407598, 124.302272
            UNION ALL
            SELECT 'asia-east1', 'Asia', 'Region', 'Taiwan', 'Taiwan Province', 'Changhua County', 24.04955, 120.516007
            UNION ALL
            SELECT 'asia-east1, asia-southeast1', 'Asia', 'Dual-Region', 'Taiwan, Singapore', 'Taiwan Province, West Region', 'Changhua County, Jurong West', 12.827841, 111.729625
            UNION ALL
            SELECT 'asia-east2', 'Asia', 'Region', 'China', 'Hong Kong', 'Hong Kong', 22.324061, 114.171655
            UNION ALL
            SELECT 'asia-northeast1', 'Asia', 'Region', 'Japan', 'Kanto', 'Tokyo', 35.673817, 139.65123
            UNION ALL
            SELECT 'asia-northeast2', 'Asia', 'Region', 'Japan', 'Kansai', 'Osaka', 34.693925, 135.500077
            UNION ALL
            SELECT 'asia-northeast3', 'Asia', 'Region', 'South Korea', 'Seoul Capital', 'Seoul', 37.552242, 126.994724
            UNION ALL
            SELECT 'asia-south1', 'India', 'Region', 'India', 'Maharashtra', 'Mumbai', 19.07439, 72.878422
            UNION ALL
            SELECT 'asia-south1, asia-south2', 'India', 'Dual-Region', 'India', 'Maharashtra, Delhi', 'New Delhi', 23.90885, 74.909678
            UNION ALL
            SELECT 'asia-south2', 'India', 'Region', 'India', 'Delhi', 'New Delhi', 28.714557, 77.098665
            UNION ALL
            SELECT 'asia-southeast1', 'Asia', 'Region', 'Singapore', 'West Region', 'Jurong West', 1.340198, 103.709014
            UNION ALL
            SELECT 'asia-southeast2', 'Indonesia', 'Region', 'Indonesia', 'Java', 'Jakarta', -6.187399, 106.822633
            UNION ALL
            SELECT 'asia1', 'Asia', 'Dual-Region', 'Japan', 'Kanto, Kansai', 'Tokyo, Osaka', 35.201581, 137.563135
            UNION ALL
            SELECT 'australia-southeast1', 'Australia', 'Region', 'Australia', 'New South Wales', 'Sidney', -33.864912, 151.207943
            UNION ALL
            SELECT 'australia-southeast1, australia-southeast2', 'Australia', 'Dual-Region', 'Australia', 'New South Wales, Victoria', 'Sidney, Melbourne', -35.871433, 148.163385
            UNION ALL
            SELECT 'australia-southeast2', 'Australia', 'Region', 'Australia', 'Victoria', 'Melbourne', -37.797206, 144.963901
            UNION ALL
            SELECT 'eu', 'Europe', 'Multi-Region', 'Poland, Finland, Spain, Belgium, Germany, Netherlands, Italy, France', 'Masovian Voivodeship, Kymenlaakso, Community of Madrid, Wallonia, Hesse, Groningen, Lombardy, Ile-de-France', 'Warsaw, Hamina, Madrid, Saint-Ghislain, Frankfurt am Main, Eemshaven, Milan, Paris', 50.552525, 8.497373
            UNION ALL
            SELECT 'eur4', 'Europe', 'Dual-Region', 'Finland, Netherlands', 'Kymenlaakso, Groningen', 'Hamina, Eemshaven', 57.415689, 16.025184
            UNION ALL
            SELECT 'eur5', 'Europe', 'Dual-Region', 'Belgium, UK', 'Wallonia, Greater London', 'Saint-Ghislain, London', 51.007176, 1.867803
            UNION ALL
            SELECT 'eur7', 'Europe', 'Dual-Region', 'UK, Germany', 'Greater London, Hesse', 'London, Frankfurt am Main', 50.894615, 4.344756
            UNION ALL
            SELECT 'eur8', 'Europe', 'Dual-Region', 'Germany, Switzerland', 'Hesse, Canton of Zurich', 'Frankfurt am Main, Zurich', 48.7453, 8.610991
            UNION ALL
            SELECT 'europe-central2', 'Europe', 'Region', 'Poland', 'Masovian Voivodeship', 'Warsaw', 52.238354, 21.009223
            UNION ALL
            SELECT 'europe-central2, europe-north1', 'Europe', 'Dual-Region', 'Poland, Finland', 'Masovian Voivodeship, Kymenlaakso', 'Warsaw, Hamina', 56.443665, 23.760607
            UNION ALL
            SELECT 'europe-central2, europe-southwest1', 'Europe', 'Dual-Region', 'Poland, Spain', 'Masovian Voivodeship, Community of Madrid', 'Warsaw, Madrid', 46.993183, 7.290628
            UNION ALL
            SELECT 'europe-central2, europe-west1', 'Europe',  'Dual-Region', 'Poland, Belgium', 'Masovian Voivodeship, Wallonia', 'Warsaw, Saint-Ghislain', 51.670086, 12.246141
            UNION ALL
            SELECT 'europe-central2, europe-west3', 'Europe', 'Dual-Region', 'Poland, Germany', 'Masovian Voivodeship, Hesse', 'Warsaw, Frankfurt am Main', 51.338001, 14.703662
            UNION ALL
            SELECT 'europe-central2, europe-west4', 'Europe', 'Dual-Region', 'Poland, Netherlands', 'Masovian Voivodeship, Groningen', 'Warsaw, Eemshaven', 53.049773, 14.02046
            UNION ALL
            SELECT 'europe-central2, europe-west8', 'Europe', 'Dual-Region', 'Poland, Italy', 'Masovian Voivodeship, Lombardy', 'Warsaw, Milan', 49.004511, 14.693853
            UNION ALL
            SELECT 'europe-central2, europe-west9', 'Europe', 'Dual-Region', 'Poland, France', 'Masovian Voivodeship, Ile-de-France', 'Warsaw, Paris', 50.922267, 11.342654
            UNION ALL
            SELECT 'europe-north1', 'Europe', 'Region', 'Finland', 'Kymenlaakso', 'Hamina', 60.573043, 27.190688
            UNION ALL
            SELECT 'europe-north1, europe-southwest1', 'Europe', 'Dual-Region', 'Finland, Spain', 'Kymenlaakso, Community of Madrid', 'Hamina, Madrid', 51.479744, 8.333168
            UNION ALL
            SELECT 'europe-north1, europe-west1', 'Europe', 'Dual-Region', 'Finland, Belgium', 'Kymenlaakso, Wallonia', 'Hamina, Saint-Ghislain', 56.070808, 13.978923
            UNION ALL
            SELECT 'europe-north1, europe-west3', 'Europe', 'Dual-Region', 'Finland, Germany', 'Kymenlaakso, Hesse', 'Hamina, Frankfurt am Main', 55.687379, 16.701199
            UNION ALL
            SELECT 'europe-north1, europe-west8', 'Europe', 'Dual-Region', 'Finland, Italy', 'Kymenlaakso, Lombardy', 'Hamina, Milan', 53.351265, 16.588117
            UNION ALL
            SELECT 'europe-north1, europe-west9', 'Europe', 'Dual-Region', 'Finland, France', 'Kymenlaakso, Ile-de-France', 'Hamina, Paris', 55.339813, 12.942202
            UNION ALL
            SELECT 'europe-southwest1', 'Europe', 'Region', 'Spain', 'Community of Madrid', 'Madrid', 40.423203, -3.707017
            UNION ALL
            SELECT 'europe-southwest1, europe-west1', 'Europe', 'Dual-Region', 'Spain, Belgium', 'Community of Madrid, Wallonia', 'Madrid, Saint-Ghislain', 45.508623, -0.281364
            UNION ALL
            SELECT 'europe-southwest1, europe-west3', 'Europe', 'Dual-Region', 'Spain, Germany', 'Community of Madrid, Hesse', 'Madrid, Frankfurt am Main', 45.434948, 1.955968
            UNION ALL
            SELECT 'europe-southwest1, europe-west4', 'Europe', 'Dual-Region', 'Spain, Netherlands', 'Community of Madrid, Groningen', 'Madrid, Eemshaven', 47.050201, 0.918886
            UNION ALL
            SELECT 'europe-southwest1, europe-west8', 'Europe', 'Dual-Region', 'Spain, Italy', 'Community of Madrid, Lombardy', 'Madrid, Milan', 43.127066, 2.471997
            UNION ALL
            SELECT 'europe-southwest1, europe-west9', 'Europe', 'Dual-Region', 'Spain, France', 'Community of Madrid, Ile-de-France', 'Madrid, Paris', 44.681187, -0.898924
            UNION ALL
            SELECT 'europe-west1', 'Europe', 'Region', 'Belgium', 'Wallonia', 'Saint-Ghislain', 50.471449, 3.817129
            UNION ALL
            SELECT 'europe-west1, europe-west3', 'Europe', 'Dual-Region', 'Belgium, Germany', 'Wallonia, Hesse', 'Saint-Ghislain, Frankfurt am Main', 50.317925, 6.259341
            UNION ALL
            SELECT 'europe-west1, europe-west4', 'Europe', 'Dual-Region', 'Belgium, Netherlands', 'Wallonia, Groningen', 'Saint-Ghislain, Eemshaven', 51.964664, 5.276023
            UNION ALL
            SELECT 'europe-west1, europe-west8', 'Europe', 'Dual-Region', 'Belgium, Italy', 'Wallonia, Lombardy', 'Saint-Ghislain, Milan', 48.001489, 6.629576
            UNION ALL
            SELECT 'europe-west1, europe-west9', 'Europe', 'Dual-Region', 'Belgium, France', 'Wallonia, Ile-de-France', 'Saint-Ghislain, Paris', 49.66779, 3.071821
            UNION ALL
            SELECT 'europe-west10', 'Europe', 'Region', 'Germany', 'Berlin', 'Berlin', 52.523101, 13.401341
            UNION ALL
            SELECT 'europe-west12', 'Europe', 'Region', 'Italy', 'Piedmont', 'Turin', 45.211965, 7.379327
            UNION ALL
            SELECT 'europe-west2', 'Europe', 'Region', 'UK', 'Greater London', 'London', 51.509726, -0.125643
            UNION ALL
            SELECT 'europe-west3', 'Europe', 'Region', 'Germany', 'Hesse', 'Frankfurt am Main', 50.11361, 8.683244
            UNION ALL
            SELECT 'europe-west3, europe-west4', 'Europe', 'Dual-Region', 'Germany, Netherlands', 'Hesse, Groningen', 'Frankfurt am Main, Eemshaven', 51.779731, 7.793091
            UNION ALL
            SELECT 'europe-west3, europe-west8', 'Europe', 'Dual-Region', 'Germany, Italy', 'Hesse, Lombardy', 'Frankfurt am Main, Milan', 47.791677, 8.943693
            UNION ALL
            SELECT 'europe-west3, europe-west9', 'Europe', 'Dual-Region', 'Germany, France', 'Hesse, Ile-de-France', 'Frankfurt am Main, Paris', 49.529773, 5.476431
            UNION ALL
            SELECT 'europe-west4', 'Europe', 'Region', 'Netherlands', 'Groningen', 'Eemshaven', 53.438615, 6.834814
            UNION ALL
            SELECT 'europe-west4, europe-west8', 'Europe', 'Dual-Region', 'Netherlands, Italy', 'Groningen, Lombardy', 'Eemshaven, Milan', 49.459807, 8.103901
            UNION ALL
            SELECT 'europe-west4, europe-west9', 'Europe', 'Dual-Region', 'Netherlands, France', 'Groningen, Ile-de-France', 'Eemshaven, Paris', 51.170439, 4.481469
            UNION ALL
            SELECT 'europe-west6', 'Europe', 'Region', 'Switzerland', 'Canton of Zurich', 'Zurich', 47.376947, 8.542569
            UNION ALL
            SELECT 'europe-west8', 'Europe', 'Region', 'Italy', 'Lombardy', 'Milan', 45.469205, 9.181849
            UNION ALL
            SELECT 'europe-west8, europe-west9', 'Europe', 'Dual-Region', 'Italy, France', 'Lombardy, Ile-de-France', 'Milan, Paris', 47.215085, 5.875479
            UNION ALL
            SELECT 'europe-west9', 'Europe', 'Region', 'France', 'Ile-de-France', 'Milan', 48.859503, 2.350808
            UNION ALL
            SELECT 'me-central1', 'Middle East', 'Region', 'Qatar', 'Doha', 'Doha', 25.295093, 51.531892
            UNION ALL
            SELECT 'me-central2', 'Middle East', 'Region', 'Saudi Arabia', 'Eastern Province', 'Dammam', 26.421517, 50.087281
            UNION ALL
            SELECT 'me-west1', 'Middle East', 'Region', 'Israel', 'Tel Aviv', 'Tel Aviv', 32.088196, 34.780762
            UNION ALL
            SELECT 'nam4', 'North America', 'Dual-Region', 'United States', 'Iowa, South Carolina', 'Council Bluffs, Moncks Corner', 37.495021, -87.509075
            UNION ALL
            SELECT 'northamerica-northeast1', 'North America', 'Region', 'Canada', 'Quebec', 'Montreal', 45.498936, -73.564944
            UNION ALL
            SELECT 'northamerica-northeast1, northamerica-northeast2', 'North America', 'Dual-Region', 'Canada', 'Quebec,Ontario', 'Montreal, Toronto', 44.613403, -76.519054
            UNION ALL
            SELECT 'northamerica-northeast2', 'North America', 'Region', 'Canada', 'Ontario', 'Toronto', 43.65407, -79.380818
            UNION ALL
            SELECT 'southamerica-east1', 'South America', 'Region', 'Brazil', 'Sao Paulo', 'Osasco', -23.532524, -46.788355
            UNION ALL
            SELECT 'southamerica-west1', 'South America', 'Region', 'Chile', 'Santiago Province', 'Santiago', -33.440271, -70.671093
            UNION ALL
            SELECT 'us', 'North America', 'Multi-Region', 'United States', 'Iowa, South Carolina, Virginia, Ohio, Texas, Oregon, California, Utah, Nevada', 'Council Bluffs, Moncks Corner, Ashburn, Columbus, Dallas, The Dalles, Los Angeles, Salt Lake City, Las Vegas', 39.226851, -99.750223
            UNION ALL
            SELECT 'us-central1', 'North America', 'Region', 'United States', 'Iowa', 'Council Bluffs', 41.264954, -95.860417
            UNION ALL
            SELECT 'us-central1, us-east4', 'North America', 'Dual-Region', 'United States', 'Iowa, Virginia', 'Council Bluffs, Ashburn', 40.520283, -86.522509
            UNION ALL
            SELECT 'us-central1, us-east5', 'North America', 'Dual-Region', 'United States', 'Iowa, Ohio', 'Council Bluffs, Columbus', 40.793702, -89.367268
            UNION ALL
            SELECT 'us-central1, us-south1', 'North America', 'Dual-Region', 'United States', 'Iowa, Dallas', 'Council Bluffs, Dallas', 37.031975, -96.356595
            UNION ALL
            SELECT 'us-central1, us-west1', 'North America', 'Dual-Region', 'United States', 'Iowa, Oregon', 'Council Bluffs, The Dalles', 44.137471, -108.060859
            UNION ALL
            SELECT 'us-central1, us-west2', 'North America', 'Dual-Region', 'United States', 'Iowa, California', 'Council Bluffs, Los Angeles', 38.201122, -107.605979
            UNION ALL
            SELECT 'us-central1, us-west3', 'North America', 'Dual-Region', 'United States', 'Iowa, Utah', 'Council Bluffs, Salt Lake City', 41.295649, -103.90781
            UNION ALL
            SELECT 'us-central1, us-west4', 'North America', 'Dual-Region', 'United States', 'Iowa, Nevada', 'Council Bluffs, Las Vegas', 39.121295, -105.844031
            UNION ALL
            SELECT 'us-east1', 'North America', 'Region', 'United States', 'South Carolina', 'Moncks Corner', 33.196352, -80.012519
            UNION ALL
            SELECT 'us-east1, us-east4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Virginia', 'Moncks Corner, Ashburn', 36.127768, -78.797054
            UNION ALL
            SELECT 'us-east1, us-east5', 'North America', 'Dual-Region', 'United States', 'South Carolina, Ohio', 'Moncks Corner, Columbus', 36.589948, -81.440579
            UNION ALL
            SELECT 'us-east1, us-south1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Texas', 'Moncks Corner, Dallas', 33.279208, -88.425525
            UNION ALL
            SELECT 'us-east1, us-west1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Oregon', 'Moncks Corner, The Dalles', 41.248379, -98.677918
            UNION ALL
            SELECT 'us-east1, us-west2', 'North America', 'Dual-Region', 'United States', 'South Carolina, California', 'Moncks Corner, Los Angeles', 35.149434, -99.030803
            UNION ALL
            SELECT 'us-east1, us-west3', 'North America', 'Dual-Region', 'United States', 'South Carolina, Utah', 'Moncks Corner, Salt Lake City', 38.064993, -95.137921
            UNION ALL
            SELECT 'us-east1, us-west4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Nevada', 'Moncks Corner, Las Vegas', 35.979049, -97.246718
            UNION ALL
            SELECT 'us-east4', 'North America', 'Region', 'United States', 'Virginia', 'Ashburn', 39.045953, -77.487424
            UNION ALL
            SELECT 'us-east4, us-south1', 'North America', 'Dual-Region', 'United States', 'Virginia, Texas', 'Ashburn, Dallas', 36.310235, -87.529362
            UNION ALL
            SELECT 'us-east4, us-west1', 'North America', 'Dual-Region', 'United States', 'Virginia, Oregon', 'Ashburn, The Dalles', 44.449481, -98.137685
            UNION ALL
            SELECT 'us-east4, us-west2', 'North America', 'Dual-Region', 'United States', 'Virginia, California', 'Ashburn, Los Angeles', 38.345123, -98.55482
            UNION ALL
            SELECT 'us-east4, us-west3', 'North America', 'Dual-Region', 'United States', 'Virginia, Utah', 'Ashburn, Salt Lake City', 41.202492, -94.46785
            UNION ALL
            SELECT 'us-east4, us-west4', 'North America', 'Dual-Region', 'United States', 'Virginia, Nevada', 'Ashburn, Las Vegas', 39.146327, -96.687428
            UNION ALL
            SELECT 'us-east5', 'North America', 'Region', 'United States', 'Ohio', 'Columbus', 39.964944, -82.99977
            UNION ALL
            SELECT 'us-east7', 'North America', 'Region', 'United States', 'Alabama', NULL, NULL, NULL
            UNION ALL
            SELECT 'us-south1', 'North America', 'Region', 'United States', 'Texas', 'Dallas', 32.797148, -96.800281
            UNION ALL
            SELECT 'us-south1, us-west1', 'North America', 'Dual-Region', 'United States', 'Texas, Oregon', 'Dallas, The Dalles', 39.83593, -107.859488
            UNION ALL
            SELECT 'us-south1, us-west2', 'North America', 'Dual-Region', 'United States', 'Texas, California', 'Dallas, Los Angeles', 33.900635, -107.445958
            UNION ALL
            SELECT 'us-south1, us-west3', 'North America', 'Dual-Region', 'United States', 'Texas, Utah', 'Dallas, Salt Lake City', 37.021621, -103.951912
            UNION ALL
            SELECT 'us-south1, us-west4', 'North America', 'Dual-Region', 'United States', 'Texas, Nevada', 'Dallas, Las Vegas', 34.83426, -105.780008
            UNION ALL
            SELECT 'us-west1', 'North America', 'Region', 'United States', 'Oregon', 'The Dalles', 45.602508, -121.184333
            UNION ALL
            SELECT 'us-west1, us-west2', 'North America', 'Dual-Region', 'United States', 'Oregon, California', 'The Dalles, Los Angeles', 39.846614, -119.594169
            UNION ALL
            SELECT 'us-west1, us-west3', 'North America', 'Dual-Region', 'United States', 'Oregon, Utah', 'The Dalles, Salt Lake City', 43.279517, -116.354951
            UNION ALL
            SELECT 'us-west1, us-west4', 'North America', 'Dual-Region', 'United States', 'Oregon, Nevada', 'The Dalles, Las Vegas', 40.931855, -117.943759
            UNION ALL
            SELECT 'us-west2', 'North America', 'Region', 'United States', 'California', 'Los Angeles', 34.072382, -118.25112
            UNION ALL
            SELECT 'us-west2, us-west3', 'North America', 'Dual-Region', 'United States', 'California, Utah', 'Los Angeles, Salt Lake City', 37.463026, -115.215179
            UNION ALL
            SELECT 'us-west2, us-west4', 'North America', 'Dual-Region', 'United States', 'California, Nevada', 'Los Angeles, Las Vegas', 35.137433, -116.713346
            UNION ALL
            SELECT 'us-west3', 'North America', 'Region', 'United States', 'Utah', 'Salt Lake City', 40.768691, -111.894407
            UNION ALL
            SELECT 'us-west3, us-west4', 'North America', 'Dual-Region', 'United States', 'Utah, Nevada', 'Salt Lake City, Las Vegas', 38.48677, -113.566379
            UNION ALL
            SELECT 'us-west4', 'North America', 'Region', 'United States', 'Nevada', 'Las Vegas', 36.182547, -115.13519
            UNION ALL
            SELECT 'us-west8', 'North America', 'Region', 'United States', 'Arizona', 'Phoenix', 33.450430, -112.075676), geographic_area_location AS (
            SELECT 'Africa' as geographic_area, '-26.206619, 28.031437' as geographic_area_coordinates
            UNION ALL
            SELECT 'Asia', '26.22887736583804, 122.92055542594133'
            UNION ALL 
            SELECT 'Australia', '-35.87143312617882, 148.16338509205735'
            UNION ALL
            SELECT 'Europe', '50.419437352900566, 8.148756653935328'
            UNION ALL 
            SELECT 'India', '23.908849931246557, 74.90967780978364'
            UNION ALL
            SELECT 'Indonesia', '-6.187399000000001, 106.822633'
            UNION ALL
            SELECT 'Middle East', '28.1383073720678, 45.697549748255476'
            UNION ALL
            SELECT 'North America', '39.49067855459679, -99.26613885745078'
            UNION ALL
            SELECT 'South America', '-29.01378617077172, -58.159799449834644')
            SELECT
            ls.location,
            ls.geographic_area,
            gal.geographic_area_coordinates,
            ls.location_type,        
            ls.countries,
            ls.latitude,
            ls.longitude,
            CONCAT(CAST(ls.latitude AS STRING), ', ', CAST(ls.longitude AS STRING)) AS location_coordinates,
            CASE
                WHEN ls.geographic_area = 'Europe' AND NOT (ls.countries LIKE ('%UK%') OR ls.countries LIKE ('%Switzerland%')) THEN 'European Union'
                WHEN ls.countries = 'United States' THEN 'United States'
                ELSE countries
            END AS countries_union,
            CASE 
                WHEN ls.countries = 'United States' THEN FALSE
                ELSE TRUE
            END AS outside_us,
            CASE
                WHEN ls.geographic_area = 'Europe' AND NOT (ls.countries LIKE ('%UK%') OR ls.countries LIKE ('%Switzerland%')) THEN FALSE
                ELSE TRUE
            END AS outside_eu
            FROM
            location_set AS ls 
        JOIN 
            geographic_area_location as gal
            ON ls.geographic_area = gal.geographic_area)
        SELECT 
            e.*,
            ri.location_type,
            ri.geographic_area,
            ri.geographic_area_coordinates,
            ri.location_coordinates,
            ri.countries,
            ri.countries_union,
            ri.outside_us,
            ri.outside_eu
            
        FROM
            events_view AS e
        LEFT JOIN
            regions_information AS ri
        ON
            e.manifest.location = ri.location
    """.format(table_name)

    # Create the view in BigQuery.
    view_events = bq_client.create_table(view_events)  # Make an API request.
    print(f"Created {view_events.table_type}: {str(view_events.reference)}")
   
    # Create View for Bucket Attribute
    # Create View for Events
    view_id_buckets = "{}.{}.bucket_attributes_view_looker".format(args.PROJECT_ID, args.DATASET_NAME)
    view_buckets = bigquery.Table(view_id_buckets)
    # The SQL query to be executed for events.
    table_name_bucket = "{}.{}".format(dataset, bucket)
    table_name_objecta = "{}.{}".format(dataset, objecta)
    table_name_project = "{}.{}".format(dataset, project)
    view_buckets.view_query = r"""
        WITH
            distinct_snapshots AS (
            SELECT
                DISTINCT snapshotTime
            FROM
                {}
            WHERE
                snapshotTime is NOT NULL
            INTERSECT DISTINCT
            SELECT
                DISTINCT snapshotTime
            FROM
                {}
            WHERE
                snapshotTime IS NOT NULL), bucket_attributes_latest AS (
            SELECT
                *
            FROM
                {}
            WHERE
                snapshotTime = (
                SELECT
                    MAX(snapshotTime)
                FROM
                    distinct_snapshots
                )
            ),project_attributes_latest AS (
            SELECT
                *
            FROM
                {}
            WHERE
                snapshotTime = (
                SELECT
                    MAX(snapshotTime)
                FROM
                    distinct_snapshots
                )
            ), regions_information AS (
                    WITH location_set AS (SELECT 'africa-south1' AS location, 'Africa' AS geographic_area, 'Region' AS location_type, 'South Africa' AS countries, 'Gauteng' AS state, 'Johannesburg' AS city, -26.206619 AS latitude, 28.031437 AS longitude
                UNION ALL
                SELECT 'asia', 'Asia', 'Multi-Region', 'Taiwan, Japan, South Korea, Singapore', 'Taiwan Province, Kanto, Kansai, Seoul Capital, West Region', 'Changhua County, Tokyo, Osaka, Seoul, Jurong West', 27.407598, 124.302272
                UNION ALL
                SELECT 'asia-east1', 'Asia', 'Region', 'Taiwan', 'Taiwan Province', 'Changhua County', 24.04955, 120.516007
                UNION ALL
                SELECT 'asia-east1, asia-southeast1', 'Asia', 'Dual-Region', 'Taiwan, Singapore', 'Taiwan Province, West Region', 'Changhua County, Jurong West', 12.827841, 111.729625
                UNION ALL
                SELECT 'asia-east2', 'Asia', 'Region', 'China', 'Hong Kong', 'Hong Kong', 22.324061, 114.171655
                UNION ALL
                SELECT 'asia-northeast1', 'Asia', 'Region', 'Japan', 'Kanto', 'Tokyo', 35.673817, 139.65123
                UNION ALL
                SELECT 'asia-northeast2', 'Asia', 'Region', 'Japan', 'Kansai', 'Osaka', 34.693925, 135.500077
                UNION ALL
                SELECT 'asia-northeast3', 'Asia', 'Region', 'South Korea', 'Seoul Capital', 'Seoul', 37.552242, 126.994724
                UNION ALL
                SELECT 'asia-south1', 'India', 'Region', 'India', 'Maharashtra', 'Mumbai', 19.07439, 72.878422
                UNION ALL
                SELECT 'asia-south1, asia-south2', 'India', 'Dual-Region', 'India', 'Maharashtra, Delhi', 'New Delhi', 23.90885, 74.909678
                UNION ALL
                SELECT 'asia-south2', 'India', 'Region', 'India', 'Delhi', 'New Delhi', 28.714557, 77.098665
                UNION ALL
                SELECT 'asia-southeast1', 'Asia', 'Region', 'Singapore', 'West Region', 'Jurong West', 1.340198, 103.709014
                UNION ALL
                SELECT 'asia-southeast2', 'Indonesia', 'Region', 'Indonesia', 'Java', 'Jakarta', -6.187399, 106.822633
                UNION ALL
                SELECT 'asia1', 'Asia', 'Dual-Region', 'Japan', 'Kanto, Kansai', 'Tokyo, Osaka', 35.201581, 137.563135
                UNION ALL
                SELECT 'australia-southeast1', 'Australia', 'Region', 'Australia', 'New South Wales', 'Sidney', -33.864912, 151.207943
                UNION ALL
                SELECT 'australia-southeast1, australia-southeast2', 'Australia', 'Dual-Region', 'Australia', 'New South Wales, Victoria', 'Sidney, Melbourne', -35.871433, 148.163385
                UNION ALL
                SELECT 'australia-southeast2', 'Australia', 'Region', 'Australia', 'Victoria', 'Melbourne', -37.797206, 144.963901
                UNION ALL
                SELECT 'eu', 'Europe', 'Multi-Region', 'Poland, Finland, Spain, Belgium, Germany, Netherlands, Italy, France', 'Masovian Voivodeship, Kymenlaakso, Community of Madrid, Wallonia, Hesse, Groningen, Lombardy, Ile-de-France', 'Warsaw, Hamina, Madrid, Saint-Ghislain, Frankfurt am Main, Eemshaven, Milan, Paris', 50.552525, 8.497373
                UNION ALL
                SELECT 'eur4', 'Europe', 'Dual-Region', 'Finland, Netherlands', 'Kymenlaakso, Groningen', 'Hamina, Eemshaven', 57.415689, 16.025184
                UNION ALL
                SELECT 'eur5', 'Europe', 'Dual-Region', 'Belgium, UK', 'Wallonia, Greater London', 'Saint-Ghislain, London', 51.007176, 1.867803
                UNION ALL
                SELECT 'eur7', 'Europe', 'Dual-Region', 'UK, Germany', 'Greater London, Hesse', 'London, Frankfurt am Main', 50.894615, 4.344756
                UNION ALL
                SELECT 'eur8', 'Europe', 'Dual-Region', 'Germany, Switzerland', 'Hesse, Canton of Zurich', 'Frankfurt am Main, Zurich', 48.7453, 8.610991
                UNION ALL
                SELECT 'europe-central2', 'Europe', 'Region', 'Poland', 'Masovian Voivodeship', 'Warsaw', 52.238354, 21.009223
                UNION ALL
                SELECT 'europe-central2, europe-north1', 'Europe', 'Dual-Region', 'Poland, Finland', 'Masovian Voivodeship, Kymenlaakso', 'Warsaw, Hamina', 56.443665, 23.760607
                UNION ALL
                SELECT 'europe-central2, europe-southwest1', 'Europe', 'Dual-Region', 'Poland, Spain', 'Masovian Voivodeship, Community of Madrid', 'Warsaw, Madrid', 46.993183, 7.290628
                UNION ALL
                SELECT 'europe-central2, europe-west1', 'Europe',  'Dual-Region', 'Poland, Belgium', 'Masovian Voivodeship, Wallonia', 'Warsaw, Saint-Ghislain', 51.670086, 12.246141
                UNION ALL
                SELECT 'europe-central2, europe-west3', 'Europe', 'Dual-Region', 'Poland, Germany', 'Masovian Voivodeship, Hesse', 'Warsaw, Frankfurt am Main', 51.338001, 14.703662
                UNION ALL
                SELECT 'europe-central2, europe-west4', 'Europe', 'Dual-Region', 'Poland, Netherlands', 'Masovian Voivodeship, Groningen', 'Warsaw, Eemshaven', 53.049773, 14.02046
                UNION ALL
                SELECT 'europe-central2, europe-west8', 'Europe', 'Dual-Region', 'Poland, Italy', 'Masovian Voivodeship, Lombardy', 'Warsaw, Milan', 49.004511, 14.693853
                UNION ALL
                SELECT 'europe-central2, europe-west9', 'Europe', 'Dual-Region', 'Poland, France', 'Masovian Voivodeship, Ile-de-France', 'Warsaw, Paris', 50.922267, 11.342654
                UNION ALL
                SELECT 'europe-north1', 'Europe', 'Region', 'Finland', 'Kymenlaakso', 'Hamina', 60.573043, 27.190688
                UNION ALL
                SELECT 'europe-north1, europe-southwest1', 'Europe', 'Dual-Region', 'Finland, Spain', 'Kymenlaakso, Community of Madrid', 'Hamina, Madrid', 51.479744, 8.333168
                UNION ALL
                SELECT 'europe-north1, europe-west1', 'Europe', 'Dual-Region', 'Finland, Belgium', 'Kymenlaakso, Wallonia', 'Hamina, Saint-Ghislain', 56.070808, 13.978923
                UNION ALL
                SELECT 'europe-north1, europe-west3', 'Europe', 'Dual-Region', 'Finland, Germany', 'Kymenlaakso, Hesse', 'Hamina, Frankfurt am Main', 55.687379, 16.701199
                UNION ALL
                SELECT 'europe-north1, europe-west8', 'Europe', 'Dual-Region', 'Finland, Italy', 'Kymenlaakso, Lombardy', 'Hamina, Milan', 53.351265, 16.588117
                UNION ALL
                SELECT 'europe-north1, europe-west9', 'Europe', 'Dual-Region', 'Finland, France', 'Kymenlaakso, Ile-de-France', 'Hamina, Paris', 55.339813, 12.942202
                UNION ALL
                SELECT 'europe-southwest1', 'Europe', 'Region', 'Spain', 'Community of Madrid', 'Madrid', 40.423203, -3.707017
                UNION ALL
                SELECT 'europe-southwest1, europe-west1', 'Europe', 'Dual-Region', 'Spain, Belgium', 'Community of Madrid, Wallonia', 'Madrid, Saint-Ghislain', 45.508623, -0.281364
                UNION ALL
                SELECT 'europe-southwest1, europe-west3', 'Europe', 'Dual-Region', 'Spain, Germany', 'Community of Madrid, Hesse', 'Madrid, Frankfurt am Main', 45.434948, 1.955968
                UNION ALL
                SELECT 'europe-southwest1, europe-west4', 'Europe', 'Dual-Region', 'Spain, Netherlands', 'Community of Madrid, Groningen', 'Madrid, Eemshaven', 47.050201, 0.918886
                UNION ALL
                SELECT 'europe-southwest1, europe-west8', 'Europe', 'Dual-Region', 'Spain, Italy', 'Community of Madrid, Lombardy', 'Madrid, Milan', 43.127066, 2.471997
                UNION ALL
                SELECT 'europe-southwest1, europe-west9', 'Europe', 'Dual-Region', 'Spain, France', 'Community of Madrid, Ile-de-France', 'Madrid, Paris', 44.681187, -0.898924
                UNION ALL
                SELECT 'europe-west1', 'Europe', 'Region', 'Belgium', 'Wallonia', 'Saint-Ghislain', 50.471449, 3.817129
                UNION ALL
                SELECT 'europe-west1, europe-west3', 'Europe', 'Dual-Region', 'Belgium, Germany', 'Wallonia, Hesse', 'Saint-Ghislain, Frankfurt am Main', 50.317925, 6.259341
                UNION ALL
                SELECT 'europe-west1, europe-west4', 'Europe', 'Dual-Region', 'Belgium, Netherlands', 'Wallonia, Groningen', 'Saint-Ghislain, Eemshaven', 51.964664, 5.276023
                UNION ALL
                SELECT 'europe-west1, europe-west8', 'Europe', 'Dual-Region', 'Belgium, Italy', 'Wallonia, Lombardy', 'Saint-Ghislain, Milan', 48.001489, 6.629576
                UNION ALL
                SELECT 'europe-west1, europe-west9', 'Europe', 'Dual-Region', 'Belgium, France', 'Wallonia, Ile-de-France', 'Saint-Ghislain, Paris', 49.66779, 3.071821
                UNION ALL
                SELECT 'europe-west10', 'Europe', 'Region', 'Germany', 'Berlin', 'Berlin', 52.523101, 13.401341
                UNION ALL
                SELECT 'europe-west12', 'Europe', 'Region', 'Italy', 'Piedmont', 'Turin', 45.211965, 7.379327
                UNION ALL
                SELECT 'europe-west2', 'Europe', 'Region', 'UK', 'Greater London', 'London', 51.509726, -0.125643
                UNION ALL
                SELECT 'europe-west3', 'Europe', 'Region', 'Germany', 'Hesse', 'Frankfurt am Main', 50.11361, 8.683244
                UNION ALL
                SELECT 'europe-west3, europe-west4', 'Europe', 'Dual-Region', 'Germany, Netherlands', 'Hesse, Groningen', 'Frankfurt am Main, Eemshaven', 51.779731, 7.793091
                UNION ALL
                SELECT 'europe-west3, europe-west8', 'Europe', 'Dual-Region', 'Germany, Italy', 'Hesse, Lombardy', 'Frankfurt am Main, Milan', 47.791677, 8.943693
                UNION ALL
                SELECT 'europe-west3, europe-west9', 'Europe', 'Dual-Region', 'Germany, France', 'Hesse, Ile-de-France', 'Frankfurt am Main, Paris', 49.529773, 5.476431
                UNION ALL
                SELECT 'europe-west4', 'Europe', 'Region', 'Netherlands', 'Groningen', 'Eemshaven', 53.438615, 6.834814
                UNION ALL
                SELECT 'europe-west4, europe-west8', 'Europe', 'Dual-Region', 'Netherlands, Italy', 'Groningen, Lombardy', 'Eemshaven, Milan', 49.459807, 8.103901
                UNION ALL
                SELECT 'europe-west4, europe-west9', 'Europe', 'Dual-Region', 'Netherlands, France', 'Groningen, Ile-de-France', 'Eemshaven, Paris', 51.170439, 4.481469
                UNION ALL
                SELECT 'europe-west6', 'Europe', 'Region', 'Switzerland', 'Canton of Zurich', 'Zurich', 47.376947, 8.542569
                UNION ALL
                SELECT 'europe-west8', 'Europe', 'Region', 'Italy', 'Lombardy', 'Milan', 45.469205, 9.181849
                UNION ALL
                SELECT 'europe-west8, europe-west9', 'Europe', 'Dual-Region', 'Italy, France', 'Lombardy, Ile-de-France', 'Milan, Paris', 47.215085, 5.875479
                UNION ALL
                SELECT 'europe-west9', 'Europe', 'Region', 'France', 'Ile-de-France', 'Milan', 48.859503, 2.350808
                UNION ALL
                SELECT 'me-central1', 'Middle East', 'Region', 'Qatar', 'Doha', 'Doha', 25.295093, 51.531892
                UNION ALL
                SELECT 'me-central2', 'Middle East', 'Region', 'Saudi Arabia', 'Eastern Province', 'Dammam', 26.421517, 50.087281
                UNION ALL
                SELECT 'me-west1', 'Middle East', 'Region', 'Israel', 'Tel Aviv', 'Tel Aviv', 32.088196, 34.780762
                UNION ALL
                SELECT 'nam4', 'North America', 'Dual-Region', 'United States', 'Iowa, South Carolina', 'Council Bluffs, Moncks Corner', 37.495021, -87.509075
                UNION ALL
                SELECT 'northamerica-northeast1', 'North America', 'Region', 'Canada', 'Quebec', 'Montreal', 45.498936, -73.564944
                UNION ALL
                SELECT 'northamerica-northeast1, northamerica-northeast2', 'North America', 'Dual-Region', 'Canada', 'Quebec,Ontario', 'Montreal, Toronto', 44.613403, -76.519054
                UNION ALL
                SELECT 'northamerica-northeast2', 'North America', 'Region', 'Canada', 'Ontario', 'Toronto', 43.65407, -79.380818
                UNION ALL
                SELECT 'southamerica-east1', 'South America', 'Region', 'Brazil', 'Sao Paulo', 'Osasco', -23.532524, -46.788355
                UNION ALL
                SELECT 'southamerica-west1', 'South America', 'Region', 'Chile', 'Santiago Province', 'Santiago', -33.440271, -70.671093
                UNION ALL
                SELECT 'us', 'North America', 'Multi-Region', 'United States', 'Iowa, South Carolina, Virginia, Ohio, Texas, Oregon, California, Utah, Nevada', 'Council Bluffs, Moncks Corner, Ashburn, Columbus, Dallas, The Dalles, Los Angeles, Salt Lake City, Las Vegas', 39.226851, -99.750223
                UNION ALL
                SELECT 'us-central1', 'North America', 'Region', 'United States', 'Iowa', 'Council Bluffs', 41.264954, -95.860417
                UNION ALL
                SELECT 'us-central1, us-east4', 'North America', 'Dual-Region', 'United States', 'Iowa, Virginia', 'Council Bluffs, Ashburn', 40.520283, -86.522509
                UNION ALL
                SELECT 'us-central1, us-east5', 'North America', 'Dual-Region', 'United States', 'Iowa, Ohio', 'Council Bluffs, Columbus', 40.793702, -89.367268
                UNION ALL
                SELECT 'us-central1, us-south1', 'North America', 'Dual-Region', 'United States', 'Iowa, Dallas', 'Council Bluffs, Dallas', 37.031975, -96.356595
                UNION ALL
                SELECT 'us-central1, us-west1', 'North America', 'Dual-Region', 'United States', 'Iowa, Oregon', 'Council Bluffs, The Dalles', 44.137471, -108.060859
                UNION ALL
                SELECT 'us-central1, us-west2', 'North America', 'Dual-Region', 'United States', 'Iowa, California', 'Council Bluffs, Los Angeles', 38.201122, -107.605979
                UNION ALL
                SELECT 'us-central1, us-west3', 'North America', 'Dual-Region', 'United States', 'Iowa, Utah', 'Council Bluffs, Salt Lake City', 41.295649, -103.90781
                UNION ALL
                SELECT 'us-central1, us-west4', 'North America', 'Dual-Region', 'United States', 'Iowa, Nevada', 'Council Bluffs, Las Vegas', 39.121295, -105.844031
                UNION ALL
                SELECT 'us-east1', 'North America', 'Region', 'United States', 'South Carolina', 'Moncks Corner', 33.196352, -80.012519
                UNION ALL
                SELECT 'us-east1, us-east4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Virginia', 'Moncks Corner, Ashburn', 36.127768, -78.797054
                UNION ALL
                SELECT 'us-east1, us-east5', 'North America', 'Dual-Region', 'United States', 'South Carolina, Ohio', 'Moncks Corner, Columbus', 36.589948, -81.440579
                UNION ALL
                SELECT 'us-east1, us-south1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Texas', 'Moncks Corner, Dallas', 33.279208, -88.425525
                UNION ALL
                SELECT 'us-east1, us-west1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Oregon', 'Moncks Corner, The Dalles', 41.248379, -98.677918
                UNION ALL
                SELECT 'us-east1, us-west2', 'North America', 'Dual-Region', 'United States', 'South Carolina, California', 'Moncks Corner, Los Angeles', 35.149434, -99.030803
                UNION ALL
                SELECT 'us-east1, us-west3', 'North America', 'Dual-Region', 'United States', 'South Carolina, Utah', 'Moncks Corner, Salt Lake City', 38.064993, -95.137921
                UNION ALL
                SELECT 'us-east1, us-west4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Nevada', 'Moncks Corner, Las Vegas', 35.979049, -97.246718
                UNION ALL
                SELECT 'us-east4', 'North America', 'Region', 'United States', 'Virginia', 'Ashburn', 39.045953, -77.487424
                UNION ALL
                SELECT 'us-east4, us-south1', 'North America', 'Dual-Region', 'United States', 'Virginia, Texas', 'Ashburn, Dallas', 36.310235, -87.529362
                UNION ALL
                SELECT 'us-east4, us-west1', 'North America', 'Dual-Region', 'United States', 'Virginia, Oregon', 'Ashburn, The Dalles', 44.449481, -98.137685
                UNION ALL
                SELECT 'us-east4, us-west2', 'North America', 'Dual-Region', 'United States', 'Virginia, California', 'Ashburn, Los Angeles', 38.345123, -98.55482
                UNION ALL
                SELECT 'us-east4, us-west3', 'North America', 'Dual-Region', 'United States', 'Virginia, Utah', 'Ashburn, Salt Lake City', 41.202492, -94.46785
                UNION ALL
                SELECT 'us-east4, us-west4', 'North America', 'Dual-Region', 'United States', 'Virginia, Nevada', 'Ashburn, Las Vegas', 39.146327, -96.687428
                UNION ALL
                SELECT 'us-east5', 'North America', 'Region', 'United States', 'Ohio', 'Columbus', 39.964944, -82.99977
                UNION ALL
                SELECT 'us-east7', 'North America', 'Region', 'United States', 'Alabama', NULL, NULL, NULL
                UNION ALL
                SELECT 'us-south1', 'North America', 'Region', 'United States', 'Texas', 'Dallas', 32.797148, -96.800281
                UNION ALL
                SELECT 'us-south1, us-west1', 'North America', 'Dual-Region', 'United States', 'Texas, Oregon', 'Dallas, The Dalles', 39.83593, -107.859488
                UNION ALL
                SELECT 'us-south1, us-west2', 'North America', 'Dual-Region', 'United States', 'Texas, California', 'Dallas, Los Angeles', 33.900635, -107.445958
                UNION ALL
                SELECT 'us-south1, us-west3', 'North America', 'Dual-Region', 'United States', 'Texas, Utah', 'Dallas, Salt Lake City', 37.021621, -103.951912
                UNION ALL
                SELECT 'us-south1, us-west4', 'North America', 'Dual-Region', 'United States', 'Texas, Nevada', 'Dallas, Las Vegas', 34.83426, -105.780008
                UNION ALL
                SELECT 'us-west1', 'North America', 'Region', 'United States', 'Oregon', 'The Dalles', 45.602508, -121.184333
                UNION ALL
                SELECT 'us-west1, us-west2', 'North America', 'Dual-Region', 'United States', 'Oregon, California', 'The Dalles, Los Angeles', 39.846614, -119.594169
                UNION ALL
                SELECT 'us-west1, us-west3', 'North America', 'Dual-Region', 'United States', 'Oregon, Utah', 'The Dalles, Salt Lake City', 43.279517, -116.354951
                UNION ALL
                SELECT 'us-west1, us-west4', 'North America', 'Dual-Region', 'United States', 'Oregon, Nevada', 'The Dalles, Las Vegas', 40.931855, -117.943759
                UNION ALL
                SELECT 'us-west2', 'North America', 'Region', 'United States', 'California', 'Los Angeles', 34.072382, -118.25112
                UNION ALL
                SELECT 'us-west2, us-west3', 'North America', 'Dual-Region', 'United States', 'California, Utah', 'Los Angeles, Salt Lake City', 37.463026, -115.215179
                UNION ALL
                SELECT 'us-west2, us-west4', 'North America', 'Dual-Region', 'United States', 'California, Nevada', 'Los Angeles, Las Vegas', 35.137433, -116.713346
                UNION ALL
                SELECT 'us-west3', 'North America', 'Region', 'United States', 'Utah', 'Salt Lake City', 40.768691, -111.894407
                UNION ALL
                SELECT 'us-west3, us-west4', 'North America', 'Dual-Region', 'United States', 'Utah, Nevada', 'Salt Lake City, Las Vegas', 38.48677, -113.566379
                UNION ALL
                SELECT 'us-west4', 'North America', 'Region', 'United States', 'Nevada', 'Las Vegas', 36.182547, -115.13519
                UNION ALL
                SELECT 'us-west8', 'North America', 'Region', 'United States', 'Arizona', 'Phoenix', 33.450430, -112.075676), geographic_area_location AS (
                SELECT 'Africa' as geographic_area, '-26.206619, 28.031437' as geographic_area_coordinates
                UNION ALL
                SELECT 'Asia', '26.22887736583804, 122.92055542594133'
                UNION ALL 
                SELECT 'Australia', '-35.87143312617882, 148.16338509205735'
                UNION ALL
                SELECT 'Europe', '50.419437352900566, 8.148756653935328'
                UNION ALL 
                SELECT 'India', '23.908849931246557, 74.90967780978364'
                UNION ALL
                SELECT 'Indonesia', '-6.187399000000001, 106.822633'
                UNION ALL
                SELECT 'Middle East', '28.1383073720678, 45.697549748255476'
                UNION ALL
                SELECT 'North America', '39.49067855459679, -99.26613885745078'
                UNION ALL
                SELECT 'South America', '-29.01378617077172, -58.159799449834644')
                SELECT
                ls.location,
                ls.geographic_area,
                gal.geographic_area_coordinates,
                ls.location_type,        
                ls.countries,
                ls.latitude,
                ls.longitude,
                CONCAT(CAST(ls.latitude AS STRING), ', ', CAST(ls.longitude AS STRING)) AS location_coordinates,
                CASE
                    WHEN ls.geographic_area = 'Europe' AND NOT (ls.countries LIKE ('%UK%') OR ls.countries LIKE ('%Switzerland%')) THEN 'European Union'
                    WHEN ls.countries = 'United States' THEN 'United States'
                    ELSE countries
                END AS countries_union,
                CASE 
                    WHEN ls.countries = 'United States' THEN FALSE
                    ELSE TRUE
                END AS outside_us,
                CASE
                    WHEN ls.geographic_area = 'Europe' AND NOT (ls.countries LIKE ('%UK%') OR ls.countries LIKE ('%Switzerland%')) THEN FALSE
                    ELSE TRUE
                END AS outside_eu
                FROM
                location_set AS ls 
            JOIN 
                geographic_area_location as gal
                ON ls.geographic_area = gal.geographic_area)
            SELECT

            ba.snapshotTime,
            ba.name AS bucket,	
            ba.location,	
            ba.project,
            pa.id AS project_id,
            pa.name AS project_name,
            ba.storageClass,	
            ba.public.publicAccessPrevention,
            ba.public.bucketPolicyOnly,
            ba.autoclass,	
            ba.versioning,
            ba.lifecycle,
            ba.metageneration,	
            ba.timeCreated,
            CASE
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 3650 THEN 'T011[10 years, inf)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 1825 THEN 'T010[5 years, 10 years)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 1095 THEN 'T09[3 years, 5 years)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 730 THEN 'T08[2 years, 3 years)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 365 THEN 'T07[1 year, 2 years)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 180 THEN 'T06[6 months, 1 year)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 90 THEN 'T05[3 months, 6 months)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 30 THEN 'T04[1 month, 3 months)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 15 THEN 'T03[2 weeks, 1 month)'
                WHEN DATE_DIFF(CURRENT_DATE(), DATE(ba.timeCreated), DAY) > 8 THEN 'T02[1 week, 2 weeks)'
                ELSE 'T01[Current day , 1 week)'
            END AS created_tier,
            ba.tags,
            ba.labels,
            ba.softDeletePolicy,
            ri.location_type,
            ri.geographic_area,
            ri.geographic_area_coordinates,
            ri.location_coordinates,
            ri.countries,
            ri.countries_union,
            ri.outside_us,
            ri.outside_eu

            FROM 
                bucket_attributes_latest AS ba
            JOIN
                regions_information AS ri
            ON
                ba.location = ri.location
            JOIN
                project_attributes_latest AS pa
            ON
                ba.project = pa.number
    """.format(table_name_objecta, table_name_bucket, table_name_bucket, table_name_project )

    # Create the view in BigQuery.
    view_buckets = bq_client.create_table(view_buckets)  # Make an API request.
    print(f"Created {view_buckets.table_type}: {str(view_buckets.reference)}")
   
    # Create View for Error Attribute
    # Create View for Error
    view_id_error = "{}.{}.error_attributes_view_looker".format(args.PROJECT_ID, args.DATASET_NAME)
    view_error = bigquery.Table(view_id_error)
    # The SQL query to be executed for events.
    table_name_error = "{}.{}".format(dataset, error)
    view_error.view_query = r"""
        WITH error_view AS (
            SELECT
                *
            FROM
                {}
        ), regions_information AS (
                WITH location_set AS (SELECT 'africa-south1' AS location, 'Africa' AS geographic_area, 'Region' AS location_type, 'South Africa' AS countries, 'Gauteng' AS state, 'Johannesburg' AS city, -26.206619 AS latitude, 28.031437 AS longitude
            UNION ALL
            SELECT 'asia', 'Asia', 'Multi-Region', 'Taiwan, Japan, South Korea, Singapore', 'Taiwan Province, Kanto, Kansai, Seoul Capital, West Region', 'Changhua County, Tokyo, Osaka, Seoul, Jurong West', 27.407598, 124.302272
            UNION ALL
            SELECT 'asia-east1', 'Asia', 'Region', 'Taiwan', 'Taiwan Province', 'Changhua County', 24.04955, 120.516007
            UNION ALL
            SELECT 'asia-east1, asia-southeast1', 'Asia', 'Dual-Region', 'Taiwan, Singapore', 'Taiwan Province, West Region', 'Changhua County, Jurong West', 12.827841, 111.729625
            UNION ALL
            SELECT 'asia-east2', 'Asia', 'Region', 'China', 'Hong Kong', 'Hong Kong', 22.324061, 114.171655
            UNION ALL
            SELECT 'asia-northeast1', 'Asia', 'Region', 'Japan', 'Kanto', 'Tokyo', 35.673817, 139.65123
            UNION ALL
            SELECT 'asia-northeast2', 'Asia', 'Region', 'Japan', 'Kansai', 'Osaka', 34.693925, 135.500077
            UNION ALL
            SELECT 'asia-northeast3', 'Asia', 'Region', 'South Korea', 'Seoul Capital', 'Seoul', 37.552242, 126.994724
            UNION ALL
            SELECT 'asia-south1', 'India', 'Region', 'India', 'Maharashtra', 'Mumbai', 19.07439, 72.878422
            UNION ALL
            SELECT 'asia-south1, asia-south2', 'India', 'Dual-Region', 'India', 'Maharashtra, Delhi', 'New Delhi', 23.90885, 74.909678
            UNION ALL
            SELECT 'asia-south2', 'India', 'Region', 'India', 'Delhi', 'New Delhi', 28.714557, 77.098665
            UNION ALL
            SELECT 'asia-southeast1', 'Asia', 'Region', 'Singapore', 'West Region', 'Jurong West', 1.340198, 103.709014
            UNION ALL
            SELECT 'asia-southeast2', 'Indonesia', 'Region', 'Indonesia', 'Java', 'Jakarta', -6.187399, 106.822633
            UNION ALL
            SELECT 'asia1', 'Asia', 'Dual-Region', 'Japan', 'Kanto, Kansai', 'Tokyo, Osaka', 35.201581, 137.563135
            UNION ALL
            SELECT 'australia-southeast1', 'Australia', 'Region', 'Australia', 'New South Wales', 'Sidney', -33.864912, 151.207943
            UNION ALL
            SELECT 'australia-southeast1, australia-southeast2', 'Australia', 'Dual-Region', 'Australia', 'New South Wales, Victoria', 'Sidney, Melbourne', -35.871433, 148.163385
            UNION ALL
            SELECT 'australia-southeast2', 'Australia', 'Region', 'Australia', 'Victoria', 'Melbourne', -37.797206, 144.963901
            UNION ALL
            SELECT 'eu', 'Europe', 'Multi-Region', 'Poland, Finland, Spain, Belgium, Germany, Netherlands, Italy, France', 'Masovian Voivodeship, Kymenlaakso, Community of Madrid, Wallonia, Hesse, Groningen, Lombardy, Ile-de-France', 'Warsaw, Hamina, Madrid, Saint-Ghislain, Frankfurt am Main, Eemshaven, Milan, Paris', 50.552525, 8.497373
            UNION ALL
            SELECT 'eur4', 'Europe', 'Dual-Region', 'Finland, Netherlands', 'Kymenlaakso, Groningen', 'Hamina, Eemshaven', 57.415689, 16.025184
            UNION ALL
            SELECT 'eur5', 'Europe', 'Dual-Region', 'Belgium, UK', 'Wallonia, Greater London', 'Saint-Ghislain, London', 51.007176, 1.867803
            UNION ALL
            SELECT 'eur7', 'Europe', 'Dual-Region', 'UK, Germany', 'Greater London, Hesse', 'London, Frankfurt am Main', 50.894615, 4.344756
            UNION ALL
            SELECT 'eur8', 'Europe', 'Dual-Region', 'Germany, Switzerland', 'Hesse, Canton of Zurich', 'Frankfurt am Main, Zurich', 48.7453, 8.610991
            UNION ALL
            SELECT 'europe-central2', 'Europe', 'Region', 'Poland', 'Masovian Voivodeship', 'Warsaw', 52.238354, 21.009223
            UNION ALL
            SELECT 'europe-central2, europe-north1', 'Europe', 'Dual-Region', 'Poland, Finland', 'Masovian Voivodeship, Kymenlaakso', 'Warsaw, Hamina', 56.443665, 23.760607
            UNION ALL
            SELECT 'europe-central2, europe-southwest1', 'Europe', 'Dual-Region', 'Poland, Spain', 'Masovian Voivodeship, Community of Madrid', 'Warsaw, Madrid', 46.993183, 7.290628
            UNION ALL
            SELECT 'europe-central2, europe-west1', 'Europe',  'Dual-Region', 'Poland, Belgium', 'Masovian Voivodeship, Wallonia', 'Warsaw, Saint-Ghislain', 51.670086, 12.246141
            UNION ALL
            SELECT 'europe-central2, europe-west3', 'Europe', 'Dual-Region', 'Poland, Germany', 'Masovian Voivodeship, Hesse', 'Warsaw, Frankfurt am Main', 51.338001, 14.703662
            UNION ALL
            SELECT 'europe-central2, europe-west4', 'Europe', 'Dual-Region', 'Poland, Netherlands', 'Masovian Voivodeship, Groningen', 'Warsaw, Eemshaven', 53.049773, 14.02046
            UNION ALL
            SELECT 'europe-central2, europe-west8', 'Europe', 'Dual-Region', 'Poland, Italy', 'Masovian Voivodeship, Lombardy', 'Warsaw, Milan', 49.004511, 14.693853
            UNION ALL
            SELECT 'europe-central2, europe-west9', 'Europe', 'Dual-Region', 'Poland, France', 'Masovian Voivodeship, Ile-de-France', 'Warsaw, Paris', 50.922267, 11.342654
            UNION ALL
            SELECT 'europe-north1', 'Europe', 'Region', 'Finland', 'Kymenlaakso', 'Hamina', 60.573043, 27.190688
            UNION ALL
            SELECT 'europe-north1, europe-southwest1', 'Europe', 'Dual-Region', 'Finland, Spain', 'Kymenlaakso, Community of Madrid', 'Hamina, Madrid', 51.479744, 8.333168
            UNION ALL
            SELECT 'europe-north1, europe-west1', 'Europe', 'Dual-Region', 'Finland, Belgium', 'Kymenlaakso, Wallonia', 'Hamina, Saint-Ghislain', 56.070808, 13.978923
            UNION ALL
            SELECT 'europe-north1, europe-west3', 'Europe', 'Dual-Region', 'Finland, Germany', 'Kymenlaakso, Hesse', 'Hamina, Frankfurt am Main', 55.687379, 16.701199
            UNION ALL
            SELECT 'europe-north1, europe-west8', 'Europe', 'Dual-Region', 'Finland, Italy', 'Kymenlaakso, Lombardy', 'Hamina, Milan', 53.351265, 16.588117
            UNION ALL
            SELECT 'europe-north1, europe-west9', 'Europe', 'Dual-Region', 'Finland, France', 'Kymenlaakso, Ile-de-France', 'Hamina, Paris', 55.339813, 12.942202
            UNION ALL
            SELECT 'europe-southwest1', 'Europe', 'Region', 'Spain', 'Community of Madrid', 'Madrid', 40.423203, -3.707017
            UNION ALL
            SELECT 'europe-southwest1, europe-west1', 'Europe', 'Dual-Region', 'Spain, Belgium', 'Community of Madrid, Wallonia', 'Madrid, Saint-Ghislain', 45.508623, -0.281364
            UNION ALL
            SELECT 'europe-southwest1, europe-west3', 'Europe', 'Dual-Region', 'Spain, Germany', 'Community of Madrid, Hesse', 'Madrid, Frankfurt am Main', 45.434948, 1.955968
            UNION ALL
            SELECT 'europe-southwest1, europe-west4', 'Europe', 'Dual-Region', 'Spain, Netherlands', 'Community of Madrid, Groningen', 'Madrid, Eemshaven', 47.050201, 0.918886
            UNION ALL
            SELECT 'europe-southwest1, europe-west8', 'Europe', 'Dual-Region', 'Spain, Italy', 'Community of Madrid, Lombardy', 'Madrid, Milan', 43.127066, 2.471997
            UNION ALL
            SELECT 'europe-southwest1, europe-west9', 'Europe', 'Dual-Region', 'Spain, France', 'Community of Madrid, Ile-de-France', 'Madrid, Paris', 44.681187, -0.898924
            UNION ALL
            SELECT 'europe-west1', 'Europe', 'Region', 'Belgium', 'Wallonia', 'Saint-Ghislain', 50.471449, 3.817129
            UNION ALL
            SELECT 'europe-west1, europe-west3', 'Europe', 'Dual-Region', 'Belgium, Germany', 'Wallonia, Hesse', 'Saint-Ghislain, Frankfurt am Main', 50.317925, 6.259341
            UNION ALL
            SELECT 'europe-west1, europe-west4', 'Europe', 'Dual-Region', 'Belgium, Netherlands', 'Wallonia, Groningen', 'Saint-Ghislain, Eemshaven', 51.964664, 5.276023
            UNION ALL
            SELECT 'europe-west1, europe-west8', 'Europe', 'Dual-Region', 'Belgium, Italy', 'Wallonia, Lombardy', 'Saint-Ghislain, Milan', 48.001489, 6.629576
            UNION ALL
            SELECT 'europe-west1, europe-west9', 'Europe', 'Dual-Region', 'Belgium, France', 'Wallonia, Ile-de-France', 'Saint-Ghislain, Paris', 49.66779, 3.071821
            UNION ALL
            SELECT 'europe-west10', 'Europe', 'Region', 'Germany', 'Berlin', 'Berlin', 52.523101, 13.401341
            UNION ALL
            SELECT 'europe-west12', 'Europe', 'Region', 'Italy', 'Piedmont', 'Turin', 45.211965, 7.379327
            UNION ALL
            SELECT 'europe-west2', 'Europe', 'Region', 'UK', 'Greater London', 'London', 51.509726, -0.125643
            UNION ALL
            SELECT 'europe-west3', 'Europe', 'Region', 'Germany', 'Hesse', 'Frankfurt am Main', 50.11361, 8.683244
            UNION ALL
            SELECT 'europe-west3, europe-west4', 'Europe', 'Dual-Region', 'Germany, Netherlands', 'Hesse, Groningen', 'Frankfurt am Main, Eemshaven', 51.779731, 7.793091
            UNION ALL
            SELECT 'europe-west3, europe-west8', 'Europe', 'Dual-Region', 'Germany, Italy', 'Hesse, Lombardy', 'Frankfurt am Main, Milan', 47.791677, 8.943693
            UNION ALL
            SELECT 'europe-west3, europe-west9', 'Europe', 'Dual-Region', 'Germany, France', 'Hesse, Ile-de-France', 'Frankfurt am Main, Paris', 49.529773, 5.476431
            UNION ALL
            SELECT 'europe-west4', 'Europe', 'Region', 'Netherlands', 'Groningen', 'Eemshaven', 53.438615, 6.834814
            UNION ALL
            SELECT 'europe-west4, europe-west8', 'Europe', 'Dual-Region', 'Netherlands, Italy', 'Groningen, Lombardy', 'Eemshaven, Milan', 49.459807, 8.103901
            UNION ALL
            SELECT 'europe-west4, europe-west9', 'Europe', 'Dual-Region', 'Netherlands, France', 'Groningen, Ile-de-France', 'Eemshaven, Paris', 51.170439, 4.481469
            UNION ALL
            SELECT 'europe-west6', 'Europe', 'Region', 'Switzerland', 'Canton of Zurich', 'Zurich', 47.376947, 8.542569
            UNION ALL
            SELECT 'europe-west8', 'Europe', 'Region', 'Italy', 'Lombardy', 'Milan', 45.469205, 9.181849
            UNION ALL
            SELECT 'europe-west8, europe-west9', 'Europe', 'Dual-Region', 'Italy, France', 'Lombardy, Ile-de-France', 'Milan, Paris', 47.215085, 5.875479
            UNION ALL
            SELECT 'europe-west9', 'Europe', 'Region', 'France', 'Ile-de-France', 'Milan', 48.859503, 2.350808
            UNION ALL
            SELECT 'me-central1', 'Middle East', 'Region', 'Qatar', 'Doha', 'Doha', 25.295093, 51.531892
            UNION ALL
            SELECT 'me-central2', 'Middle East', 'Region', 'Saudi Arabia', 'Eastern Province', 'Dammam', 26.421517, 50.087281
            UNION ALL
            SELECT 'me-west1', 'Middle East', 'Region', 'Israel', 'Tel Aviv', 'Tel Aviv', 32.088196, 34.780762
            UNION ALL
            SELECT 'nam4', 'North America', 'Dual-Region', 'United States', 'Iowa, South Carolina', 'Council Bluffs, Moncks Corner', 37.495021, -87.509075
            UNION ALL
            SELECT 'northamerica-northeast1', 'North America', 'Region', 'Canada', 'Quebec', 'Montreal', 45.498936, -73.564944
            UNION ALL
            SELECT 'northamerica-northeast1, northamerica-northeast2', 'North America', 'Dual-Region', 'Canada', 'Quebec,Ontario', 'Montreal, Toronto', 44.613403, -76.519054
            UNION ALL
            SELECT 'northamerica-northeast2', 'North America', 'Region', 'Canada', 'Ontario', 'Toronto', 43.65407, -79.380818
            UNION ALL
            SELECT 'southamerica-east1', 'South America', 'Region', 'Brazil', 'Sao Paulo', 'Osasco', -23.532524, -46.788355
            UNION ALL
            SELECT 'southamerica-west1', 'South America', 'Region', 'Chile', 'Santiago Province', 'Santiago', -33.440271, -70.671093
            UNION ALL
            SELECT 'us', 'North America', 'Multi-Region', 'United States', 'Iowa, South Carolina, Virginia, Ohio, Texas, Oregon, California, Utah, Nevada', 'Council Bluffs, Moncks Corner, Ashburn, Columbus, Dallas, The Dalles, Los Angeles, Salt Lake City, Las Vegas', 39.226851, -99.750223
            UNION ALL
            SELECT 'us-central1', 'North America', 'Region', 'United States', 'Iowa', 'Council Bluffs', 41.264954, -95.860417
            UNION ALL
            SELECT 'us-central1, us-east4', 'North America', 'Dual-Region', 'United States', 'Iowa, Virginia', 'Council Bluffs, Ashburn', 40.520283, -86.522509
            UNION ALL
            SELECT 'us-central1, us-east5', 'North America', 'Dual-Region', 'United States', 'Iowa, Ohio', 'Council Bluffs, Columbus', 40.793702, -89.367268
            UNION ALL
            SELECT 'us-central1, us-south1', 'North America', 'Dual-Region', 'United States', 'Iowa, Dallas', 'Council Bluffs, Dallas', 37.031975, -96.356595
            UNION ALL
            SELECT 'us-central1, us-west1', 'North America', 'Dual-Region', 'United States', 'Iowa, Oregon', 'Council Bluffs, The Dalles', 44.137471, -108.060859
            UNION ALL
            SELECT 'us-central1, us-west2', 'North America', 'Dual-Region', 'United States', 'Iowa, California', 'Council Bluffs, Los Angeles', 38.201122, -107.605979
            UNION ALL
            SELECT 'us-central1, us-west3', 'North America', 'Dual-Region', 'United States', 'Iowa, Utah', 'Council Bluffs, Salt Lake City', 41.295649, -103.90781
            UNION ALL
            SELECT 'us-central1, us-west4', 'North America', 'Dual-Region', 'United States', 'Iowa, Nevada', 'Council Bluffs, Las Vegas', 39.121295, -105.844031
            UNION ALL
            SELECT 'us-east1', 'North America', 'Region', 'United States', 'South Carolina', 'Moncks Corner', 33.196352, -80.012519
            UNION ALL
            SELECT 'us-east1, us-east4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Virginia', 'Moncks Corner, Ashburn', 36.127768, -78.797054
            UNION ALL
            SELECT 'us-east1, us-east5', 'North America', 'Dual-Region', 'United States', 'South Carolina, Ohio', 'Moncks Corner, Columbus', 36.589948, -81.440579
            UNION ALL
            SELECT 'us-east1, us-south1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Texas', 'Moncks Corner, Dallas', 33.279208, -88.425525
            UNION ALL
            SELECT 'us-east1, us-west1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Oregon', 'Moncks Corner, The Dalles', 41.248379, -98.677918
            UNION ALL
            SELECT 'us-east1, us-west2', 'North America', 'Dual-Region', 'United States', 'South Carolina, California', 'Moncks Corner, Los Angeles', 35.149434, -99.030803
            UNION ALL
            SELECT 'us-east1, us-west3', 'North America', 'Dual-Region', 'United States', 'South Carolina, Utah', 'Moncks Corner, Salt Lake City', 38.064993, -95.137921
            UNION ALL
            SELECT 'us-east1, us-west4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Nevada', 'Moncks Corner, Las Vegas', 35.979049, -97.246718
            UNION ALL
            SELECT 'us-east4', 'North America', 'Region', 'United States', 'Virginia', 'Ashburn', 39.045953, -77.487424
            UNION ALL
            SELECT 'us-east4, us-south1', 'North America', 'Dual-Region', 'United States', 'Virginia, Texas', 'Ashburn, Dallas', 36.310235, -87.529362
            UNION ALL
            SELECT 'us-east4, us-west1', 'North America', 'Dual-Region', 'United States', 'Virginia, Oregon', 'Ashburn, The Dalles', 44.449481, -98.137685
            UNION ALL
            SELECT 'us-east4, us-west2', 'North America', 'Dual-Region', 'United States', 'Virginia, California', 'Ashburn, Los Angeles', 38.345123, -98.55482
            UNION ALL
            SELECT 'us-east4, us-west3', 'North America', 'Dual-Region', 'United States', 'Virginia, Utah', 'Ashburn, Salt Lake City', 41.202492, -94.46785
            UNION ALL
            SELECT 'us-east4, us-west4', 'North America', 'Dual-Region', 'United States', 'Virginia, Nevada', 'Ashburn, Las Vegas', 39.146327, -96.687428
            UNION ALL
            SELECT 'us-east5', 'North America', 'Region', 'United States', 'Ohio', 'Columbus', 39.964944, -82.99977
            UNION ALL
            SELECT 'us-east7', 'North America', 'Region', 'United States', 'Alabama', NULL, NULL, NULL
            UNION ALL
            SELECT 'us-south1', 'North America', 'Region', 'United States', 'Texas', 'Dallas', 32.797148, -96.800281
            UNION ALL
            SELECT 'us-south1, us-west1', 'North America', 'Dual-Region', 'United States', 'Texas, Oregon', 'Dallas, The Dalles', 39.83593, -107.859488
            UNION ALL
            SELECT 'us-south1, us-west2', 'North America', 'Dual-Region', 'United States', 'Texas, California', 'Dallas, Los Angeles', 33.900635, -107.445958
            UNION ALL
            SELECT 'us-south1, us-west3', 'North America', 'Dual-Region', 'United States', 'Texas, Utah', 'Dallas, Salt Lake City', 37.021621, -103.951912
            UNION ALL
            SELECT 'us-south1, us-west4', 'North America', 'Dual-Region', 'United States', 'Texas, Nevada', 'Dallas, Las Vegas', 34.83426, -105.780008
            UNION ALL
            SELECT 'us-west1', 'North America', 'Region', 'United States', 'Oregon', 'The Dalles', 45.602508, -121.184333
            UNION ALL
            SELECT 'us-west1, us-west2', 'North America', 'Dual-Region', 'United States', 'Oregon, California', 'The Dalles, Los Angeles', 39.846614, -119.594169
            UNION ALL
            SELECT 'us-west1, us-west3', 'North America', 'Dual-Region', 'United States', 'Oregon, Utah', 'The Dalles, Salt Lake City', 43.279517, -116.354951
            UNION ALL
            SELECT 'us-west1, us-west4', 'North America', 'Dual-Region', 'United States', 'Oregon, Nevada', 'The Dalles, Las Vegas', 40.931855, -117.943759
            UNION ALL
            SELECT 'us-west2', 'North America', 'Region', 'United States', 'California', 'Los Angeles', 34.072382, -118.25112
            UNION ALL
            SELECT 'us-west2, us-west3', 'North America', 'Dual-Region', 'United States', 'California, Utah', 'Los Angeles, Salt Lake City', 37.463026, -115.215179
            UNION ALL
            SELECT 'us-west2, us-west4', 'North America', 'Dual-Region', 'United States', 'California, Nevada', 'Los Angeles, Las Vegas', 35.137433, -116.713346
            UNION ALL
            SELECT 'us-west3', 'North America', 'Region', 'United States', 'Utah', 'Salt Lake City', 40.768691, -111.894407
            UNION ALL
            SELECT 'us-west3, us-west4', 'North America', 'Dual-Region', 'United States', 'Utah, Nevada', 'Salt Lake City, Las Vegas', 38.48677, -113.566379
            UNION ALL
            SELECT 'us-west4', 'North America', 'Region', 'United States', 'Nevada', 'Las Vegas', 36.182547, -115.13519
            UNION ALL
            SELECT 'us-west8', 'North America', 'Region', 'United States', 'Arizona', 'Phoenix', 33.450430, -112.075676), geographic_area_location AS (
            SELECT 'Africa' as geographic_area, '-26.206619, 28.031437' as geographic_area_coordinates
            UNION ALL
            SELECT 'Asia', '26.22887736583804, 122.92055542594133'
            UNION ALL 
            SELECT 'Australia', '-35.87143312617882, 148.16338509205735'
            UNION ALL
            SELECT 'Europe', '50.419437352900566, 8.148756653935328'
            UNION ALL 
            SELECT 'India', '23.908849931246557, 74.90967780978364'
            UNION ALL
            SELECT 'Indonesia', '-6.187399000000001, 106.822633'
            UNION ALL
            SELECT 'Middle East', '28.1383073720678, 45.697549748255476'
            UNION ALL
            SELECT 'North America', '39.49067855459679, -99.26613885745078'
            UNION ALL
            SELECT 'South America', '-29.01378617077172, -58.159799449834644')
            SELECT
            ls.location,
            ls.geographic_area,
            gal.geographic_area_coordinates,
            ls.location_type,        
            ls.countries,
            ls.latitude,
            ls.longitude,
            CONCAT(CAST(ls.latitude AS STRING), ', ', CAST(ls.longitude AS STRING)) AS location_coordinates,
            CASE
                WHEN ls.geographic_area = 'Europe' AND NOT (ls.countries LIKE ('%UK%') OR ls.countries LIKE ('%Switzerland%')) THEN 'European Union'
                WHEN ls.countries = 'United States' THEN 'United States'
                ELSE countries
            END AS countries_union,
            CASE 
                WHEN ls.countries = 'United States' THEN FALSE
                ELSE TRUE
            END AS outside_us,
            CASE
                WHEN ls.geographic_area = 'Europe' AND NOT (ls.countries LIKE ('%UK%') OR ls.countries LIKE ('%Switzerland%')) THEN FALSE
                ELSE TRUE
            END AS outside_eu
            FROM
            location_set AS ls 
        JOIN 
            geographic_area_location as gal
            ON ls.geographic_area = gal.geographic_area)
        SELECT 
            e.errorCode,
            e.errorSource,
            e.errorTime,
            e.sourceGcsLocation,
            e.bucketErrorRecord,
            ri.location_type,
            ri.geographic_area,
            ri.geographic_area_coordinates,
            ri.location_coordinates,
            ri.countries,
            ri.countries_union,
            ri.outside_us,
            ri.outside_eu
            
        FROM
            error_view AS e
        LEFT JOIN
            regions_information AS ri
        ON
            e.sourceGcsLocation = ri.location
    """.format(table_name_error)

    # Create the view in BigQuery.
    view_error = bq_client.create_table(view_error)  # Make an API request.
    print(f"Created {view_error.table_type}: {str(view_error.reference)}")
   













    # Create View for Object Attribute
    # Create View for Object
    view_id_objecta = "{}.{}.object_attributes_view_looker".format(args.PROJECT_ID, args.DATASET_NAME)
    view_objecta = bigquery.Table(view_id_objecta)
    # The SQL query to be executed for events.
    table_name_objecta = "{}.{}".format(dataset, objecta)
    view_objecta.view_query = r"""
        WITH
            distinct_snapshots AS (
            SELECT
                DISTINCT snapshotTime
            FROM
                {}
            WHERE
                snapshotTime IS NOT NULL
            INTERSECT DISTINCT
            SELECT
                DISTINCT snapshotTime
            FROM
                {}
            WHERE
                snapshotTime IS NOT NULL), object_attributes_latest AS (
            SELECT
                *
            FROM
                {}
            WHERE
                snapshotTime = (
                SELECT
                    MAX(snapshotTime)
                FROM 
                    distinct_snapshots 
                )
            ), bucket_attributes_latest AS (
            SELECT
                *
            FROM
                {}
            WHERE
                snapshotTime = (
                SELECT
                    MAX(snapshotTime)
                FROM
                    distinct_snapshots
                )
            ), project_attributes_latest AS (
            SELECT
                *
            FROM
                {}
            WHERE
                snapshotTime = (
                SELECT
                    MAX(snapshotTime)
                FROM
                    distinct_snapshots
                )
            ), regions_information AS (
                    WITH location_set AS (SELECT 'africa-south1' AS location, 'Africa' AS geographic_area, 'Region' AS location_type, 'South Africa' AS countries, 'Gauteng' AS state, 'Johannesburg' AS city, -26.206619 AS latitude, 28.031437 AS longitude
                UNION ALL
                SELECT 'asia', 'Asia', 'Multi-Region', 'Taiwan, Japan, South Korea, Singapore', 'Taiwan Province, Kanto, Kansai, Seoul Capital, West Region', 'Changhua County, Tokyo, Osaka, Seoul, Jurong West', 27.407598, 124.302272
                UNION ALL
                SELECT 'asia-east1', 'Asia', 'Region', 'Taiwan', 'Taiwan Province', 'Changhua County', 24.04955, 120.516007
                UNION ALL
                SELECT 'asia-east1, asia-southeast1', 'Asia', 'Dual-Region', 'Taiwan, Singapore', 'Taiwan Province, West Region', 'Changhua County, Jurong West', 12.827841, 111.729625
                UNION ALL
                SELECT 'asia-east2', 'Asia', 'Region', 'China', 'Hong Kong', 'Hong Kong', 22.324061, 114.171655
                UNION ALL
                SELECT 'asia-northeast1', 'Asia', 'Region', 'Japan', 'Kanto', 'Tokyo', 35.673817, 139.65123
                UNION ALL
                SELECT 'asia-northeast2', 'Asia', 'Region', 'Japan', 'Kansai', 'Osaka', 34.693925, 135.500077
                UNION ALL
                SELECT 'asia-northeast3', 'Asia', 'Region', 'South Korea', 'Seoul Capital', 'Seoul', 37.552242, 126.994724
                UNION ALL
                SELECT 'asia-south1', 'India', 'Region', 'India', 'Maharashtra', 'Mumbai', 19.07439, 72.878422
                UNION ALL
                SELECT 'asia-south1, asia-south2', 'India', 'Dual-Region', 'India', 'Maharashtra, Delhi', 'New Delhi', 23.90885, 74.909678
                UNION ALL
                SELECT 'asia-south2', 'India', 'Region', 'India', 'Delhi', 'New Delhi', 28.714557, 77.098665
                UNION ALL
                SELECT 'asia-southeast1', 'Asia', 'Region', 'Singapore', 'West Region', 'Jurong West', 1.340198, 103.709014
                UNION ALL
                SELECT 'asia-southeast2', 'Indonesia', 'Region', 'Indonesia', 'Java', 'Jakarta', -6.187399, 106.822633
                UNION ALL
                SELECT 'asia1', 'Asia', 'Dual-Region', 'Japan', 'Kanto, Kansai', 'Tokyo, Osaka', 35.201581, 137.563135
                UNION ALL
                SELECT 'australia-southeast1', 'Australia', 'Region', 'Australia', 'New South Wales', 'Sidney', -33.864912, 151.207943
                UNION ALL
                SELECT 'australia-southeast1, australia-southeast2', 'Australia', 'Dual-Region', 'Australia', 'New South Wales, Victoria', 'Sidney, Melbourne', -35.871433, 148.163385
                UNION ALL
                SELECT 'australia-southeast2', 'Australia', 'Region', 'Australia', 'Victoria', 'Melbourne', -37.797206, 144.963901
                UNION ALL
                SELECT 'eu', 'Europe', 'Multi-Region', 'Poland, Finland, Spain, Belgium, Germany, Netherlands, Italy, France', 'Masovian Voivodeship, Kymenlaakso, Community of Madrid, Wallonia, Hesse, Groningen, Lombardy, Ile-de-France', 'Warsaw, Hamina, Madrid, Saint-Ghislain, Frankfurt am Main, Eemshaven, Milan, Paris', 50.552525, 8.497373
                UNION ALL
                SELECT 'eur4', 'Europe', 'Dual-Region', 'Finland, Netherlands', 'Kymenlaakso, Groningen', 'Hamina, Eemshaven', 57.415689, 16.025184
                UNION ALL
                SELECT 'eur5', 'Europe', 'Dual-Region', 'Belgium, UK', 'Wallonia, Greater London', 'Saint-Ghislain, London', 51.007176, 1.867803
                UNION ALL
                SELECT 'eur7', 'Europe', 'Dual-Region', 'UK, Germany', 'Greater London, Hesse', 'London, Frankfurt am Main', 50.894615, 4.344756
                UNION ALL
                SELECT 'eur8', 'Europe', 'Dual-Region', 'Germany, Switzerland', 'Hesse, Canton of Zurich', 'Frankfurt am Main, Zurich', 48.7453, 8.610991
                UNION ALL
                SELECT 'europe-central2', 'Europe', 'Region', 'Poland', 'Masovian Voivodeship', 'Warsaw', 52.238354, 21.009223
                UNION ALL
                SELECT 'europe-central2, europe-north1', 'Europe', 'Dual-Region', 'Poland, Finland', 'Masovian Voivodeship, Kymenlaakso', 'Warsaw, Hamina', 56.443665, 23.760607
                UNION ALL
                SELECT 'europe-central2, europe-southwest1', 'Europe', 'Dual-Region', 'Poland, Spain', 'Masovian Voivodeship, Community of Madrid', 'Warsaw, Madrid', 46.993183, 7.290628
                UNION ALL
                SELECT 'europe-central2, europe-west1', 'Europe',  'Dual-Region', 'Poland, Belgium', 'Masovian Voivodeship, Wallonia', 'Warsaw, Saint-Ghislain', 51.670086, 12.246141
                UNION ALL
                SELECT 'europe-central2, europe-west3', 'Europe', 'Dual-Region', 'Poland, Germany', 'Masovian Voivodeship, Hesse', 'Warsaw, Frankfurt am Main', 51.338001, 14.703662
                UNION ALL
                SELECT 'europe-central2, europe-west4', 'Europe', 'Dual-Region', 'Poland, Netherlands', 'Masovian Voivodeship, Groningen', 'Warsaw, Eemshaven', 53.049773, 14.02046
                UNION ALL
                SELECT 'europe-central2, europe-west8', 'Europe', 'Dual-Region', 'Poland, Italy', 'Masovian Voivodeship, Lombardy', 'Warsaw, Milan', 49.004511, 14.693853
                UNION ALL
                SELECT 'europe-central2, europe-west9', 'Europe', 'Dual-Region', 'Poland, France', 'Masovian Voivodeship, Ile-de-France', 'Warsaw, Paris', 50.922267, 11.342654
                UNION ALL
                SELECT 'europe-north1', 'Europe', 'Region', 'Finland', 'Kymenlaakso', 'Hamina', 60.573043, 27.190688
                UNION ALL
                SELECT 'europe-north1, europe-southwest1', 'Europe', 'Dual-Region', 'Finland, Spain', 'Kymenlaakso, Community of Madrid', 'Hamina, Madrid', 51.479744, 8.333168
                UNION ALL
                SELECT 'europe-north1, europe-west1', 'Europe', 'Dual-Region', 'Finland, Belgium', 'Kymenlaakso, Wallonia', 'Hamina, Saint-Ghislain', 56.070808, 13.978923
                UNION ALL
                SELECT 'europe-north1, europe-west3', 'Europe', 'Dual-Region', 'Finland, Germany', 'Kymenlaakso, Hesse', 'Hamina, Frankfurt am Main', 55.687379, 16.701199
                UNION ALL
                SELECT 'europe-north1, europe-west8', 'Europe', 'Dual-Region', 'Finland, Italy', 'Kymenlaakso, Lombardy', 'Hamina, Milan', 53.351265, 16.588117
                UNION ALL
                SELECT 'europe-north1, europe-west9', 'Europe', 'Dual-Region', 'Finland, France', 'Kymenlaakso, Ile-de-France', 'Hamina, Paris', 55.339813, 12.942202
                UNION ALL
                SELECT 'europe-southwest1', 'Europe', 'Region', 'Spain', 'Community of Madrid', 'Madrid', 40.423203, -3.707017
                UNION ALL
                SELECT 'europe-southwest1, europe-west1', 'Europe', 'Dual-Region', 'Spain, Belgium', 'Community of Madrid, Wallonia', 'Madrid, Saint-Ghislain', 45.508623, -0.281364
                UNION ALL
                SELECT 'europe-southwest1, europe-west3', 'Europe', 'Dual-Region', 'Spain, Germany', 'Community of Madrid, Hesse', 'Madrid, Frankfurt am Main', 45.434948, 1.955968
                UNION ALL
                SELECT 'europe-southwest1, europe-west4', 'Europe', 'Dual-Region', 'Spain, Netherlands', 'Community of Madrid, Groningen', 'Madrid, Eemshaven', 47.050201, 0.918886
                UNION ALL
                SELECT 'europe-southwest1, europe-west8', 'Europe', 'Dual-Region', 'Spain, Italy', 'Community of Madrid, Lombardy', 'Madrid, Milan', 43.127066, 2.471997
                UNION ALL
                SELECT 'europe-southwest1, europe-west9', 'Europe', 'Dual-Region', 'Spain, France', 'Community of Madrid, Ile-de-France', 'Madrid, Paris', 44.681187, -0.898924
                UNION ALL
                SELECT 'europe-west1', 'Europe', 'Region', 'Belgium', 'Wallonia', 'Saint-Ghislain', 50.471449, 3.817129
                UNION ALL
                SELECT 'europe-west1, europe-west3', 'Europe', 'Dual-Region', 'Belgium, Germany', 'Wallonia, Hesse', 'Saint-Ghislain, Frankfurt am Main', 50.317925, 6.259341
                UNION ALL
                SELECT 'europe-west1, europe-west4', 'Europe', 'Dual-Region', 'Belgium, Netherlands', 'Wallonia, Groningen', 'Saint-Ghislain, Eemshaven', 51.964664, 5.276023
                UNION ALL
                SELECT 'europe-west1, europe-west8', 'Europe', 'Dual-Region', 'Belgium, Italy', 'Wallonia, Lombardy', 'Saint-Ghislain, Milan', 48.001489, 6.629576
                UNION ALL
                SELECT 'europe-west1, europe-west9', 'Europe', 'Dual-Region', 'Belgium, France', 'Wallonia, Ile-de-France', 'Saint-Ghislain, Paris', 49.66779, 3.071821
                UNION ALL
                SELECT 'europe-west10', 'Europe', 'Region', 'Germany', 'Berlin', 'Berlin', 52.523101, 13.401341
                UNION ALL
                SELECT 'europe-west12', 'Europe', 'Region', 'Italy', 'Piedmont', 'Turin', 45.211965, 7.379327
                UNION ALL
                SELECT 'europe-west2', 'Europe', 'Region', 'UK', 'Greater London', 'London', 51.509726, -0.125643
                UNION ALL
                SELECT 'europe-west3', 'Europe', 'Region', 'Germany', 'Hesse', 'Frankfurt am Main', 50.11361, 8.683244
                UNION ALL
                SELECT 'europe-west3, europe-west4', 'Europe', 'Dual-Region', 'Germany, Netherlands', 'Hesse, Groningen', 'Frankfurt am Main, Eemshaven', 51.779731, 7.793091
                UNION ALL
                SELECT 'europe-west3, europe-west8', 'Europe', 'Dual-Region', 'Germany, Italy', 'Hesse, Lombardy', 'Frankfurt am Main, Milan', 47.791677, 8.943693
                UNION ALL
                SELECT 'europe-west3, europe-west9', 'Europe', 'Dual-Region', 'Germany, France', 'Hesse, Ile-de-France', 'Frankfurt am Main, Paris', 49.529773, 5.476431
                UNION ALL
                SELECT 'europe-west4', 'Europe', 'Region', 'Netherlands', 'Groningen', 'Eemshaven', 53.438615, 6.834814
                UNION ALL
                SELECT 'europe-west4, europe-west8', 'Europe', 'Dual-Region', 'Netherlands, Italy', 'Groningen, Lombardy', 'Eemshaven, Milan', 49.459807, 8.103901
                UNION ALL
                SELECT 'europe-west4, europe-west9', 'Europe', 'Dual-Region', 'Netherlands, France', 'Groningen, Ile-de-France', 'Eemshaven, Paris', 51.170439, 4.481469
                UNION ALL
                SELECT 'europe-west6', 'Europe', 'Region', 'Switzerland', 'Canton of Zurich', 'Zurich', 47.376947, 8.542569
                UNION ALL
                SELECT 'europe-west8', 'Europe', 'Region', 'Italy', 'Lombardy', 'Milan', 45.469205, 9.181849
                UNION ALL
                SELECT 'europe-west8, europe-west9', 'Europe', 'Dual-Region', 'Italy, France', 'Lombardy, Ile-de-France', 'Milan, Paris', 47.215085, 5.875479
                UNION ALL
                SELECT 'europe-west9', 'Europe', 'Region', 'France', 'Ile-de-France', 'Milan', 48.859503, 2.350808
                UNION ALL
                SELECT 'me-central1', 'Middle East', 'Region', 'Qatar', 'Doha', 'Doha', 25.295093, 51.531892
                UNION ALL
                SELECT 'me-central2', 'Middle East', 'Region', 'Saudi Arabia', 'Eastern Province', 'Dammam', 26.421517, 50.087281
                UNION ALL
                SELECT 'me-west1', 'Middle East', 'Region', 'Israel', 'Tel Aviv', 'Tel Aviv', 32.088196, 34.780762
                UNION ALL
                SELECT 'nam4', 'North America', 'Dual-Region', 'United States', 'Iowa, South Carolina', 'Council Bluffs, Moncks Corner', 37.495021, -87.509075
                UNION ALL
                SELECT 'northamerica-northeast1', 'North America', 'Region', 'Canada', 'Quebec', 'Montreal', 45.498936, -73.564944
                UNION ALL
                SELECT 'northamerica-northeast1, northamerica-northeast2', 'North America', 'Dual-Region', 'Canada', 'Quebec,Ontario', 'Montreal, Toronto', 44.613403, -76.519054
                UNION ALL
                SELECT 'northamerica-northeast2', 'North America', 'Region', 'Canada', 'Ontario', 'Toronto', 43.65407, -79.380818
                UNION ALL
                SELECT 'southamerica-east1', 'South America', 'Region', 'Brazil', 'Sao Paulo', 'Osasco', -23.532524, -46.788355
                UNION ALL
                SELECT 'southamerica-west1', 'South America', 'Region', 'Chile', 'Santiago Province', 'Santiago', -33.440271, -70.671093
                UNION ALL
                SELECT 'us', 'North America', 'Multi-Region', 'United States', 'Iowa, South Carolina, Virginia, Ohio, Texas, Oregon, California, Utah, Nevada', 'Council Bluffs, Moncks Corner, Ashburn, Columbus, Dallas, The Dalles, Los Angeles, Salt Lake City, Las Vegas', 39.226851, -99.750223
                UNION ALL
                SELECT 'us-central1', 'North America', 'Region', 'United States', 'Iowa', 'Council Bluffs', 41.264954, -95.860417
                UNION ALL
                SELECT 'us-central1, us-east4', 'North America', 'Dual-Region', 'United States', 'Iowa, Virginia', 'Council Bluffs, Ashburn', 40.520283, -86.522509
                UNION ALL
                SELECT 'us-central1, us-east5', 'North America', 'Dual-Region', 'United States', 'Iowa, Ohio', 'Council Bluffs, Columbus', 40.793702, -89.367268
                UNION ALL
                SELECT 'us-central1, us-south1', 'North America', 'Dual-Region', 'United States', 'Iowa, Dallas', 'Council Bluffs, Dallas', 37.031975, -96.356595
                UNION ALL
                SELECT 'us-central1, us-west1', 'North America', 'Dual-Region', 'United States', 'Iowa, Oregon', 'Council Bluffs, The Dalles', 44.137471, -108.060859
                UNION ALL
                SELECT 'us-central1, us-west2', 'North America', 'Dual-Region', 'United States', 'Iowa, California', 'Council Bluffs, Los Angeles', 38.201122, -107.605979
                UNION ALL
                SELECT 'us-central1, us-west3', 'North America', 'Dual-Region', 'United States', 'Iowa, Utah', 'Council Bluffs, Salt Lake City', 41.295649, -103.90781
                UNION ALL
                SELECT 'us-central1, us-west4', 'North America', 'Dual-Region', 'United States', 'Iowa, Nevada', 'Council Bluffs, Las Vegas', 39.121295, -105.844031
                UNION ALL
                SELECT 'us-east1', 'North America', 'Region', 'United States', 'South Carolina', 'Moncks Corner', 33.196352, -80.012519
                UNION ALL
                SELECT 'us-east1, us-east4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Virginia', 'Moncks Corner, Ashburn', 36.127768, -78.797054
                UNION ALL
                SELECT 'us-east1, us-east5', 'North America', 'Dual-Region', 'United States', 'South Carolina, Ohio', 'Moncks Corner, Columbus', 36.589948, -81.440579
                UNION ALL
                SELECT 'us-east1, us-south1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Texas', 'Moncks Corner, Dallas', 33.279208, -88.425525
                UNION ALL
                SELECT 'us-east1, us-west1', 'North America', 'Dual-Region', 'United States', 'South Carolina, Oregon', 'Moncks Corner, The Dalles', 41.248379, -98.677918
                UNION ALL
                SELECT 'us-east1, us-west2', 'North America', 'Dual-Region', 'United States', 'South Carolina, California', 'Moncks Corner, Los Angeles', 35.149434, -99.030803
                UNION ALL
                SELECT 'us-east1, us-west3', 'North America', 'Dual-Region', 'United States', 'South Carolina, Utah', 'Moncks Corner, Salt Lake City', 38.064993, -95.137921
                UNION ALL
                SELECT 'us-east1, us-west4', 'North America', 'Dual-Region', 'United States', 'South Carolina, Nevada', 'Moncks Corner, Las Vegas', 35.979049, -97.246718
                UNION ALL
                SELECT 'us-east4', 'North America', 'Region', 'United States', 'Virginia', 'Ashburn', 39.045953, -77.487424
                UNION ALL
                SELECT 'us-east4, us-south1', 'North America', 'Dual-Region', 'United States', 'Virginia, Texas', 'Ashburn, Dallas', 36.310235, -87.529362
                UNION ALL
                SELECT 'us-east4, us-west1', 'North America', 'Dual-Region', 'United States', 'Virginia, Oregon', 'Ashburn, The Dalles', 44.449481, -98.137685
                UNION ALL
                SELECT 'us-east4, us-west2', 'North America', 'Dual-Region', 'United States', 'Virginia, California', 'Ashburn, Los Angeles', 38.345123, -98.55482
                UNION ALL
                SELECT 'us-east4, us-west3', 'North America', 'Dual-Region', 'United States', 'Virginia, Utah', 'Ashburn, Salt Lake City', 41.202492, -94.46785
                UNION ALL
                SELECT 'us-east4, us-west4', 'North America', 'Dual-Region', 'United States', 'Virginia, Nevada', 'Ashburn, Las Vegas', 39.146327, -96.687428
                UNION ALL
                SELECT 'us-east5', 'North America', 'Region', 'United States', 'Ohio', 'Columbus', 39.964944, -82.99977
                UNION ALL
                SELECT 'us-east7', 'North America', 'Region', 'United States', 'Alabama', NULL, NULL, NULL
                UNION ALL
                SELECT 'us-south1', 'North America', 'Region', 'United States', 'Texas', 'Dallas', 32.797148, -96.800281
                UNION ALL
                SELECT 'us-south1, us-west1', 'North America', 'Dual-Region', 'United States', 'Texas, Oregon', 'Dallas, The Dalles', 39.83593, -107.859488
                UNION ALL
                SELECT 'us-south1, us-west2', 'North America', 'Dual-Region', 'United States', 'Texas, California', 'Dallas, Los Angeles', 33.900635, -107.445958
                UNION ALL
                SELECT 'us-south1, us-west3', 'North America', 'Dual-Region', 'United States', 'Texas, Utah', 'Dallas, Salt Lake City', 37.021621, -103.951912
                UNION ALL
                SELECT 'us-south1, us-west4', 'North America', 'Dual-Region', 'United States', 'Texas, Nevada', 'Dallas, Las Vegas', 34.83426, -105.780008
                UNION ALL
                SELECT 'us-west1', 'North America', 'Region', 'United States', 'Oregon', 'The Dalles', 45.602508, -121.184333
                UNION ALL
                SELECT 'us-west1, us-west2', 'North America', 'Dual-Region', 'United States', 'Oregon, California', 'The Dalles, Los Angeles', 39.846614, -119.594169
                UNION ALL
                SELECT 'us-west1, us-west3', 'North America', 'Dual-Region', 'United States', 'Oregon, Utah', 'The Dalles, Salt Lake City', 43.279517, -116.354951
                UNION ALL
                SELECT 'us-west1, us-west4', 'North America', 'Dual-Region', 'United States', 'Oregon, Nevada', 'The Dalles, Las Vegas', 40.931855, -117.943759
                UNION ALL
                SELECT 'us-west2', 'North America', 'Region', 'United States', 'California', 'Los Angeles', 34.072382, -118.25112
                UNION ALL
                SELECT 'us-west2, us-west3', 'North America', 'Dual-Region', 'United States', 'California, Utah', 'Los Angeles, Salt Lake City', 37.463026, -115.215179
                UNION ALL
                SELECT 'us-west2, us-west4', 'North America', 'Dual-Region', 'United States', 'California, Nevada', 'Los Angeles, Las Vegas', 35.137433, -116.713346
                UNION ALL
                SELECT 'us-west3', 'North America', 'Region', 'United States', 'Utah', 'Salt Lake City', 40.768691, -111.894407
                UNION ALL
                SELECT 'us-west3, us-west4', 'North America', 'Dual-Region', 'United States', 'Utah, Nevada', 'Salt Lake City, Las Vegas', 38.48677, -113.566379
                UNION ALL
                SELECT 'us-west4', 'North America', 'Region', 'United States', 'Nevada', 'Las Vegas', 36.182547, -115.13519
                UNION ALL
                SELECT 'us-west8', 'North America', 'Region', 'United States', 'Arizona', 'Phoenix', 33.450430, -112.075676), geographic_area_location AS (
                SELECT 'Africa' as geographic_area, '-26.206619, 28.031437' as geographic_area_coordinates
                UNION ALL
                SELECT 'Asia', '26.22887736583804, 122.92055542594133'
                UNION ALL 
                SELECT 'Australia', '-35.87143312617882, 148.16338509205735'
                UNION ALL
                SELECT 'Europe', '50.419437352900566, 8.148756653935328'
                UNION ALL 
                SELECT 'India', '23.908849931246557, 74.90967780978364'
                UNION ALL
                SELECT 'Indonesia', '-6.187399000000001, 106.822633'
                UNION ALL
                SELECT 'Middle East', '28.1383073720678, 45.697549748255476'
                UNION ALL
                SELECT 'North America', '39.49067855459679, -99.26613885745078'
                UNION ALL
                SELECT 'South America', '-29.01378617077172, -58.159799449834644')
                SELECT
                ls.location,
                ls.geographic_area,
                gal.geographic_area_coordinates,
                ls.location_type,        
                ls.countries,
                ls.latitude,
                ls.longitude,
                CONCAT(CAST(ls.latitude AS STRING), ', ', CAST(ls.longitude AS STRING)) AS location_coordinates
                FROM
                location_set AS ls 
            JOIN 
                geographic_area_location as gal
                ON ls.geographic_area = gal.geographic_area)
            SELECT
                oa.snapshotTime,
                oa.bucket,
                oa.location,
                oa.componentCount,
                oa.contentDisposition,
                oa.contentEncoding,
                oa.contentLanguage,	
                oa.contentType,	
                oa.crc32c,	
                oa.customTime,
                oa.etag,
                oa.eventBasedHold,	
                oa.generation,
                oa.md5Hash,	
                oa.mediaLink,
                oa.metadata,
                oa.metageneration,
                oa.name,
                LENGTH(oa.name) - LENGTH(REPLACE(oa.name, "/", "")) AS prefix_number,
                REGEXP_EXTRACT(oa.name, r'^(.*\/)[^\/]+$') AS object_prefix,
                oa.selfLink,
                oa.size,
                oa.storageClass,
                oa.temporaryHold,
                oa.timeCreated,	
                oa.timeDeleted,	
                oa.updated,
                oa.timeStorageClassUpdated,
                oa.retentionExpirationTime,
                CASE
                    WHEN DATE_DIFF(DATE(oa.retentionExpirationTime), DATE(CURRENT_DATE()), DAY) > 0 
                        THEN DATE_DIFF(DATE(oa.retentionExpirationTime), DATE(CURRENT_DATE()), DAY)
                    WHEN DATE_DIFF(DATE(oa.retentionExpirationTime), DATE(CURRENT_DATE()), DAY) <= 0 
                        THEN 0
                    ELSE 0
                END AS retentionExpirationDays,
                oa.softDeleteTime,
                oa.hardDeleteTime,
                CASE
                    WHEN DATE_DIFF(DATE(oa.hardDeleteTime), DATE(CURRENT_DATE()), DAY) > 0 
                        THEN DATE_DIFF(DATE(oa.hardDeleteTime), DATE(CURRENT_DATE()), DAY)
                    WHEN DATE_DIFF(DATE(oa.hardDeleteTime), DATE(CURRENT_DATE()), DAY) <= 0
                        THEN 0
                END AS hardDeleteDays,
                oa.size/(1024) AS storage_size_KiB,
                oa.size/(1024*1024) AS storage_size_MiB,
                oa.size/(1024*1024*1024) AS storage_size_GiB,
                CASE
                    WHEN oa.size > 1125899906842624 THEN "PiB"
                    WHEN oa.size > 1099511627776 THEN "TiB"
                    WHEN oa.size > 1073741824 THEN "GiB"
                    WHEN oa.size > 1048576 THEN "MiB"
                    WHEN oa.size > 1024 THEN " KiB"
                    WHEN oa.size <= 1024 THEN "B"
                END AS object_size_unit,
                DATE_DIFF(CURRENT_DATE(),DATE(oa.timeCreated), DAY) AS date_diff_timeCreated,
                DATE_DIFF(CURRENT_DATE(),DATE(oa.retentionExpirationTime),DAY) AS date_diff_retentionExpirationTime,
                REGEXP_EXTRACT(oa.name,r'^([^\/]*\/).*') as folder,
                REGEXP_EXTRACT(oa.contentType,r'^([^\\/]*)/[^\\/]*') as contentType_prefix,
                UPPER(REGEXP_EXTRACT(oa.contentType,r'^[^\\/]*/([^\\/]*)')) as contentType_suffix,
                UPPER(REGEXP_EXTRACT(oa.name, r'([^.]+)$')) AS file_extension,
                ba.autoclass.enabled AS autoclass_enabled, 
                ba.lifecycle, 
                ba.public.publicAccessPrevention, 
                ba.versioning,
                ba.project,
                pa.id as project_id,
                pa.name as project_name,
                ba.softDeletePolicy,
                ri.geographic_area,
                ri.geographic_area_coordinates,
                ri.location_coordinates,
                ri.location_type,
                ri.countries,
                CASE
                    WHEN oa.timeDeleted IS NOT NULL 
                        THEN 'Noncurrent'
                    WHEN oa.softDeleteTime IS NOT NULL
                        THEN 'Soft-deleted'
                    ELSE
                        'Live'
                END AS object_status,
                CASE 
                    WHEN ba.softDeletePolicy IS NOT NULL AND oa.softDeleteTime IS NOT NULL THEN TRUE
                    ELSE FALSE
                END AS softDeleteActive
            FROM 
                object_attributes_latest AS oa
            JOIN
                bucket_attributes_latest AS ba
            ON
                oa.bucket = ba.name
            LEFT JOIN 
                project_attributes_latest AS pa
            ON
                ba.project = pa.number
            JOIN
            regions_information AS ri
            ON
            oa.location = ri.location
    """.format(table_name_objecta, table_name_bucket, table_name_objecta, table_name_bucket, table_name_project )

    # Create the view in BigQuery.
    view_objecta = bq_client.create_table(view_objecta)  # Make an API request.
    print(f"Created {view_objecta.table_type}: {str(view_objecta.reference)}")
def generate_datastudio_url(args):
    projectId = args.PROJECT_ID
    dataset = args.DATASET_NAME
    object_table = "object_attributes_view_looker"
    events_table = "event_view_looker"
    buckets_table = "bucket_attributes_view_looker"
    error_table =  "error_attributes_view_looker"
    output_url = report_base_url + final_url.format(projectId, args.DATASET_NAME, events_table, 
    projectId, args.DATASET_NAME, buckets_table, projectId, args.DATASET_NAME, error_table, projectId, args.DATASET_NAME, object_table)

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
                        help='The Project Id with the GCS Insisight Datasets',
                        required=True)
    parser.add_argument('-dataset',
                        dest='DATASET_NAME',
                        type=str,
                        help='The name of the dataset that cointains the GCS Insight views in Bigquery',
                        required=True)
    #parser.add_argument('-table',
    #                    dest='TABLE_NAME',
    #                    type=str,
    #                    required=True)

    args = parser.parse_args()
    print('Version of GCS_Insight.py  ' + app_version + "\n")

    
    # This is creates the views ofr now we will focus on just dashboard
    #create_view(args)
    create_bq_views(args)

    #list_tables(args)
    generate_datastudio_url(args)  # to create urls
    

# Main entry point
if __name__ == "__main__":
    main(sys.argv[1:])
