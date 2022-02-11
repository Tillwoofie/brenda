# Brenda -- Blender render tool for Amazon Web Services
# Copyright (C) 2022 Tillwoofie (rvbcaboose@gmail.com)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os, time, datetime, calendar, urllib.request, urllib.error, urllib.parse
import boto3
from brenda import utils
from brenda.error import ValueErrorRetry
#gcp-related
import googleapiclient.discovery
#import google-cloud-compute

def gcp_creds(conf):
    return {
        'google_application_credentials' : conf['GOOGLE_APPLICATION_CREDENTIALS'],
    }

def get_conn(conf, resource_type="compute", api_version="v1"):
    api_key = conf['GOOGLE_APPLICATION_CREDENTIALS']
    conn = googleapiclient.build(resource_type, api_version, developerKey=api_key)
    if not conn:
        raise ValueErrorRetry("Could not establish {} connection to region {}".format(resource_type, region))
    return conn

def get_ce_instances_from_conn(conn, project_name, zone_name, instance_ids=None,):
    filter_args = {}

    all_instances = conn.instances().list(project=project_name, zone=zone_name).execute()

    if (all_instances.get('items')):
        if instance_ids:
            pass


    if instance_ids:
        filter_args['InstanceIds'] = instance_ids
    reservations = conn.instances.filter(**filter_args)
    return [r for r in reservations]

def get_ce_instances(conf, instance_ids=None):
    conn = get_conn(conf, "compute")
    project = conf['GCP_PROJECT_NAME']
    zone = conf['GCP_ZONE_NAME']
    return get_ce_instances_from_conn(conn, project, zone, instance_ids)

def config_file_name():
    config = os.environ.get("BRENDA_CONFIG")
    if not config:
        home = os.path.expanduser("~")
        config = os.path.join(home, ".brenda.conf")
    return config