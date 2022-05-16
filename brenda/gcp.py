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
from google.cloud import storage


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


def get_name_for_instance(conf):
    #TODO: Implement to get a "next" instance name.
    pass


def parse_gs_url(url):
    if url.startswith('gs://'):
        return url[5:].split('/', 1)


def gs_get(conf, gsurl, dest, etag=None):
    """
    High-speed download from S3 that can use multiple simultaneous
    download threads to optimize the downloading of a single file.
    S3 file is given in s3url (using s3://BUCKET/FILE naming
    convention) and will be saved in dest.  If etag from previous
    download is provided, and file hasn't changed since then, don't
    download the file and instead raise an exception of type
    paracurl.Exception where the first element of the exception
    tuple == paracurl.PC_ERR_ETAG_MATCH.  Returns tuple of
    (file_length, etag).
    """

    paracurl_kw = {
        'max_threads' : int(conf.get('CURL_MAX_THREADS', '16')),
        'n_retries' : int(conf.get('CURL_N_RETRIES', '4')),
        'debug' : int(conf.get('CURL_DEBUG', '1'))
        }
    if etag:
        paracurl_kw['etag'] = etag
    gstup = parse_gs_url(gsurl)
    if not gstup or len(gstup) != 2:
        raise ValueError("gs_get: bad gs url: %r" % (gsurl,))
    # Not sure if we need this?
    # conn = get_conn(conf, "s3")
    conn = storage.Client()
    buck = conn.get_bucket(gstup[0])
    # object_ref = conn.Object(buck.name,s3tup[1])
    # key = object_ref.get()
    # etag = key['ETag']
    # content_len = key['ContentLength']
    # body = key['Body'].read()
    # with open(dest, 'wb') as file_out:
    #     file_out.write(body)
    # return content_len, etag
    blob = buck.blob(gstup[1])
    if etag is not None:
        blob.download_to_filename(dest, if_etag_not_match=etag)
    else:
        blob.download_to_filename(dest)
    ret_etag = blob.etag
    content_len = blob.size
    return content_len, ret_etag


def put_gs_file(conf, bucktup, path, gsname):
    """
    bucktup is the return tuple of get_gs_output_bucket_name
    """

    # s3 stuff
    # conn = get_conn(conf, "s3")
    #
    # object_ref = conn.Object(bucktup[1][0],bucktup[1][1] + s3name)
    # object_ref.put(Body=open(path, 'rb'), StorageClass='REDUCED_REDUNDANCY')

    conn = storage.Client()
    buck = conn.bucket(bucktup[1][0])
    blob = buck.blob(bucktup[1][1] + gsname)

    blob.upload_from_filename(path, content_type='image/aces')


def get_gs_output_bucket_name(conf):
    bn = conf.get('RENDER_OUTPUT')
    if not bn:
        raise ValueError("RENDER_OUTPUT not defined in configuration")
    bn = parse_gs_url(bn)
    if not bn:
        raise ValueError("RENDER_OUTPUT must be an gs:// URL")
    if len(bn) == 1:
        bn.append('')
    elif len(bn) == 2 and bn[1] and bn[1][-1] != '/':
        bn[1] += '/'
    return bn


def format_gs_url(bucktup, gsname):
    """
    bucktup is the return tuple of get_s3_output_bucket_name
    """
    return "gs://%s/%s%s" % (bucktup[1][0], bucktup[1][1], gsname)


def get_gs_output_bucket(conf):
    bn = get_gs_output_bucket_name(conf)
    # conn = get_conn(conf, "s3")
    # buck = conn.Bucket(bn[0])
    conn = storage.Client()
    buck = conn.bucket(bn[0])
    return buck, bn
