#!/usr/bin/env python
from swiftclient import Connection, ClientException
import subprocess
import logging
import ConfigParser
import re
import os

global_cfg = {'prefix': '', 'local_path': 'mirror-dir'}
configuration = {'source': dict(), 'destination': dict()}
listing = {'source': dict(), 'destination': dict()}

debug = logging.debug

def create_connection(authurl, username, password, tenant_name, region):
    cfconn = Connection(authurl,
                        username,
                        password,
                        snet=False,
                        tenant_name=tenant_name,
                        auth_version='2.0',
                        os_options={'region_name': region,
                                    'endpoint_type': 'publicURL',
                                    'service_type': 'object-store'})
    return cfconn


def load_config():
    required_params = ['authurl',
                       'username',
                       'password',
                       'tenant_name',
                       'region']
    config = ConfigParser.ConfigParser()
    config.read('config.ini')
    for endpoint in configuration.keys():
        for param in required_params:
            configuration[endpoint][param] = config.get(endpoint, param)

    return True


def build_move_list():
    move_list = dict()
    files_to_move = 0
    bytes_to_move = 0
    for container in listing['source'].keys():
        if container not in listing['destination']:
            move_list[container] = listing['source'][container]
            print "%s container not found in destination, \
                adding to list" % (container)
        else:
            for file in listing['source'][container].keys():
                if file not in listing['destination'][container] or \
                       listing['destination'][container][file]['hash'] != \
                       listing['source'][container][file]['hash']:
                    if listing['source'][container][file]['bytes'] == 0:
                        continue
                    if container not in move_list:
                        move_list[container] = dict()
                    move_list[container][file] = listing['source'][container][file]
                    files_to_move += 1
                    bytes_to_move += listing['source'][container][file]['bytes']
    return files_to_move, bytes_to_move, move_list


def get_connection(endpoint):
    c = create_connection(configuration[endpoint]['authurl'],
                          configuration[endpoint]['username'],
                          configuration[endpoint]['password'],
                          configuration[endpoint]['tenant_name'],
                          configuration[endpoint]['region'])
    return c


def get_contents(endpoint):
    c = get_connection(endpoint)
    listing[endpoint] = dict()
    contents = c.get_container('/')
    for container in contents[1]:
        if re.search(global_cfg['prefix'], container['name']) is None:
            continue
        if not container['name'] in listing[endpoint]:
            listing[endpoint][container['name']] = dict()

        contents = c.get_container(container['name'])
        for file in contents[1]:
            listing[endpoint][container['name']][file['name']] = dict()
            listing[endpoint][container['name']][file['name']]['hash'] = file['hash']
            listing[endpoint][container['name']][file['name']]['bytes'] = file['bytes']
    return True


def ensure_container(container):
    d = get_connection('destination')
    try:
        r = d.head_container(container)
    except ClientException as err:
        print "Creating destination container %s ..." % (container)
        r = d.put_container(container)


def exists_or_create(dir):
    try:
        os.stat(dir)
    except OSError:
        os.makedirs(dir)


def run_command(cmd):
    """
        wrapper for subprocess, returns command output
        and return_code
    """
    p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)
    output = p.communicate()[0]
    return_code = p.returncode
    return (output, return_code)


def get_md5(file):
    md5sum = None
    output, rc = run_command('/usr/bin/md5sum {0}'.format(file))
    try:
        md5sum = output.split()[0]
    except:
        print "ERROR: {0}".format(output)
    return md5sum


def save_to_disk(connection, container, path):
    # check if path local path exists first
    exists_or_create(global_cfg['local_path'])
    exists_or_create(global_cfg['local_path'] + "/" + container)

    remote_details = connection.head_object(container, path)
    local_file = global_cfg['local_path'] + '/' + container + '/' + path
    local_dir = local_file.split('/')[0:-1]
    local_mirror = False

    # check if file exists locally, is same size, and if
    # so that the MD5 is identical
    try:
        s = os.stat(local_file)
        if s.st_size == remote_details['content-length']:
            # gonna use size for now since md5 on all local is expensive
            # if get_md5(local_file) == remote_details['etag']:
            local_mirror = True
    except OSError:
        pass

    if not local_mirror:
        if (len(local_dir) > 0):
            exists_or_create("/".join(local_dir))
        with open(local_file, 'w') as fd:
            print "Downloading from source container ... "
            fd.write(connection.get_object(container, path)[1])
        # ensure file matches
        print "Calculating MD5 sum of retrieved file ..."
        local_md5 = get_md5(local_file)
        if not (local_md5 == remote_details['etag']):
            # abort
            print "Local mirror MD5 doesn't match, removing file %s" % \
                (local_file)
            os.remove(local_file)
            return None
        print "Local mirror MD5 sum matches remote source file ... "
    return True


def move_object(container, path, counter=1):
    s = get_connection('source')
    try:
        if save_to_disk(s, container, path):
            d = get_connection('destination')
            print "Uploading object %s/%s to destination ..." % (container, path)
            fd = open(global_cfg['local_path'] + '/' + container + '/' + path, 'r')
            d.put_object(container, path, fd.read())
        else:
            print "Download failed."
    except Exception as err:
        if counter < 3:
            print "Problem found: %s\nRetrying..." % (err)
            counter += 1
            move_object(container, path, counter)
        else:
            print "Problem found, tried too many times, giving up:\n%s" % (err)


def begin_sync(move_list, total_files, total_bytes):
    total_moved_bytes = 0
    total_moved_files = 0
    for container in move_list:
        ensure_container(container)
        for file in move_list[container].keys():
            # print "%s/%s" % (container, file)
            if move_list[container][file]['bytes'] > 0:
                print "\nProgress: %s/%s files, %s/%s bytes, %s%%" % \
                    (total_moved_files,
                     total_files,
                     total_moved_bytes,
                     total_bytes,
                     (total_moved_bytes*100.0/total_bytes))
                print "Moving object %s/%s (%s bytes) ..." % \
                    (container,
                     file,
                     move_list[container][file]['bytes'])
                move_object(container, file)
                total_moved_bytes += move_list[container][file]['bytes']
                total_moved_files += 1


if __name__ == "__main__":
    load_config()
    get_contents('source')
    get_contents('destination')
    files_to_move, bytes_to_move, move_list = build_move_list()
    if len(move_list) == 0:
        print "Destination endpoint appears to have " \
              "everything the source endpoint contains."
    else:
        begin_sync(move_list,
                   files_to_move,
                   bytes_to_move)
