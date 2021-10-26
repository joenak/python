import argparse
import os
import shutil
import subprocess
import time
import yaml

parser = argparse.ArgumentParser(description='Generates SQL deployment and rollback scripts based on yaml file.')
parser.add_argument('-d', dest='location', help='location of output files', default='c:\\tempDeploy\\', required=False)
parser.add_argument('-y', dest='yaml', help='full path to yaml file containing objects to script', default=None, required=True)
parser.add_argument('-l', dest='live', help='Live data center: city', default='atl', required=False)
args = parser.parse_args()

object_list = ['Schema', 'Table', 'View', 'Function', 'StoredProcedure']
live = args.live.lower()

error_codes = {'0':'Success',
    '1':'General error code',
    '3':'Illegal argument duplication',
    '8':'Unsatisfied argument dependency',
    '32':'Value out of range',
    '33':'Value overflow',
    '34':'Invalid value',
    '35':'Invalid license',
    '61':'Deployment warnings',
    '62':'High level parser error',
    '63':'Databases identical',
    '64':'Command line usage error',
    '65':'Data error',
    '69':'Resource unavailable',
    '70':'An unhandled exception occurred',
    '73':'Failed to create report',
    '74':'I/O error',
    '77':'Insufficient permissions',
    '79':'Datases not identical',
    '126':'SQL Server error',
    '130':'Ctrl-Break',
    '400':'Bad request',
    '402':'Not licensed',
    '499':'Activation cancelled by user',
    '500':'Unhandled exception'}

def build():
    global loc
    global roll
    global out_log

    head, tail = os.path.split(args.yaml)
    tail = tail.replace('.yaml', '')

    loc = build_folder(args.location, tail + '_' + time.strftime('%Y%m%d+%H%M%S'))
    roll = build_folder(loc, 'rollback')
    out_log = os.path.join(loc, 'script_log.txt')
    post = build_folder(loc, 'post_deploy')

    # copy default post deploy files

    yaml_dest = build_folder(loc, 'yaml')
    yaml_dest = os.path.join(yaml_dest, tail)
    shutil.copyfile(args.yaml, yaml_dest)

    return read_yaml(args.yaml)


def log(file, msg, new):
    log = open(file, 'a')
    if new == 1:
        log.write(msg + '\n')
    else:
        log.write(msg)
    log.close()


def build_folder(path, new):
    directory = os.path.join(path, new)
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def read_yaml(path):
    curdir = os.getcwd()
    full_path = os.path.join(curdir, path)
    config = ""
    with open(full_path, 'r') as stream:
        config = yaml.safe_load(stream)
    return config


def get_app1(env):
    if env == 'qa':
        server = ['qa_server']
    elif env == 'uat':
        server = ['uat_server']
    elif env == 'sb':
        server = ['sb_server']
    elif env == 'prod':
        if live == 'atl':
            server = ['atl_server1']
        elif live == 'dc2':
            server = ['dc2_server1']

    return server


def get_rpt(env):
    if env == 'qa':
        server = ['qa_server']
    elif env == 'uat':
        server = ['uat_server']
    elif env == 'sb':
        server = ['sb_server']
    elif env == 'prod':
        if live == 'atl':
            server = ['atl_server1', 'atl_server2']
        elif live == 'dc2':
            server = ['dc2_server']

    return server


def get_v(env):
    if env == 'qa':
        server = ['v_qa_server']
    elif env == 'uat':
        server = ['v_uat_server']
    elif env == 'sb':
        server = ['v_sb_server']
    elif env == 'prod':
        if live == 'atl':
            server = ['v_atl server']
        elif live == 'dc2':
            server = ['v_dc2 server']

    return server


def get_v2(object, env):
    if env == 'qa':
        server = ['v2_qa_server']
    elif env == 'uat':
        server = ['v2_uat_server']
    elif env == 'sb':
        server = ['v2_sb_server']
    elif env == 'prod':
        if object.split('.')[1] == 'data':
            if live == 'atl':
                server = ['v2_atl server']
            elif live == 'dc2':
                server = ['v2_dc2 server']
        if object.split('.')[1] == 'app1':
            if live == 'atl':
                server = ['v2app_atl server']
            elif live == 'dc2':
                server = ['v2app_dc2 server']

    return server


def get_servers(object, source, dest):
    app1_list = ['db1', 'db2', 'db3', 'db4', 'db5']
    rpt_list = ['reportDB']
    v_list = ['appV']
    v2_list = ['appV2']

    if object.split('.')[0].lower() in app1_list:
        source_servers = get_app1(source)
        dest_servers = get_app1(dest)
        source_env = 'app1'
    if object.split('.')[0].lower() in rpt_list:
        source_servers = get_rpt(source)
        dest_servers = get_rpt(dest)
        source_env = 'rpt'
    if object.split('.')[0].lower() in v_list:
        source_servers = get_v(source)
        dest_servers = get_v(dest)
        source_env = 'app1'
    if object.split('.')[0].lower() in v2_list:
        source_servers = get_v2(source)
        dest_servers = get_v2(dest)
        source_env = 'app2'

    output = []
    output.append(source_servers)
    output.append(dest_servers)
    output.append(source_env)

    return output


def build_base_cmd():
    path = os.getcwd()
    path = os.path.join(path, '..\config\conn.yaml')
    config = read_yaml(path)
    creds = config.get('script_creds')
    username = creds['username']
    pwd = creds['pwd']

    cmd = '"C:\Program Files (x86)\\Red Gate\\SQL Compare 14\\sqlcompare.exe"'
    cmd += ' /Options:AddDatabaseUseStatement,DoNotOutputCommentHeader,IgnoreComments'
    cmd += ',IgnoreConstraintNames,IgnoreDataCompression,IgnoreExtenddedProperties,IgnoreFillFactor'
    cmd += ',IgnoreIndexes,IgnoreNotForReplication,IgnoreQuotedIdentifiersAndAnsiNullSettings'
    cmd += ',IgnoreSquareBrackets,IgnoreStatistics,IgnoreSystemNamedConstraintNames'
    cmd += ',IgnoreWhiteSpace,IgnoreWithNocheck,NoTransactions'
    cmd += ' /Quiet /Force /UserName1:' + username + ' /Password1:' + pwd
    cmd += ' /UserName2:' + username + ' /Password2:' + pwd

    return cmd


def run_sql_compare(cmd):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = p.communicate()
    returncode = p.returncode
    ret = '......results: ' + str(returncode) + ' - ' + error_codes.get(str(returncode))
    lot(out_log, ret, 1)
    print(ret)


def generate_files():
    config = build()
    n = 0
    base_cmd = build_base_cmd()
    for item in object_list:
        if config.get(item):
            for i in config.get(item):
                n += 1
                format_n = '0000' + str(n)
                db = i.split('.')[0]
                schema = i.split('.')[1]

                cmd = ' /Database1:' + db + ' /Database2:' + db

                if item == 'Schema':
                    cmd += ' /Include:' + item + ' /Include:' + item + ':' + schema
                    file = '_deploy_' + format_n[-4:] + '_' + item + '_' + db + '_' + schema + '.sql'
                else:
                    obj = i.split('.')[2]
                    cmd += ' /Include:' + item + ' /Include:' + item + ':\[' + schema + '\]\.\[' + obj + '\]'
                    file = '_deploy_' + format_n[-4:] + '_' + item + '_' + db + '_' + schema + '_' + obj + '.sql'

                servers = get_servers(i, config.get('Source').lower(), config.get('Destination').lower())
                file = servers[2] + file
                cmd2 = ' /ScriptFile:' + os.path.join(loc, file)
                log(out_log, file, 0)
                print(file)
                srv = ' /Server1:' + servers[0][0] + ' /Server2:' + servers[1][0]
                run_sql_compare(base_cmd + srv + cmd + cmd2)

                file = file.replace('deploy', 'rollback')
                cmd2 = ' /ScriptFile:' + os.path.join(roll, file)
                log(out_log, file, 0)
                print(file)
                srv = ' /Server1:' + servers[1][0] + ' /Server2:' + servers[0][0]
                run_sql_compare(base_cmd + server + cmd + cmd2)


# add main
print('....starting....')
generate_files()
print('....finished....')
