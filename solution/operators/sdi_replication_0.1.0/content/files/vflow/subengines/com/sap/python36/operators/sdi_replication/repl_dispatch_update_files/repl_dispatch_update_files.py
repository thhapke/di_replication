import io

import os
import time

import subprocess

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name'] :
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.1.0"

            operator_description = "Repl. Dispatch Update Files"
            operator_name = 'repl_dispatch_update_files'
            operator_description_long = "Dispatch update files."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



files_list = list()
dir = ''
file_index = 0

def on_files_process(msg) :
    global files_list
    global dir
    files_list = msg.body['update_files']
    dir = msg.body['dir']
    process(msg)

def process(msg) :
    global files_list
    global file_index
    global dir

    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_update_files'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info('Send Files counter {}/{}'.format(file_index, len(files_list)))
    logger.debug(att)

    att['message.index_update'] = file_index
    att['message.last_update'] = False
    if len(files_list) == 0 :
        logger.warning('No files to process - ending pipeline')
        api.send(outports[0]['name'], log_stream.getvalue())
        att['message.last_update'] = True
        api.send(outports[1]['name'], api.Message(attributes=att, body=None))
        return 0

    if file_index == len(files_list) - 1 :
        att['message.last_update'] = True
    att['file']['path'] = os.path.join(dir,files_list[file_index])

    logger.info('Send File: {} ({}/{})'.format(files_list[file_index],file_index, len(files_list)))
    api.send(outports[1]['name'], api.Message(attributes=att,body=files_list[file_index]))
    api.send(outports[0]['name'], log_stream.getvalue())
    file_index += 1


inports = [{'name': 'files', 'type': 'message.file',"description":"List of files"},
           {'name': 'trigger', 'type': 'message.*',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'file', 'type': 'message.file',"description":"file"}]


api.set_port_callback(inports[0]['name'], on_files_process)
api.set_port_callback(inports[1]['name'], process)


def test_operator() :

    att = {'operator':'collect_files','file':{'path':'/adfg/asdf.cfg'}}

    files = {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": ["22222_TEST_TABLE_17.csv", "11111_TEST_TABLE_17.csv", "33333_TEST_TABLE_17.csv"],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "key": "TEST_TABLE_17_primary_keys.csv",
            "consistency": "",
            "misc": []
        }
    files['update_files'] = sorted(files['update_files'])
    on_files_process(api.Message(attributes=att, body=files))
    for i in range(1,len(files['update_files'])) :
        process(api.Message(attributes=att,body=''))

    for m in api.queue :
        print(m.attributes)
        print(m.body)

