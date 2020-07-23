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

            operator_description = "Repl. Dispatch Files"
            operator_name = 'repl_dispatch_files'
            operator_description_long = "Dispatch files."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



files_list = list()
file_index = 0

def on_files_process(msg) :
    global files_list
    files_list = msg.body
    process(msg)

def process(msg) :
    global files_list
    global file_index

    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_files'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info('Send Files counter {}/{}'.format(file_index, len(files_list)))

    if len(files_list) == 0:
        err_statement = 'No files to process!'
        logger.error(err_statement)
        raise ValueError(err_statement)

    if file_index == len(files_list):
        logger.info('No files to process - ending pipeline')
        api.send(outports[0]['name'], log_stream.getvalue())
        api.send(outports[2]['name'], api.Message(attributes=att, body=None))
        return 0

    files_list[file_index].attributes['message.batchIndex'] = file_index
    files_list[file_index].attributes['message.lastBatch'] = False

    # get table and schema from folder structure
    file_path = files_list[file_index].attributes['file']['path']
    files_list[file_index].attributes['table_name'] = os.path.basename(os.path.dirname(file_path))
    files_list[file_index].attributes['schema_name'] = os.path.basename(os.path.dirname(os.path.dirname(file_path)))

    logger.info('Send File: {} ({}/{})'.format(att['file']['path'], file_index, len(files_list)))
    api.send(outports[1]['name'], files_list[file_index])

    api.send(outports[0]['name'], log_stream.getvalue())
    file_index += 1


inports = [{'name': 'files', 'type': 'message.file',"description":"List of files"},
           {'name': 'trigger', 'type': 'message.*',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'file', 'type': 'message.file',"description":"file"},
            {'name': 'limit', 'type': 'message',"description":"Limit"}]


api.set_port_callback(inports[0]['name'], on_files_process)
api.set_port_callback(inports[1]['name'], process)


def test_operator() :

    file1 = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"ADL_THH"},\
                     "isDir":False,"modTime":"2020-07-21T10:13:01Z","path":"/replication/REPLICATION/TEST_TABLE_17/TEST_TABLE_17.csv","size":67},\
             "message.batchIndex":39,"message.batchSize":1,"message.lastBatch":False}
    msg1 = api.Message(attributes=file1,body=file1)
    file2 = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"ADL_THH"},\
                     "isDir":False,"modTime":"2020-07-21T10:10:04Z","path":"/replication/REPLICATION/TEST_TABLE_19/TEST_TABLE_19.csv","size":82},\
             "message.batchIndex":40,"message.batchSize":1,"message.lastBatch":False}
    msg2 = api.Message(attributes=file2, body=file2)
    file3 = {"file":{"connection":{"configurationType":"Connection Management","connectionID":"ADL_THH"},\
                     "isDir":False,"modTime":"2020-07-21T10:05:56Z","path":"/replication/REPLICATION/TEST_TABLE_18/TEST_TABLE_18.csv","size":4891},\
             "message.batchIndex":41,"message.batchSize":1,"message.lastBatch":True}
    msg3 = api.Message(attributes=file3, body=file3)

    files = [msg1,msg2,msg3]


    on_files_process(api.Message(attributes=file1, body=files))
    for i in range(1,len(files)) :
        process(api.Message(attributes=file1,body=''))


    for m in api.queue :
        print(m.attributes)
        print(m.body)

