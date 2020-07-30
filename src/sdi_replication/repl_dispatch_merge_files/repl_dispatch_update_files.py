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

            operator_description = "Dispatch Update Files"
            operator_name = 'repl_dispatch_update_files'
            operator_description_long = "Dispatch update files."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}




file_index = 0

def on_files(msg) :

    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_update_files_on_files'
    att['message.last_update_file'] = False

    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)
    att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['base_file'])

    api.send(outports[1]['name'], api.Message(attributes=att, body=msg.body))
    api.send(outports[0]['name'], log_stream.getvalue())


def on_trigger(msg) :

    global file_index

    att = dict(msg.attributes)
    att['operator'] = 'repl_dispatch_update_files'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    files_list = msg.attributes['current_file']['update_files']

    att['message.index_update_file'] = file_index
    att['message.last_update_file'] = False
    if file_index == len(files_list) - 1 :
        att['message.last_update_file'] = True
    att['file']['path'] = os.path.join(msg.attributes['current_file']['dir'],files_list[file_index])

    logger.info('Send File: {} ({}/{})'.format(files_list[file_index],file_index, len(files_list)))
    api.send(outports[1]['name'], api.Message(attributes=att,body=files_list[file_index]))
    api.send(outports[0]['name'], log_stream.getvalue())
    file_index += 1


inports = [{'name': 'files', 'type': 'message.file',"description":"List of files"},
           {'name': 'trigger', 'type': 'message.*',"description":"Trigger"}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'file', 'type': 'message.file',"description":"file"}]


#api.set_port_callback(inports[0]['name'], on_files)
#api.set_port_callback(inports[1]['name'], on_trigger)


def test_operator() :

    att = {'operator':'collect_files','file':{'path':'/adfg/asdf.cfg'}}
    att['current_file'] = {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": ["22222_TEST_TABLE_17.csv", "11111_TEST_TABLE_17.csv", "33333_TEST_TABLE_17.csv"],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "key": "TEST_TABLE_17_primary_keys.csv",
            "consistency": "",
            "misc": []
        }
    att['current_file']['update_files'] = sorted(att['current_file']['update_files'])

    csv = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
0,1595842726816,U,2020-07-27T09:38:46.038Z,0,1
0,1595842726816,U,2020-07-27T09:38:46.038Z,3,4
1,1595842726816,U,2020-07-27T09:38:46.038Z,6,7
1,1595842726817,U,2020-07-27T09:38:46.038Z,9,10
2,1595842726817,U,2020-07-27T09:38:46.038Z,12,13
3,1595842726817,U,2020-07-27T09:38:46.038Z,15,16
3,1595842726818,U,2020-07-27T09:38:46.038Z,18,19
4,1595842726818,U,2020-07-27T09:38:46.038Z,21,22
4,1595842726818,U,2020-07-27T09:38:46.038Z,24,25'''

    on_files(api.Message(attributes=att, body=csv))
    for i in range(0,len(att['current_file']['update_files'])) :
        on_trigger(api.Message(attributes=att,body=''))

    for m in api.queue :
        print(m.attributes)
        print(m.body)

if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])



