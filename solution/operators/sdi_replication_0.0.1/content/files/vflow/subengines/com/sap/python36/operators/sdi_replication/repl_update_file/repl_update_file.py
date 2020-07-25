import io
import subprocess
import os
import pandas as pd
import io

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes

        def send(port,msg) :
            if port == outports[1]['name'] :
                print('ATTRIBUTES: ')
                print(msg.attributes)#
                print('CSV-String: ')
                print(msg.body)

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_name = 'repl_check_file'
            operator_description = "check_file"
            operator_description_long = "Checks file on consistency with original table."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


df = pd.DataFrame()
att = dict()
update_list = list()

def on_base(msg) :
    global df
    global att
    ## read csv
    csv_io = io.BytesIO(msg.body)
    df = pd.read_csv(csv_io)
    indices = msg.attributes['current_primary_keys']
    df = df.set_index(indices)
    process()

def on_update(msg):
    global update_list
    global att
    ## read csv
    csv_io = io.BytesIO(msg.body)
    att = dict(msg.attributes)
    df = pd.read_csv(csv_io)
    indices = att['current_primary_keys']
    df = df.set_index(indices)
    update_list.append(df)
    process()

def process():
    global df
    global update_list
    global att

    att['operator'] = 'repl_update_file'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    if df.empty :
        logger.warning('Base dataframe not loaded yet')
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()
        return 0

    if len(update_list) == 0 :
        logger.warning('Update dataframe not loaded yet')
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()
        return 0

    for ul in update_list :
        df.update(ul)

    if att['message.last_update'] == True :
        df = df.reset_index()
        csv = df.to_csv(index=False)
        att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['base_file'])
        api.send(outports[1]['name'],api.Message(attributes=att,body = csv))
    else :
        api.send(outports[2]['name'],api.Message(attributes=att, body=None))

    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'update', 'type': 'message.file', "description": "Update Data"}, \
           {'name': 'base', 'type': 'message.file', "description": "Base Data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'csv', 'type': 'message.file', "description": "Output data as csv"},
            {'name': 'trigger', 'type': 'message', "description": "Trigger"}]


api.set_port_callback(inports[0]['name'], on_update)
api.set_port_callback(inports[1]['name'], on_base)

def test_operator() :
    att = {'operator': 'collect_files', 'file': {'path': '/adbd/abd.csv'},'current_primary_keys':['INDEX'],\
           'message.last_update':False}
    att['current_file'] = {
            "dir": "/replication/REPLICATION/TEST_TABLE_17",
            "update_files": [
                "12345_TEST_TABLE_17.csv"
            ],
            "base_file": "TEST_TABLE_17.csv",
            "schema_name": "REPLICATION",
            "table_name": "TEST_TABLE_17",
            "primary_key_file": "TEST_TABLE_17_primary_keys.csv",
            "consistency_file": "",
            "misc": []
        }

    csv_base = b"INDEX,NUM_INT,PID\n0,1,1234\n1,2,1234\n2,3,1234\n3,4,1234\n4,5,1234"
    msg_base = api.Message(attributes=att,body=csv_base)
    on_base(msg_base)
    csv_update = b"INDEX,NUM_INT,PID\n0,5,1111"
    on_update(api.Message(attributes=att,body=csv_update))
    csv_update = b"INDEX,NUM_INT,PID\n2,5,2222\n3,6,2222"
    on_update(api.Message(attributes=att,body=csv_update))
    csv_update = b"INDEX,NUM_INT,PID\n4,7,2222\n5,8,3333"
    att['message.last_update'] = True
    on_update(api.Message(attributes=att,body=csv_update))


