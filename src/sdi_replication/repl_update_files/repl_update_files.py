import io
import subprocess
import os
import pandas as pd
import io

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

pd.set_option('expand_frame_repr', True)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 200)
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
            version = "0.0.1"
            operator_name = 'repl_update_files'
            operator_description = "Update Files"
            operator_description_long = "Merges all the update files with the base file."
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

df = pd.DataFrame()


def process(msg):

    global df

    att = dict(msg.attributes)
    att['operator'] = 'repl_update_files'
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    csv_io = io.BytesIO(msg.body)
    df = df.append(pd.read_csv(csv_io), ignore_index=True)

    if att['message.last_update_file']:

        #  cases:
        #  I,U: normal, U,I : should not happen, sth went wrong with original table (no test)
        #  I,D: normal, D,I : either sth went wrong in the repl. table or new record (no test)
        #  U,D: normal, D,U : should not happen, sth went wrong with original table (no test)

        # keep only the most updated records irrespective of change type I,U,D
        gdf = df.groupby(by = att['current_primary_keys'])['DIREPL_UPDATED'].max().reset_index()
        keys = att['current_primary_keys'] + ['DIREPL_UPDATED']
        df = pd.merge(gdf, df, on=keys, how='inner')

        # remove D-type records
        df = df.loc[~(df['DIREPL_TYPE']=='D')]

        # prepare for saving
        df = df[sorted(df.columns)]
        csv = df.to_csv(index=False)
        att['file']['path'] = os.path.join(att['current_file']['dir'], att['current_file']['base_file'])
        api.send(outports[1]['name'],api.Message(attributes=att, body=csv))

    else:
        api.send(outports[1]['name'], api.Message(attributes=att, body=''))

    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'data', 'type': 'message.file', "description": "Input Data as csv"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'csv', 'type': 'message.file', "description": "Output data as csv"},
            {'name': 'trigger', 'type': 'message.file', "description": "Trigger for next file"}]


# api.set_port_callback(inports[0]['name'], process)


def test_operator() :
    att = {'operator': 'collect_files', 'file': {'path': '/adbd/abd.csv'},'current_primary_keys':['INDEX'],\
           'message.last_update_file':False}
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
    csv1 = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
0,1,U,2020-07-27T09:38:46.038Z,0,0
0,1,U,2020-07-27T09:38:46.038Z,1,1
1,1,U,2020-07-27T09:38:46.038Z,2,2
1,1,U,2020-07-27T09:38:47.038Z,3,3
4,1,U,2020-07-27T09:38:48.038Z,4,4
0,1,I,2020-07-27T09:38:49.657Z,5,5'''
    csv1 = str.encode(csv1)
    csv2 = r'''DIREPL_PACKAGEID,DIREPL_PID,DIREPL_TYPE,DIREPL_UPDATED,INDEX,INT_NUM
0,2,U,2020-07-27T09:39:06.657Z,6,6
3,2,I,2020-07-27T09:39:09.657Z,7,7
4,2,D,2020-07-27T09:39:06.657Z,0,0
4,2,U,2020-07-27T09:39:06.657Z,1,2
4,2,D,2020-07-27T09:36:06.657Z,3,3'''
    csv2 = str.encode(csv2)

    msg_base = api.Message(attributes=att,body=csv1)
    process(msg_base)
    att['message.last_update_file'] =  True
    msg_base = api.Message(attributes=att, body=csv2)
    process(msg_base)

    for q in api.queue :
        print(q.body)



if __name__ == '__main__':
    test_operator()
    if True :
        subprocess.run(["rm", '-r','../../../solution/operators/sdi_replication_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",'../../../solution/operators/sdi_replication_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])
