import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess
import logging
import os
import random
from datetime import datetime, timezone
import pandas as pd

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)

        class config:
            ## Meta data
            config_params = dict()
            version = '0.0.1'
            tags = {'sdi_utils': ''}
            operator_name = 'repl_block'
            operator_description = "Repl. Block"

            operator_description_long = "Update replication table status to done."
            add_readme = dict()
            add_readme["References"] = ""

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            use_package_id = True
            config_params['use_package_id'] = {'title': 'Using Package ID',
                                           'description': 'Using Package ID rather than generated packages by package size',
                                           'type': 'boolean'}

            package_size = 1000
            config_params['package_size'] = {'title': 'Package size',
                                           'description': 'Size of the packages of each replication',
                                           'type': 'integer'}


def process(msg):

    att = dict(msg.attributes)
    att['operator'] = 'repl_block'
    #att['pid'] = msg.attributes['pid']
    logger, log_stream = slog.set_logging(att['operator'], loglevel=api.config.debug_mode)

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()
    logger.debug('Attributes: {}'.format(str(dict)))

    if not api.config.use_package_id and api.config.package_size <1 :
        err_str = 'If not using DIREPL_PACKAGEID a package size >0 must be given in configuration'
        logger.error(err_str)
        raise ValueError(err_str)

    logger.info('Replication table from attributes: {}'.format(att['table']))

    att['pid'] = datetime.utcnow()

    change_type = 'I' if att['append_mode'] == True else 'U'

    if api.config.use_package_id :
        update_sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\', '\
                     '\"DIREPL_UPDATED\" =  CURRENT_UTCTIMESTAMP WHERE ' \
                     '\"DIREPL_PACKAGEID\" = (SELECT min(\"DIREPL_PACKAGEID\") ' \
                     'FROM {table} WHERE \"DIREPL_STATUS\" = \'W\' AND \"DIREPL_TYPE\" = \'{ct}\')'.format(table=att['table'], pid = att['pid'],ct = change_type)
    else :
        update_sql = 'UPDATE {table} SET \"DIREPL_STATUS\" = \'B\', \"DIREPL_PID\" = \'{pid}\', '\
                     '\"DIREPL_UPDATED\" =  CURRENT_UTCTIMESTAMP WHERE ' \
                     '\"DIREPL_PACKAGEID\" = (SELECT min(\"DIREPL_PACKAGEID\") ' \
                     'FROM {table} WHERE \"DIREPL_STATUS\" = \'W\' AND \"DIREPL_TYPE\" = \'{ct}\')'.format(table=att['table'], pid = att['pid'],ct = change_type)

    logger.info('Update statement: {}'.format(update_sql))
    att['update_sql'] = update_sql

    logger.debug('Process ended: {}'.format(time_monitor.elapsed_time()))

    api.send(outports[1]['name'], update_sql)
    api.send(outports[2]['name'], api.Message(attributes=att,body=update_sql))

    log = log_stream.getvalue()
    if len(log) > 0 :
        api.send(outports[0]['name'], log )


inports = [{'name': 'data', 'type': 'message', "description": "Input data"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sql', 'type': 'string', "description": "sql statement"},
            {'name': 'msg', 'type': 'message', "description": "msg with sql statement"}]

#api.set_port_callback(inports[0]['name'], process)

def test_operator():
    api.config.use_package_id = False
    api.config.package_size = 1

    msg = api.Message(attributes={'packageid':4711,'table':'repl_table','base_table':'repl_table','latency':30,\
                                  'append_mode' : 'I', 'data_outcome':True},body='')
    process(msg)

    for st in api.queue :
        print(st)


if __name__ == '__main__':
    test_operator()
    if True:
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/sdi_utils/solution/operators/sdi_utils_operators_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])

