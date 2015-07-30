#!/usr/local/bin/python2.7

# Azkaban API Documantation
#   http://azkaban.github.io/azkaban/docs/2.5/#ajax-api
# Additional information about Azkaban API
#   https://github.com/erwa/notes/blob/master/azkabanAndReportal.md

import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from Azusa import AzkabanWeb, AzkabanJob
from datetime import datetime

LOG_LEVEL = "INFO"


def create_flowA():
    flow = AzkabanJob.Flow('flowA',)
    comm1 = flow.register_command(
        AzkabanJob.Command('commA_1', {'command': 'echo "Start"',
                                       'retries': 10,
                                       'retry.backoff': 100,
                                       })
    )
    comm2 = flow.register_command(
        AzkabanJob.Command('commA_2', {'command': 'echo "Execute1-1"',
                                       'retries': 10,
                                       'retry.backoff': 100,
                                       })
    )
    comm3 = flow.register_command(
        AzkabanJob.Command('commA_3', {'command': 'echo "Execute1-2"',
                                       'retries': 10,
                                       'retry.backoff': 100,
                                       })
    )
    comm4 = flow.register_command(
        AzkabanJob.Command('commA_4', {'command': 'echo "Execute2"',
                                       'retries': 10,
                                       'retry.backoff': 100,
                                       })
    )
    comm5 = flow.register_command(
        AzkabanJob.Command('commA_5', {'command': 'echo "finish"',
                                       'retries': 10,
                                       'retry.backoff': 100,
                                       })
    )
    flow.set_dependencies(comm1, comm2)
    flow.set_dependencies(comm1, comm3)
    flow.set_dependencies(comm2, comm4)
    flow.set_dependencies(comm3, comm4)
    flow.set_dependencies(comm4, comm5)
    return flow


def create_flowB():
    def create_flowBsub():
        flow = AzkabanJob.Flow('flowBsub',
                               properties={
                                   'FLOW_NAME': 'flowBsub',
                               })
        comm1 = flow.register_command(
            AzkabanJob.Command('commBsub_1', {'command': 'echo "Execute1"',
                                              })
        )
        comm2 = flow.register_command(
            AzkabanJob.Command('commBsub_2', {'command': 'echo "Execute2"',
                                              })
        )
        comm3 = flow.register_command(
            AzkabanJob.Command('commBsub_3', {'command': 'echo "Execute3"',
                                              })
        )
        flow.set_dependencies(comm1, comm2)
        flow.set_dependencies(comm1, comm3)
        flow.set_dependencies(comm2, comm3)
        return flow
    subflow = create_flowBsub()
    flow = AzkabanJob.Flow('flowB',
                           properties={
                               'FLOW_NAME': 'flowB',
                           })
    comm1 = flow.register_command(
        AzkabanJob.Command('commB_1', {'command': 'echo "Start"',
                                       })
    )
    comm2 = flow.register_command(
        AzkabanJob.Command('commB_2', {'command': 'echo "Execute1"',
                                       })
    )
    comm3 = flow.register_command(
        AzkabanJob.Command('commB_3', {'command': 'echo "End"',
                                       })
    )
    flow.set_dependencies(comm1, comm2)
    flow.set_dependencies(comm1, subflow)
    flow.set_dependencies(comm2, comm3)
    flow.set_dependencies(subflow, comm3)
    return flow


def print_check_proj(proj):
    import networkx as nx
    for flow in proj:
        print "=====", flow
        f = flow.first_jobs()[0]
        l = flow.last_nodes()[0]
        for path in nx.all_simple_paths(flow, f, l):
            print path
        for job in flow:
            print "   >", job.filename
            print '\n'.join(map(lambda x: "\t{0}".format(x), job.body.split('\n')))


def main():

    args = AzkabanWeb.parse_arguments()

    proj = AzkabanJob.Project(
        'test_new_upload_script',
        "You can remove this project anytime.",
        properties={
            'PROJECT_NAME': 'test_new_upload_script'
        }
    )

    flowA = create_flowA()
    flowB = create_flowB()

    proj.add_flow(flowA)
    proj.add_flow(flowB)

    # print_check_proj(proj)

    zipfilepath = proj.create_zipfile('/tmp', overwrite=True)

    az = AzkabanWeb.AjaxAPI(args.host, args.username, args.password, log_level=LOG_LEVEL)
    az.create_project(proj.name, proj.description, if_not_exists=True)
    res_upload = az.upload_project(proj.name, zipfilepath)
    project_id = res_upload['projectId']
    az.schedule_flow(proj.name, flowA.name, datetime(2015, 5, 30, 10, 0, 0), recurring_period='1d')
    az.schedule_flow(proj.name, flowB.name, datetime(2015, 5, 30, 15, 0, 0), recurring_period='1w')

if __name__ == "__main__":
    main()
