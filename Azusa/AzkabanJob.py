#!/usr/local/bin/python2.7
"""
    Azkaban Job-Flow Creator.

    To get more details, read the Azkaban Flow document.
    http://azkaban.github.io/azkaban/docs/2.5/#creating-flows

    :copyright: 2015, Tasuku OKUDA.
"""

import os
import zipfile
import collections
import networkx as nx
from AzkabanJobBase import AzkabanFileAbstruct, AzkabanJobAbstruct


class Properties(AzkabanFileAbstruct):
    """ Azkaban properties class (such as system.properties)
    """

    def __init__(self, name, params):
        """
        :param name: Properties' unique name. It is used as properties filename.
        :type name: str
        :param params: Properties key-value parameters.
        :type params: dict
        """
        AzkabanFileAbstruct.__init__(self, name, params, file_ext='properties')


class Command(AzkabanJobAbstruct):
    """ Azkaban Job class with command type.
    """

    def __init__(self, name, params):
        """
        :param name:
        :type name: str
        :param params:
        :type params: dict
        """
        params.update({'type': 'command'})
        AzkabanJobAbstruct.__init__(self, 'command', name, params)


class Flow(AzkabanJobAbstruct):
    """ Azkaban Flow class (also sub-Flow job)
    """

    def __init__(self, name, params=None, properties=None):
        """
        :param name: The name of flow.
        :param params: The parameters when this flow is used by sub-flow.
        :param properties: The properties affecting registered jobs under this flow.
        :type name: str
        :type params: dict
        :type properties: Properties or dict
        """
        params = params or {}
        params.update({'type': 'flow', 'flow.name': name})
        AzkabanJobAbstruct.__init__(self, 'flow', name, params)
        if properties is None:
            self.__properties = None
        elif isinstance(properties, Properties):
            self.__properties = properties
        elif isinstance(properties, dict):
            self.__properties = Properties(self.name, properties)
        else:
            raise TypeError("properties is dict or Properties. (actual: {0})".format(type(properties)))
        self.__graph = nx.DiGraph()
        self.__finish_command = Command(self.name, {'command': 'echo "Finish {0} at $(date)"'.format(self.name)})
        self.__graph.add_node(self.__finish_command)

    @property
    def basename(self):
        """ Jobs unique name. It is used as job filename.

        :rtype: str
        """
        return "flow_{0}".format(self.name)

    @property
    def properties(self):
        """ Properties under flow.

        :rtype: Properties
        """
        return self.__properties

    @property
    def finish_command(self):
        """ The last command of this Job.

        :rtype: Command
        """
        return self.__finish_command

    @property
    def jobs(self):
        """ Jobs list.

        :return: nx.DiGraph
        """
        return self.__graph

    @property
    def first_jobs(self):
        """ The list of first job of this flow.

        :rtype: list
        """
        return [node for node in self.__graph.nodes() if len(self.__graph.predecessors(node)) == 0]

    @property
    def last_nodes(self):
        """ The list of last job of this flow.

        :rtype: list
        """
        return [node for node in self.__graph.nodes() if len(self.__graph.successors(node)) == 0]

    @property
    def jobs_before_last(self):
        """ The list of jobs before last of this flow.

        :rtype: list
        """
        return self.__graph.predecessors(self.finish_command)

    def register_command(self, command):
        """ Register command to this flow.

        :param command: new appended command
        :type command: Command
        """
        if command in self.__graph.nodes():
            raise self.DuplicatedJobError("{0} is already exists.".format(command))
        if not isinstance(command, Command):
            raise TypeError("{0} is not instance of Command".format(command))
        self.__graph.add_node(command)
        self.__append_finish_command(command)
        return command

    def register_subflow(self, subflow):
        """ Register subflow to this flow.

        :param subflow: new appended flow.
        :type subflow: Flow
        """
        if subflow in self.__graph.nodes():
            raise self.DuplicatedJobError("{0} is already exists.".format(subflow))
        if not isinstance(subflow, Flow):
            raise TypeError("{0} is not instance of Flow".format(subflow))
        self.__graph.add_node(subflow)
        self.__append_finish_command(subflow)
        return subflow

    def set_dependencies(self, previous_job, next_job):
        """ Create new dependencies edge.

        :param previous_job: Before job.
        :param next_job: After job.
        :type previous_job: Command or Flow
        :type next_job: Command or Flow
        """
        if (previous_job, next_job) in self.__graph.edges():
            raise self.DuplicatedDependenciesError("This dependencies {0} to {1} is already exists.".format(previous_job, next_job))
        self.__set_dependencies(previous_job, next_job)
        self.__arrange_finish_command()

    def remove_dependencies(self, previous_job, next_job):
        """ Remove existing dependincies edge.

        :param previous_job: Before job.
        :param next_job: After job.
        :type previous_job: Command or Flow
        :type next_job: Command or Flow
        """
        if next_job == self.finish_command:
            raise self.FinishCommandError("Do not remove finish command dependencies manually.")
        self.__remove_dependencies(previous_job, next_job)

    def __set_dependencies(self, previous_job, next_job):
        """ Set job to 'dependencies' parameter.

        :param previous_job: Before job.
        :param next_job: After job.
        :type previous_job: Command or Flow
        :type next_job: Command or Flow
        """
        next_job.params._set_dependencies(previous_job)
        self.__graph.add_edge(previous_job, next_job)

    def __remove_dependencies(self, previous_job, next_job):
        """ Remove job from 'dependencies' parameter.

        :param previous_job: Before job.
        :param next_job: After job.
        :type previous_job: Command or Flow
        :type next_job: Command or Flow
        """
        next_job.params._remove_dependencies(previous_job)
        self.__graph.remove_edge(previous_job, next_job)

    def __append_finish_command(self, job):
        """ Append last job command.

        :param job: previous job before last.
        :type job: Command or Flow
        """
        self.set_dependencies(job, self.finish_command)

    def __arrange_finish_command(self):
        """ reallocation last job
        """
        for job in self.jobs_before_last:
            self.__remove_dependencies(job, self.finish_command)
        for job in self.last_nodes:
            if not job == self.finish_command:
                self.__set_dependencies(job, self.finish_command)

    class DuplicatedJobError(Exception):
        """ All jobs require to be unique.
        """
        pass

    class DuplicatedDependenciesError(Exception):
        """ All dependencies require to be unique.
        """
        pass

    class FinishCommandError(Exception):
        """ Last command is managed automatically. We should not touch this.
        """
        pass


class Project(collections.Set):
    """ Azkaban Project class.
    """

    def __init__(self, name, description, properties=None):
        """
        :param name: Project name.
        :param description: Project description.
        :param properties: Porperties affecting to all jobs in this project.
        :raise TypeError: Unknown properties type.
        :type name: str
        :type description: str
        :type properties: Properties or dict
        """
        self.__name = name
        self.__description = description
        self.__flows = set()
        if properties is None:
            self.__properties = None
        elif isinstance(properties, Properties):
            self.__properties = properties
        elif isinstance(properties, dict):
            self.__properties = Properties(self.name, properties)
        else:
            raise TypeError("properties is dict or Properties. (actual: {0})".format(type(properties)))

    @property
    def name(self):
        """
        :rtype: str
        """
        return self.__name

    @property
    def filename(self):
        """ The zip filename.

        :rtype: str
        """
        return "{0}.zip".format(self.name)

    @property
    def description(self):
        """ Project description.

        :return: str
        """
        return self.__description

    @property
    def flows(self):
        """ The set of flows in this project.

        :return: set
        """
        return self.__flows

    @property
    def properties(self):
        """ Properties under project.

        :rtype: Properties
        """
        return self.__properties

    def __len__(self):
        return len(self.flows)

    def __iter__(self):
        return iter(self.flows)

    def __contains__(self, value):
        return value in self.flows

    def add_flow(self, flow):
        """ Add new flow.

        :param flow: new appended flow.
        :type flow: Flow
        """
        if not isinstance(flow, Flow):
            raise TypeError("{0} is not Flow.".format(flow))
        self.__flows.add(flow)

    def create_zipfile(self, out_dir='./', overwrite=False):
        """ Create new zipfile.

        :param out_dir: output dir. (default: current directory)
        :param overwrite: Overwrite flag if already exists. (default: False)
        :type out_dir: str
        :type overwrite: bool
        :return: output full path
        """
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        filepath = os.path.join(out_dir, self.filename)
        if os.path.exists(filepath) and not overwrite:
            raise IOError("Already exists. {0}".format(filepath))
        with zipfile.ZipFile(filepath, mode='w') as project_zip:
            for flow in self.flows:
                self.__add_flow_to_zip(flow, project_zip, basedir=os.path.join(self.name, flow.name))
            if self.properties is not None:
                self.__add_properties_to_zip(self.properties, project_zip, basedir=self.name)
        return filepath

    def __add_flow_to_zip(self, flow, zipfile_obj, basedir='./'):
        """ Write flow's job files into zipfile.

        :param flow: target flow
        :param zipfile_obj: target zipfile object.
        :param basedir: Basedir in zipfile. (default: root)
        :type flow: Flow
        :type zipfile_obj: zipfile.ZipFile
        :type basedir: str
        """
        if len(flow.last_nodes) != 1:
            raise self.MultipleLastJobError("{0} will be separated because it has multiple end node.".format(flow))
        for job in flow.jobs:
            if isinstance(job, Command):
                zipfile_obj.writestr(os.path.join(basedir, job.filename), job.text)
            elif isinstance(job, Flow):
                zipfile_obj.writestr(os.path.join(basedir, job.filename), job.text)
                self.__add_flow_to_zip(job, zipfile_obj, basedir=os.path.join(basedir, job.name))
            else:
                raise TypeError("{0} is not Command and Flow.".format(job))
        if flow.properties is not None:
            self.__add_properties_to_zip(flow.properties, zipfile_obj, basedir=basedir)

    @staticmethod
    def __add_properties_to_zip(properties, zipfile_obj, basedir='./'):
        """ Write properties files into zipfile.

        :param properties: target properties
        :param zipfile_obj: target zipfile object
        :param basedir: Basedir in zipfile. (default: root)
        :type properties: Properties
        :type zipfile_obj: zipfile.ZipFile
        :type basedir: str
        """
        filepath = os.path.join(basedir, properties.filename)
        zipfile_obj.writestr(filepath, properties.text)

    class MultipleLastJobError(Exception):
        """ Unexcepted multiple last command in jobfile.
        """
        pass
