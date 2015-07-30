#!/usr/bin/env python
# -*- coding: utf-8 -*-

import abc
import collections


class Params(collections.Mapping):
    """ parameter of jobs and properties, such as key=value.
    """

    def __init__(self, data):
        self.__data = data

    def __len__(self):
        return len(self.__data)

    def __iter__(self):
        return iter(self.__data)

    def __contains__(self, value):
        return value in self

    def __getitem__(self, key):
        return self.__data[key]

    def _set_dependencies(self, dependent_job):
        """

        :param dependent_job: Executed command or sub-flow before this command is executed.
        :type dependent_job: Command or Flow
        """
        if 'dependencies' not in self.__data:
            self.__data['dependencies'] = []
        self.__data['dependencies'].append(dependent_job.basename)

    def _remove_dependencies(self, dependent_job):
        """

        :param dependent_job: Removed registered command or sub-flow as dependencies before this command is executed.
        :type dependent_job: Command or Flow
        """
        self.__data['dependencies'].remove(dependent_job.basename)


class AzkabanFileAbstruct(object):
    """ Abstruct class for Azkaban files.

    This abstruct class has Params class internally.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, name, params, file_ext='undefined'):
        """
        :param name: Object unique name. It is used as filename.
        :type name: str
        :param params: key-value parameters.
        :type params: dict
        """
        self.__name = name
        self.__params = Params(params)
        self.__file_ext = file_ext

    @property
    def name(self):
        """
        :rtype: str
        """
        return self.__name

    @property
    def basename(self):
        """ The filename without extention.

        :rtype: str
        """
        return self.name

    @property
    def filename(self):
        """ The filename with extention, such as <name>.properties.

        :rtype: str
        """
        return "{0}.{1}".format(self.basename, self.__file_ext)

    @property
    def params(self):
        """ Object parameter, this is immutable mappnig collection.

        :rtype: Params
        """
        return self.__params

    @property
    def text(self):
        """ The file's text content.

        :rtype: str
        """
        return self.__convert_dict_to_text(self.params)

    @staticmethod
    def __convert_dict_to_text(params):
        """ Convert python dictionary type to key=value format text, such as .job, .properties in Azkaban.

        :param params: key=value parameters. (This is used by .job, .properties in Azkaban)
        :type params: dict or collections.Mapping
        :return: key=value style text.
        """

        lines = []
        for key, value in params.items():
            if isinstance(value, list):
                value = ','.join([str(x) for x in value])
            elif isinstance(value, dict):
                value = ','.join([k for k in value.keys()])
            elif isinstance(value, int) or isinstance(value, str):
                pass
            else:
                raise TypeError("Not support {0} type yet".format(type(value)))
            lines.append("{0}={1}".format(key, value))
        return '\n'.join(lines)


class AzkabanJobAbstruct(AzkabanFileAbstruct):
    """ Abstruct class for Azkaban Job files.

    This abstruct class inherit AzkabanFileAbstruct.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, type_str, name, params):
        """
        :param name: Properties' unique name. It is used as properties filename.
        :type name: str
        :param params: Properties key-value parameters.
        :type params: dict
        """
        params.update({'type': type_str})
        super(AzkabanJobAbstruct, self).__init__(name, params, 'job')
