#!/usr/local/bin/python2.7
"""
    This module is API wrapper of Azkaban Web Server.
    http://azkaban.github.io/azkaban/docs/2.5/

    Additional information about Azkabana API
    https://github.com/erwa/notes/blob/master/azkabanAndReportal.md

    :copyright: 2015, Tasuku OKUDA.
"""


from bs4 import BeautifulSoup
import logging
import re
import requests
import sys
from urlparse import urljoin
import os
from getpass import getpass
import argparse


def parse_arguments():
    """ Azkaban related argument parser

    :return: Arguments: username, password, host
    """
    parser = argparse.ArgumentParser(
        description="Azsa - Azkaban Project Uploader for LiSAP"
    )
    parser.add_argument('-u', '--username',
                        default=raw_input('Azkaban login username: '),
                        help="Azkaban login username")
    parser.add_argument('-p', '--password',
                        default=getpass('Azkaban login password: '),
                        help="Azkaban login password")
    parser.add_argument('-h', '--host',
                        default="http://localhost:22300",
                        help="Azkaban web server host")
    args = parser.parse_args()
    return args



class AjaxAPI(object):
    """ Azkaban Ajax API Wrapper.
    http://azkaban.github.io/azkaban/docs/2.5/#ajax-api
    """

    def __init__(self, base_url, username, password, log_level="INFO"):
        """
        :param base_url: Azkaban API base URL, such as https://hostname:port/
        :type base_url: str
        :param username: Azkaban login username
        :type username: str
        :param password: Azkaban login password
        :type password: str
        :param log_level: Log level to output stdout
        :type log_level: str

        """
        self.logger = self.__get_stdout_logger(__name__, log_level)
        self.logger.info("URL: {0}".format(base_url))
        self.__base_url = base_url
        self.__session_id = self.authenticate(username, password)['session.id']

    @property
    def base_url(self):
        """ Base URL.

        :rtype: str
        """
        return self.__base_url

    def authenticate(self, username, password):
        """ Login to Azkaban Web Server.

        Response Json contents.

        :param username: Azkaban login username
        :type username: str
        :param password: Azkaban login password
        :type password: str
        :return: Response Json.

            :status: The status of attempt.
            :session.id: a session id (will expired after server param 'session.time.to.live', default 24h)

        :rtype: dict
        :raises AzkabanLoginError: Login failed. (when error key exists in response.)
        """
        api_url = self.base_url
        payload = {
            'action': 'login',
            'username': username,
            'password': password
        }
        self.logger.debug(api_url)

        res = requests.post(api_url, data=payload)
        res.raise_for_status()
        if 'error' in res.json():
            self.logger.error("Login Error")
            self.logger.error(res.json()['error'])
            raise self.AzkabanLoginError(res.json()['error'])
        else:
            self.logger.info("Success: Login %s with user %s", self.base_url, username)
            self.logger.debug(res.json())
            return res.json()

    def create_project(self, project_name, description, if_not_exists=False):
        """ Create New Azkaban Project.

        :param project_name: Project Name.
        :type project_name: str
        :param description: Project Description.
        :type description: str
        :param if_not_exists: When same name project exists, skip creating project if True, raise AjaxAPIError if False.
        :return: Response Json.

            :status: The status of attempt.
            :path: The url path to redirect.
            :action: The action that is suggested for the frontend to execute. (for JavaScript)

        :rtype: dict
        :raises AjaxAPIError: Request is accepted successfully, but some error is occured in Azkaban Web Server.
        """
        api_url = urljoin(self.base_url, 'manager')
        payload = {
            'session.id': self.__session_id,
            'action': 'create',
            'name': project_name,
            'description': description
        }
        self.logger.debug("%s data:%s", api_url, payload)

        res = requests.post(api_url, data=payload)
        res.raise_for_status()
        if res.json()['status'] == 'error':
            if if_not_exists and re.match(r"Active project with name .+ already exists in db\.", res.json()['message']):
                self.logger.warning("Skip creating project %s because it already exists.", project_name)
                return res.json()
            self.logger.error("Cannot create project")
            self.logger.error(res.json())
            raise self.AjaxAPIError(res.json()['message'])
        else:
            self.logger.info("Success: Create project - %s", project_name)
            self.logger.debug(res.json())
            return res.json()

    def upload_project(self, project_name, zip_file_path):
        """ Upload a project zip file to existing Azkaban project.

        The zip file should include .job files and .properties files.

        :param project_name: Target project name. (should exist)
        :type project_name: str
        :param zip_file_path: Project zip file path.
        :type zip_file_path: str
        :return: Response json.

            :status: The status of attempt.
            :projectId: The numerical id of the project.
            :version: The version number of the upload.

        :rtype: dict
        :raises AjaxAPIError: Request is accepted successfully, but some error is occured in Azkaban Web Server.
        """
        api_url = urljoin(self.base_url, 'manager')
        self.logger.debug(api_url)
        payload = {
            'session.id': self.__session_id,
            'ajax': 'upload',
            'project': project_name
        }
        files = {'file': ('jobs.zip', open(zip_file_path, 'rb'), 'application/x-zip-compressed')}
        self.logger.debug("%s data:%s", api_url, payload)

        res = requests.post(api_url, data=payload, files=files)
        res.raise_for_status()
        if 'error' in res.json():
            self.logger.error("Cannot upload project")
            self.logger.error(res.json())
            raise self.AjaxAPIError(res.json()['error'])
        else:
            self.logger.info("Success: Upload project - %s", project_name)
            self.logger.debug(res.json())
            return res.json()

    def fetch_project_flows(self, project_name):
        """

        :param project_name: The project name to be fetched.
        :type project_name: str
        :return: Response json.

            :project: The project name.
            :projectId: The numerical id of the project.
            :flows: A list of flow ids.

        :rtype: dict
        :raises AjaxAPIError: Request is accepted successfully, but some error is occured in Azkaban Web Server.
        """
        api_url = urljoin(self.base_url, 'manager')
        payload = {
            'session.id': self.__session_id,
            'ajax': 'fetchprojectflows',
            'project': project_name
        }
        self.logger.debug("%s data:%s", api_url, payload)

        res = requests.get(api_url, params=payload)
        res.raise_for_status()
        if 'error' in res.json():
            self.logger.error("Cannot fetch project flows")
            self.logger.error(res.json()['error'])
            raise self.AjaxAPIError(res.json()['error'])
        else:
            self.logger.info("Sucecss: Fetch project flow - %s", project_name)
            self.logger.debug(res.json())
            return res.json()

    def fetch_flow_jobs(self, project_name, flow_name):
        """

        :param project_name: The project name to be fetched.
        :type project_name: str
        :param flow_name: The flow name to be fetched.
        :type flow_name: str
        :return: Response json.

            :project: The project name.
            :projectId: The numerical id of the project.
            :flow: The flow id fetched.
            :nodes: A list of job nodes belonging to this flow.

                :id: The job id.
                :type: The type of job.
                :in: The list of dependencies.

        :rtype: dict
        :raises AjaxAPIError: Request is accepted successfully, but some error is occured in Azkaban Web Server.
        """

        api_url = urljoin(self.base_url, 'manager')
        payload = {
            'session.id': self.__session_id,
            'ajax': 'fetchflowgraph',
            'project': project_name,
            'flow': flow_name
        }
        self.logger.debug("%s data:%s", api_url, payload)

        res = requests.get(api_url, params=payload)
        res.raise_for_status()
        if 'error' in res.json():
            self.logger.error("Cannot fetch flow jobs")
            self.logger.error(res.json()['error'])
            raise self.AjaxAPIError(res.json()['error'])
        else:
            self.logger.info("Sucecss: Fetch flow jobs - %s", project_name)
            self.logger.debug(res.json())
            return res.json()

    def schedule_flow(self, project_name, flow_name, start_datetime, recurring_period=None):
        """ Set existing flow to new schedule.

        If any schedule already set a flow, overwrite new one.

        :param project_name: The name of the project.
        :type project_name: str
        :param flow_name: The name of the flow.
        :type flow_name: str
        :param start_datetime: The datetime to schedule the flow.
        :type start_datetime: datetime
        :param recurring_period: Specifies the recursion period.
            Possible Values: M/Month, w/Weeks, d/Days, h/Hours, m/Minutes, s/Seconds
        :type recurring_period: str
        :return: Response json.

            :status: The status of attempt.
            :message: Success message.

        :rtype: dict
        :raises AjaxAPIError: Request is accepted successfully, but some error is occured in Azkaban Web Server.
        """
        schedule_time = start_datetime.strftime('%I,%M,%p,JST')
        schedule_date = start_datetime.strftime('%m/%d/%Y')
        flow_jobs = self.fetch_flow_jobs(project_name, flow_name)
        project_id = flow_jobs['projectId']
        for job in flow_jobs['nodes']:
            self.logger.info("Jobs: %s", job)

        api_url = urljoin(self.base_url, 'schedule')
        payload = {
            'session.id': self.__session_id,
            'ajax': 'scheduleFlow',
            'projectName': project_name,
            'projectId': project_id,
            'flow': flow_name,
            'scheduleTime': schedule_time,
            'scheduleDate': schedule_date
        }
        if recurring_period is not None:
            payload['is_recurring'] = 'on'
            payload['period'] = recurring_period
        self.logger.debug("%s data:%s", api_url, payload)

        res = requests.get(api_url, params=payload)
        res.raise_for_status()
        if 'error' in res.json():
            self.logger.error("Cannot schedule flow")
            self.logger.error(res.json()['error'])
            raise self.AjaxAPIError(res.json()['error'])
        else:
            self.logger.info("Success: Schedule flow - %s.%s", project_name, flow_name)
            self.logger.debug(res.json())
            return res.json()

    def fetch_all_project_list(self):
        """ Fetch the list of projects in specified Azkaban Web Server.

        :return: Project list.
        :rtype: list
        """
        api_url = urljoin(self.base_url, 'index?all')
        payload = {
            'azkaban.browser.session.id': self.__session_id
        }
        self.logger.debug("%s data:%s", api_url, payload)

        res = requests.get(api_url, cookies=payload)
        res.raise_for_status()
        html = res.text
        soup = BeautifulSoup(html)
        li_list = soup.find('ul', id='project-list').find_all('li')
        project_list = [li.find('div', {'class': 'project-info'}).find('h4').string for li in li_list]
        return project_list

    @staticmethod
    def __get_stdout_logger(logger_name, log_level_str):
        """ Get logger with stdout stream handler.

        :param logger_name: Logger identified name
        :type logger_name: str
        :param log_level_str: logger output level. (DEBUG, INFO, WARNING, ERROR)
        :type log_level_str: str
        :return: This class's root logger
        :rtype: logging.Logger
        """
        log_level = getattr(logging, log_level_str.upper())
        root_logger = logging.getLogger(logger_name)
        root_logger.setLevel(logging.DEBUG)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(log_level)
        stdout_handler.setFormatter(logging.Formatter("[%(asctime)s] %(name)s [%(levelname)s] %(message)s"))
        root_logger.addHandler(stdout_handler)
        return root_logger

    class AzkabanLoginError(Exception):
        """ Exception when login attempt failed.
        """
        pass

    class AjaxAPIError(Exception):
        """ Exception when requests is accepted but API call attempt failed.
        """
        pass
