# -*- coding: utf-8 -*-
#
# Copyright 2018-2019 IBM Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from kfp.dsl._container_op import BaseOp, ContainerOp


"""
The NotebookOp uses a python script to bootstrap the user supplied image with the required dependencies. 
In order for the script run properly, the image used, must at a minimum, have the 'curl' utility available
and have python3
"""


class NotebookOp(ContainerOp):

    def __init__(self,
                 notebook: str,
                 cos_endpoint: str,
                 cos_user: str,
                 cos_password: str,
                 cos_bucket: str,
                 cos_pull_archive: str,
                 **kwargs):
        """Create a new instance of ContainerOp.
        Args:
          notebook: name of the notebook that will be executed per this
              operation
          kwargs: name, image, sidecars & is_exit_handler. See ContainerOp definition
        """

        self.notebook = notebook
        self.notebook_name = \
            self._get_file_name_with_extension(notebook, 'ipynb')
        self.notebook_result = \
            self._get_file_name_with_extension(notebook + '_output', 'ipynb')
        self.notebook_html = \
            self._get_file_name_with_extension(notebook + '_output', 'html')
        self.cos_endpoint = cos_endpoint
        self.cos_user = cos_user
        self.cos_password = cos_password
        self.cos_bucket = cos_bucket
        self.cos_pull_archive = cos_pull_archive

        if 'image' not in kwargs:  # default image used if none specified
            kwargs['image'] = 'lresende/notebook-kubeflow-pipeline:dev'

        if notebook is None:
            ValueError("You need to provide a notebook.")

        if 'arguments' not in kwargs:
            """ If no arguments are passed, we use our own.
                If ['arguments'] are set, we assume container's ENTRYPOINT is set and dependencies are installed
                NOTE: Images being pulled must have python3 available on PATH and cURL utility
            """
            if 'bootstrap_script' not in kwargs:
                """ If bootstrap_script arg with URL not provided, use the one baked in here.
                """
                self.bootstrap_script_url = 'https://raw.github.ibm.com/ai-workspace/kfp-notebook/' \
                                            'master/etc/docker-scripts/' \
                                            'bootstrapper.py?token=AAAcKx3m6ZFVbMNDYJCoG0DHhiWL-D_Jks5dXuAKwA%3D%3D'
            else:
                self.bootstrap_script_url = kwargs['bootstrap_script']

            kwargs['command'] = ['sh', '-c']
            kwargs['arguments'] = ['curl -H "Cache-Control: no-cache" -L %s --output bootstrapper.py && '
                                   'python bootstrapper.py '
                                   '--endpoint %s '
                                   '--user %s '
                                   '--password %s '
                                   '--bucket %s '
                                   '--tar-archive %s '
                                   '--input %s '
                                   '--output %s '
                                   '--output-html %s' % (
                                       self.bootstrap_script_url,
                                       self.cos_endpoint,
                                       self.cos_user,
                                       self.cos_password,
                                       self.cos_bucket,
                                       self.cos_pull_archive,
                                       self.notebook_name,
                                       self.notebook_result,
                                       self.notebook_html
                                       )
                                   ]

        super().__init__(**kwargs)

    def _get_file_name_with_extension(self, name, extension):
        """ Simple function to construct a string filename
        Args:
            name: name of the file
            extension: name of the file

        Returns:
            name_with_extension: string filename
        """
        name_with_extension = name
        if extension not in name_with_extension:
            name_with_extension = '{}.{}'.format(name, extension)

        return name_with_extension
