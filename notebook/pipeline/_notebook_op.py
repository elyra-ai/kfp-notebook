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

import kfp
import notebook

from kfp.dsl._container_op import BaseOp, ContainerOp


class NotebookOp(ContainerOp):

    def __init__(self,
                 notebook: str,
                 cos_endpoint: str,
                 cos_user: str,
                 cos_password: str,
                 **kwargs):
        """Create a new instance of ContainerOp.
        Args:
          notebook: name of the notebook that will be executed per this
              operation
          kwargs: name, sidecars & is_exit_handler. See ContainerOp definition
        """
        if 'image' not in kwargs:
            kwargs['image'] = 'lresende/notebook-kubeflow-pipeline:dev'

        if notebook is None:
            ValueError("You need to provide a notebook.")

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

        super().__init__(**kwargs,
            arguments=[
                '--endpoint', self.cos_endpoint,
                '--user', self.cos_user,
                '--password', self.cos_password,
                '--bucket', 'oscon',
                '--input', self.notebook_name,
                '--output', self.notebook_result,
                '--output_html', self.notebook_html,
            ]
        )

    def _get_file_name_with_extension(self, name, extension):
        name_with_extension = name
        if extension not in name_with_extension:
            name_with_extension = '{}.{}'.format(name, extension)

        return name_with_extension
