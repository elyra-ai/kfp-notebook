#
# Copyright 2018-2020 IBM Corporation
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
import glob
import os
import subprocess
import sys

from abc import ABC, abstractmethod
from packaging import version
from typing import Optional, Any, Type, TypeVar
from urllib.parse import urlparse

# Inputs and Outputs separator character.  If updated,
# same-named variable in _notebook_op.py must be updated!
INOUT_SEPARATOR = ';'

# Setup forward reference for type hint on return from class factory method.  See
# https://stackoverflow.com/questions/39205527/can-you-annotate-return-type-when-value-is-instance-of-cls/39205612#39205612
F = TypeVar('F', bound='FileOpBase')


class FileOpBase(ABC):
    """Abstract base class for file-based operations"""
    filepath = None
    cos_client = None
    cos_bucket = None

    @classmethod
    def get_instance(cls: Type[F], **kwargs: Any) -> F:
        """Creates an appropriate subclass instance based on the extension of the filepath (-f) argument"""
        filepath = kwargs['filepath']
        if '.ipynb' in filepath:
            return NotebookFileOp(**kwargs)
        elif '.py' in filepath:
            return PythonFileOp(**kwargs)
        else:
            raise ValueError('Unsupported file type: {}'.format(filepath))

    def __init__(self, **kwargs: Any) -> None:
        """Initializes the FileOpBase instance"""
        import minio

        self.filepath = kwargs['filepath']
        self.input_params = kwargs or []
        self.cos_endpoint = urlparse(self.input_params.get('cos-endpoint'))
        self.cos_bucket = self.input_params.get('cos-bucket')
        # TODO check hardcoded false
        self.cos_client = minio.Minio(self.cos_endpoint.netloc,
                                      access_key=os.getenv('AWS_ACCESS_KEY_ID'),
                                      secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                                      secure=False)

    @abstractmethod
    def execute(self) -> None:
        """Execute the operation relative to derived class"""
        raise NotImplementedError("Method 'execute()' must be implemented by subclasses!")

    def process_dependencies(self) -> None:
        """Process dependencies

        If a dependency archive is present, it will be downloaded from object storage
        and expanded into the local directory.

        This method can be overridden by subclasses, although overrides should first
        call the superclass method.
        """
        archive_file = self.input_params.get('cos-dependencies-archive')
        self.get_file_from_object_storage(archive_file)

        print('Processing dependencies........')
        inputs = self.input_params.get('inputs')
        if inputs:
            input_list = inputs.split(INOUT_SEPARATOR)
            for file in input_list:
                self.get_file_from_object_storage(file.strip())

        print("TAR Archive pulled from Object Storage.")
        print("Unpacking........")
        subprocess.call(['tar', '-zxvf', archive_file])
        print("Unpacking Complete.")

    def process_outputs(self) -> None:
        """Process outputs

        If outputs have been specified, it will upload the appropriate files to object storage

        This method can be overridden by subclasses, although overrides should first
        call the superclass method.
        """
        print('Processing outputs........')
        outputs = self.input_params.get('outputs')
        if outputs:
            output_list = outputs.split(INOUT_SEPARATOR)
            for file in output_list:
                self.process_output_file(file.strip())

    def get_object_storage_filename(self, filename: str) -> str:
        """
        Function to pre-pend cloud storage working dir to file name

        :param filename: the local file
        :return: the full path of the object storage file
        """
        return os.path.join(self.input_params.get('cos-directory', ''), filename)

    def get_file_from_object_storage(self, file_to_get: str) -> None:
        """
        Utility function to get files from an object storage

        :param file_to_get: filename
        """

        print('Get file {} from bucket {}'.format(file_to_get, self.cos_bucket))
        object_to_get = self.get_object_storage_filename(file_to_get)

        self.cos_client.fget_object(bucket_name=self.cos_bucket,
                                    object_name=object_to_get,
                                    file_path=file_to_get)

    def put_file_to_object_storage(self, file_to_upload: str, object_name: Optional[str] = None) -> None:
        """
        Utility function to put files into an object storage
        :param file_to_upload: filename
        :param object_name: remote filename (used to rename)
        """

        object_to_upload = object_name
        if not object_to_upload:
            object_to_upload = file_to_upload

        print('Uploading file {} as {} to bucket {}'.format(file_to_upload, object_to_upload, self.cos_bucket))

        object_to_upload = self.get_object_storage_filename(object_to_upload)

        self.cos_client.fput_object(bucket_name=self.cos_bucket,
                                    object_name=object_to_upload,
                                    file_path=file_to_upload)

    def has_wildcard(self, filename):
        wildcards = ['*', '?']
        return bool(any(c in filename for c in wildcards))

    def process_output_file(self, output_file):
        """Puts the file to object storage.  Handles wildcards and directories. """

        matched_files = [output_file]
        if self.has_wildcard(output_file):  # explode the wildcarded file
            matched_files = glob.glob(output_file)

        for matched_file in matched_files:
            if os.path.isdir(matched_file):
                for file in os.listdir(matched_file):
                    self.process_output_file(os.path.join(matched_file, file))
            else:
                self.put_file_to_object_storage(matched_file)


class NotebookFileOp(FileOpBase):
    """Perform Notebook File Operation"""

    def execute(self) -> None:
        """Execute the Notebook and upload results to object storage"""
        notebook = os.path.basename(self.filepath)
        notebook_name = notebook.replace('.ipynb', '')
        notebook_output = notebook_name + '-output.ipynb'
        notebook_html = notebook_name + '.html'

        print("Executing notebook through Papermill: {} ==> {}".format(notebook, notebook_output))
        try:
            import papermill
            papermill.execute_notebook(
                notebook,
                notebook_output,
                kernel_name="python3"
                # parameters=
            )

            NotebookFileOp.convert_notebook_to_html(notebook_output, notebook_html)
            print("Uploading result Notebook back to Object Storage")
            self.put_file_to_object_storage(notebook_output, notebook)
            self.put_file_to_object_storage(notebook_html)

            self.process_outputs()
        except Exception as ex:
            # log in case of errors
            print("Unexpected error:", sys.exc_info()[0])
            NotebookFileOp.convert_notebook_to_html(notebook_output, notebook_html)

            print("Uploading errored Notebook back to Object Storage")
            self.put_file_to_object_storage(notebook_output, notebook)
            self.put_file_to_object_storage(notebook_html)
            raise ex

    @staticmethod
    def convert_notebook_to_html(notebook_file: str, html_file: str) -> str:
        """
        Function to convert a Jupyter notebook file (.ipynb) into an html file

        :param notebook_file: object storage client
        :param html_file: name of what the html output file should be
        :return: html_file: the converted notebook in html format
        """
        import nbconvert
        import nbformat

        print("Converting from ipynb to html....")
        nb = nbformat.read(notebook_file, as_version=4)
        html_exporter = nbconvert.HTMLExporter()
        data, resources = html_exporter.from_notebook_node(nb)
        with open(html_file, "w") as f:
            f.write(data)
            f.close()

        return html_file


class PythonFileOp(FileOpBase):
    """Perform Python File Operation"""

    def execute(self) -> None:
        """Execute the Python script and upload results to object storage"""
        python_script = os.path.basename(self.filepath)
        python_script_name = python_script.replace('.py', '')
        python_script_output = python_script_name + '.log'

        print("Executing Python Script : {} ==> {}".format(python_script, python_script_output))
        try:
            # with open(python_script_output, "w") as output_file:
            #     subprocess.check_call([sys.executable, python_script], stdout=output_file)

            subprocess.check_call(['python', python_script])

            # print("Uploading Python Script execution log back to Object Storage")
            # put_file_to_object_storage(python_script_output, python_script_output)

            self.process_outputs()
        except Exception as ex:
            # log in case of errors
            print("Unexpected error: {}".format(sys.exc_info()[0]))
            print("Error details: {}".format(ex))

            # print("Uploading Errored Notebook back to Object Storage")
            # put_file_to_object_storage(python_script_output, python_script_output)
            raise ex


class OpUtil(object):
    """Utility functions for preparing file execution."""
    @classmethod
    def package_install(cls, user_volume_path) -> None:

        elyra_packages = cls.package_list_to_dict("requirements-elyra.txt")
        current_packages = cls.package_list_to_dict("requirements-current.txt")
        to_install_list = []

        for package, ver in elyra_packages.items():
            if package in current_packages:
                if "git+" in current_packages[package]:
                    print("WARNING: Source package %s found already installed from %s. This may "
                          "conflict with the required version: %s . Skipping..." %
                          (package, current_packages[package], ver))
                elif isinstance(version.parse(current_packages[package]), version.LegacyVersion):
                    print("WARNING: Package %s found with unsupported Legacy version "
                          "scheme %s already installed. Skipping..." %
                          (package, current_packages[package]))
                elif version.parse(ver) > version.parse(current_packages[package]):
                    print("Updating %s package from version %s to %s..." % (package, current_packages[package], ver))
                    to_install_list.append(package+'=='+ver)
                elif version.parse(ver) < version.parse(current_packages[package]):
                    print("Newer %s package with version %s already installed. Skipping..." %
                          (package, current_packages[package]))
            else:
                print("Package not found. Installing %s package with version %s..." % (package, ver))
                to_install_list.append(package+'=='+ver)

        if to_install_list:
            if user_volume_path:
                to_install_list.insert(0, '--target=' + user_volume_path)
            subprocess.check_call([sys.executable, '-m', 'pip', 'install'] + to_install_list)

        subprocess.check_call([sys.executable, '-m', 'pip', 'freeze'])
        print("Package Installation Complete.....")

    @classmethod
    def package_list_to_dict(cls, filename: str) -> dict:
        package_dict = {}
        with open(filename) as fh:
            for line in fh:
                if line[0] != '#':
                    if " @ " in line:
                        package_name, package_version = line.strip('\n').split(sep=" @ ")
                    elif "===" in line:
                        package_name, package_version = line.strip('\n').split(sep="===")
                    else:
                        package_name, package_version = line.strip('\n').split(sep="==")

                    package_dict[package_name] = package_version

        return package_dict

    @classmethod
    def parse_arguments(cls, args) -> dict:
        import argparse

        print("Parsing Arguments.....")
        parser = argparse.ArgumentParser()
        parser.add_argument('-e', '--cos-endpoint', dest="cos-endpoint", help='Cloud object storage endpoint',
                            required=True)
        parser.add_argument('-b', '--cos-bucket', dest="cos-bucket", help='Cloud object storage bucket to use',
                            required=True)
        parser.add_argument('-d', '--cos-directory', dest="cos-directory",
                            help='Working directory in cloud object storage bucket to use', required=True)
        parser.add_argument('-t', '--cos-dependencies-archive', dest="cos-dependencies-archive",
                            help='Archive containing notebook and dependency artifacts', required=True)
        parser.add_argument('-f', '--file', dest="filepath", help='File to execute', required=True)
        parser.add_argument('-o', '--outputs', dest="outputs", help='Files to output to object store', required=False)
        parser.add_argument('-i', '--inputs', dest="inputs", help='Files to pull in from parent node', required=False)
        parser.add_argument('-p', '--user-volume-path', dest="user-volume-path",
                            help='Directory in Volume to install python libraries into', required=False)
        parsed_args = vars(parser.parse_args(args))

        return parsed_args


def main():

    # Setup packages and gather arguments
    input_params = OpUtil.parse_arguments(sys.argv[1:])
    OpUtil.package_install(input_params.get('user-volume-path'))

    # Create the appropriate instance, process dependencies and execute the operation
    file_op = FileOpBase.get_instance(**input_params)

    file_op.process_dependencies()

    file_op.execute()

    print("Execution and Upload Complete.")


if __name__ == '__main__':
    main()
