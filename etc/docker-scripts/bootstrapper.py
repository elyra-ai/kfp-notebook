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
import shutil
import subprocess
import sys


def import_with_auto_install(package):
    try:
        return __import__(package)
    except ImportError:
        print('Updating package {}'.format(package))
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', package])


def parse_arguments():
    print("Parsing Arguments.....")
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-e', '--cos-endpoint', dest="cos-endpoint", help='Cloud object storage endpoint', required=True)
    parser.add_argument('-b', '--cos-bucket', dest="cos-bucket", help='Cloud object storage bucket to use', required=True)
    parser.add_argument('-d', '--cos-directory', dest="cos-directory", help='Working directory in cloud object storage bucket to use', required=True)
    parser.add_argument('-t', '--cos-dependencies-archive', dest="cos-dependencies-archive", help='Archive containing notebook and dependency artifacts', required=True)
    parser.add_argument('-i', '--notebook', dest="notebook", help='Notebook to execute', required=True)
    parser.add_argument('-p', '--outputs', dest="outputs", help='Files to output to object store', required=True)
    parser.add_argument('-l', '--inputs', dest="inputs", help='Files to pull in from parent node', required=False)
    args = vars(parser.parse_args())

    return args


def convert_notebook_to_html(notebook_file, html_file):
    """
    Function to convert a Jupyter notebook file (.ipynb) into an html file

    :param notebook_file: object storage client
    :param html_file: name of what the html output file should be
    :return: html_file: the converted notebook in html format
    """
    print("Converting from ipynb to html....")
    nb = nbformat.read(notebook_file, as_version=4)
    html_exporter = nbconvert.HTMLExporter()
    data, resources = html_exporter.from_notebook_node(nb)
    with open(html_file, "w") as f:
        f.write(data)
        f.close()
    return html_file


def get_object_storage_filename(filename):
    """
    Function to pre-pend cloud storage working dir to file name

    :param filename: the local file
    :return: the full path of the object storage file
    """
    return os.path.join(input_params['cos-directory'], filename)


def get_file_from_object_storage(client, bucket_name, file_to_get):
    """
    Utility function to get files from an object storage

    :param client: object storage client
    :param bucket_name: bucket where files are located
    :param file_to_get: filename
    """

    print('Get file {} from bucket {}'.format(file_to_get, bucket_name))
    object_to_get = get_object_storage_filename(file_to_get)

    try:
        client.fget_object(bucket_name=bucket_name,
                           object_name=object_to_get,
                           file_path=file_to_get)
    except minio.error.ResponseError as err:
        print(err)


def put_file_to_object_storage(client, bucket_name, file_to_upload, object_name=None):
    """
    Utility function to put files into an object storage
    :param client: object storage client
    :param bucket_name: bucket where files are located
    :param file_to_upload: filename
    :param object_name: remote filename (used to rename)
    """

    object_to_upload = object_name
    if not object_to_upload:
        object_to_upload = file_to_upload

    print('Uploading file {} as {} to bucket {}'.format(file_to_upload, object_to_upload, bucket_name))

    object_to_upload = get_object_storage_filename(object_to_upload)

    try:
        client.fput_object(bucket_name=bucket_name,
                           object_name=object_to_upload,
                           file_path=file_to_upload)
    except minio.error.ResponseError as err:
        print(err)
        raise


if __name__ == '__main__':
    import_with_auto_install("jupyter_client")
    import_with_auto_install("papermill")
    import_with_auto_install("minio")
    import_with_auto_install("argparse")
    import_with_auto_install("nbconvert")
    import_with_auto_install("nbformat")
    import_with_auto_install("ipykernel")

    import os
    import minio
    import argparse
    import papermill
    import nbconvert
    import nbformat

    from urllib.parse import urlparse

    print("Imports Complete.....")

    input_params = parse_arguments()

    cos_endpoint = urlparse(input_params['cos-endpoint'])
    cos_client = minio.Minio(cos_endpoint.netloc,
                             access_key=os.getenv('AWS_ACCESS_KEY_ID'),
                             secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                             secure=False)

    get_file_from_object_storage(cos_client, input_params['cos-bucket'], input_params['cos-dependencies-archive'])

    print('Processing dependencies........')
    if 'inputs' in input_params.keys():
        input_list = input_params['inputs'].split(",")
        if input_list and len(input_list) > 0:
            for file in input_list:
                if file and file != 'None':
                    get_file_from_object_storage(cos_client, input_params['cos-bucket'], file)

    print("TAR Archive pulled from Object Storage.")
    print("Unpacking........")
    subprocess.call(['tar', '-zxvf', input_params["cos-dependencies-archive"]])
    print("Unpacking Complete.")

    # Execute notebook
    notebook = os.path.basename(str(input_params['notebook']))
    notebook_name = notebook.replace('.ipynb', '')
    notebook_output = notebook_name + '-output.ipynb'
    notebook_html = notebook_name + '.html'

    print("Executing notebook through Papermill: {} ==> {}"
          .format(notebook, notebook_output))

    try:
        papermill.execute_notebook(
            notebook,
            notebook_output,
            kernel_name="python3"
            # parameters=
        )

        convert_notebook_to_html(notebook_output, notebook_html)
        print("Uploading Result Notebook back to Object Storage")
        put_file_to_object_storage(cos_client, input_params['cos-bucket'], notebook_output, notebook)
        put_file_to_object_storage(cos_client, input_params['cos-bucket'], notebook_html)


        print('Processing outputs........')
        if 'outputs' in input_params.keys():
            output_list = input_params['outputs'].split(",")
            if output_list and len(output_list) > 0:
                for file in output_list:
                    if file and file != 'None':
                        put_file_to_object_storage(cos_client, input_params['cos-bucket'], file)
    except:
        # log in case of errors
        print("Unexpected error:", sys.exc_info()[0])
        convert_notebook_to_html(notebook_output, notebook_html)

        print("Uploading Errored Notebook back to Object Storage")
        put_file_to_object_storage(cos_client, input_params['cos-bucket'], notebook_output, notebook)
        put_file_to_object_storage(cos_client, input_params['cos-bucket'], notebook_html)

        raise

    print("Upload Complete.")
