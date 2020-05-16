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
    parser.add_argument('-e', '--endpoint', dest="endpoint", help='Cloud object storage endpoint', required=True)
    parser.add_argument('-b', '--bucket', dest="bucket", help='Cloud object storage bucket to use', required=True)
    parser.add_argument('-d', '--directory', dest="directory", help='Directory in cloud object storage bucket to use', required=True)
    parser.add_argument('-t', '--tar-archive', dest="tar-archive", help='Archive containing notebook and dependency artifacts', required=True)
    parser.add_argument('-i', '--input', dest="input", help='Notebook to execute', required=True)
    parser.add_argument('-o', '--output', dest="output", help='Executed Notebook ', required=True)
    parser.add_argument('-p', '--pipeline-outputs', dest="pipeline-outputs", help='Files to output to object storage', required=True)
    parser.add_argument('-l', '--pipeline-inputs', dest="pipeline-inputs", help='Files to pull in from parent node', required=False)
    parser.add_argument('-m', '--output-html', dest="output-html", help='Executed notebook converted to HTML', required=True)
    args = vars(parser.parse_args())

    return args


def notebook_to_html(notebook_file, html_file):
    """ Function to convert a Jupyter notebook file (.ipynb) into an html file
                Args:
                    notebook_file: object storage client
                    html_file: name of what the html output file should be

                Returns:
                    html_file: the converted notebook in html format
                """
    print("Converting from ipynb to html....")
    nb = nbformat.read(notebook_file, as_version=4)
    html_exporter = nbconvert.HTMLExporter()
    data, resources = html_exporter.from_notebook_node(nb)
    with open(html_file, "w") as f:
        f.write(data)
        f.close()
    return html_file


def get_file_object_store(client, bucket_name, file_to_get, subdir):
    """ Abstracted function to get files from an object storage
                Args:
                    client: object storage client
                    bucket_name: bucket to place the files into
                    file_to_get: filename
                """

    print('Get file {} from bucket {}'.format(file_to_get, bucket_name))

    try:
        client.fget_object(bucket_name=bucket_name,
                           object_name=subdir+file_to_get,
                           file_path=file_to_get)
    except minio.error.ResponseError as err:
        print(err)


def put_file_object_store(client, bucket_name, file_to_upload, subdir):
    """ Abstracted function to put files into an object storage
            Args:
                client: object storage client
                bucket_name: bucket to place the files into
                file_to_upload: filename
            """
    print('Uploading file {} to bucket {}'.format(file_to_upload, bucket_name))

    try:
        client.fput_object(bucket_name=bucket_name,
                           object_name=subdir+file_to_upload,
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

    import minio
    import argparse
    import papermill
    import nbconvert
    import nbformat
    import os
    from urllib.parse import urlparse

    print("Imports Complete.....")

    input_params = parse_arguments()

    cos_endpoint = urlparse(input_params['endpoint'])
    cos_client = minio.Minio(cos_endpoint.netloc,
                             access_key=os.getenv('AWS_ACCESS_KEY_ID'),
                             secret_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                             secure=False)

    cos_dir_pre = input_params['directory'] + '/'

    get_file_object_store(cos_client, input_params['bucket'], input_params['tar-archive'], cos_dir_pre)

    print('Processing dependencies........')
    if 'pipeline-inputs' in input_params.keys():
        input_list = input_params['pipeline-inputs'].split(",")
        if input_list and len(input_list) > 0:
            for file in input_list:
                if file and file != 'None':
                    get_file_object_store(cos_client, input_params['bucket'], file, cos_dir_pre)

    print("TAR Archive pulled from Object Storage.")
    print("Unpacking........")
    subprocess.call(['tar', '-zxvf', input_params["tar-archive"]])
    print("Unpacking Complete.")
    print("Executing notebook through Papermill: {} ==> {}"
          .format(str(input_params['input']),
                  str(input_params['output'])))

    try:
        papermill.execute_notebook(
            input_params['input'],
            input_params['output'],
            kernel_name="python3"
            # parameters=
        )
        output_html_file = notebook_to_html(input_params['output'], input_params['output-html'])
        print("Uploading Results back to Object Storage")
        put_file_object_store(cos_client, input_params["bucket"], output_html_file, cos_dir_pre)
        put_file_object_store(cos_client, input_params["bucket"], input_params['output'], cos_dir_pre)

        print('Processing outputs........')
        if 'pipeline-outputs' in input_params.keys():
            output_list = input_params['pipeline-outputs'].split(",")
            if output_list and len(output_list) > 0:
                for file in output_list:
                    if file and file != 'None':
                        put_file_object_store(cos_client, input_params['bucket'], file, cos_dir_pre)
    except:
        # on error - upload the failed notebook with `error` suffix for troubleshooting purposes
        print("Unexpected error:", sys.exc_info()[0])
        print("Processing Errored Notebook")
        output_error_html = input_params['output-html'].replace('.html', '-error.html')
        output_html_file = notebook_to_html(input_params['output'], output_error_html)

        output_error_ipynb = input_params['output'].replace('.ipynb', '-error.ipynb')
        shutil.copy(input_params['input'], output_error_ipynb)

        print("Uploading Errored Notebook back to Object Storage")
        put_file_object_store(cos_client, input_params["bucket"], output_error_html, cos_dir_pre)
        put_file_object_store(cos_client, input_params["bucket"], output_error_ipynb, cos_dir_pre)

        raise

    print("Upload Complete.")
