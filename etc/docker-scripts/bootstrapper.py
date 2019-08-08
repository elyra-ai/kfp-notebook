import subprocess
import sys


def import_with_auto_install(package):
    try:
        return __import__(package)
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', package])
    return __import__(package)


def parse_arguments():
    print("Parsing Arguments.....")
    parser = argparse.ArgumentParser(description='Description of your program')
    parser.add_argument('-u', '--user', dest="user", help='Description for foo argument', required=True)
    parser.add_argument('-e', '--endpoint', dest="endpoint", help='Description for foo argument', required=True)
    parser.add_argument('-p', '--password', dest="password", help='Description for bar argument', required=True)
    parser.add_argument('-t', '--tar-archive', dest="tar-archive", help='Description for foo argument', required=True)
    parser.add_argument('-b', '--bucket', dest="bucket", help='Description for bar argument', required=True)
    parser.add_argument('-i', '--input', dest="input", help='Description for bar argument', required=True)
    parser.add_argument('-o', '--output', dest="output", help='Description for bar argument', required=True)
    parser.add_argument('-m', '--output-html', dest="output-html", help='Description for bar argument', required=True)
    args = vars(parser.parse_args())

    return args


def notebook_to_html(notebook_file, html_file):
    print("Converting from ipynb to html....")
    nb = nbformat.read(notebook_file, as_version=4)
    html_exporter = nbconvert.HTMLExporter()
    data, resources = html_exporter.from_notebook_node(nb)
    with open(html_file, "w") as f:
        f.write(data)
        f.close()
    return html_file


def put_file_object_store(client, bucket, file_to_upload):
    client.fput_object(bucket_name=bucket,
                       object_name=file_to_upload,
                       file_path=file_to_upload)


if __name__ == '__main__':
    import_with_auto_install("papermill")
    import_with_auto_install("minio")
    import_with_auto_install("argparse")
    import_with_auto_install("nbconvert")
    import_with_auto_install("nbformat")

    import minio
    import argparse
    import papermill
    import nbconvert
    import nbformat

    print("Imports Complete.....")

    input_params = parse_arguments()

    # Initialize minioClient with an endpoint and access/secret keys.
    minio_client = minio.Minio(input_params["endpoint"],
                               access_key=input_params["user"],
                               secret_key=input_params["password"],
                               secure=False)

    print("Creating work directory")
    working_dir = 'jupyter-work-dir'
    subprocess.call(['mkdir', '-p', working_dir, '&&', 'cd', working_dir])
    print("Created and moving into working directory")

    minio_client.fget_object(bucket_name=input_params["bucket"],
                             object_name=input_params["tar-archive"],
                             file_path=input_params["tar-archive"])

    print("TAR Archive pulled from Object Storage.")
    print("Unpacking........")
    subprocess.call(['tar', '-zxvf', input_params["tar-archive"]])
    print("Unpacking Complete.")
    print("Executing notebook through Papermill....")
    papermill.execute_notebook(
        input_params['input'],
        input_params['output']
        # parameters=
    )
    output_html_file = notebook_to_html(input_params['output'], input_params['output-html'])

    print("Uploading Results back to Object Storage")
    put_file_object_store(minio_client, input_params["bucket"], output_html_file)
    put_file_object_store(minio_client, input_params["bucket"], input_params['output'])

    print("Upload Complete.")
