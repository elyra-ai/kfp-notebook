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

import json
import logging
import minio
import nbformat
import os
import papermill
import pytest
import mock
import sys

from pathlib import Path
from py_essentials import hashing as hs

from tempfile import TemporaryFile
sys.path.append('etc/docker-scripts/')
import bootstrapper


# To run this test from an IDE:
# 1. set PYTHONPATH='`path-to-repo`/etc/docker-scripts' and working directory to `path-to-repo`
# 2. Manually launch test_minio container: docker run --name test_minio -d -p 9000:9000 minio/minio server /data
#    (this is located in Makefile)
#
# NOTE: Any changes to etc/tests/resources/test-notebookA.ipynb require an
# update of etc/tests/resources/test-archive.tgz  using the command below:
# tar -cvzf test-archive.tgz test-notebookA.ipynb


MINIO_HOST_PORT = os.getenv("MINIO_HOST_PORT", "127.0.0.1:9000")


@pytest.fixture(scope='function')
def s3_setup():
    bucket_name = "test-bucket"
    cos_client = minio.Minio(MINIO_HOST_PORT,
                             access_key="minioadmin",
                             secret_key="minioadmin",
                             secure=False)
    cos_client.make_bucket(bucket_name)

    yield cos_client

    cleanup_files = cos_client.list_objects(bucket_name, recursive=True)
    for file in cleanup_files:
        cos_client.remove_object(bucket_name, file.object_name)
    cos_client.remove_bucket(bucket_name)


def main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict):
    """Primary body for main method testing..."""
    monkeypatch.setattr(bootstrapper.OpUtil, 'parse_arguments', lambda x: argument_dict)
    monkeypatch.setattr(bootstrapper.OpUtil, 'package_install', mock.Mock(return_value=True))

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("TEST_ENV_VAR1", "test_env_var1")

    s3_setup.fput_object(bucket_name=argument_dict['cos-bucket'],
                         object_name="test-directory/test-file.txt",
                         file_path="etc/tests/resources/test-requirements-elyra.txt")
    s3_setup.fput_object(bucket_name=argument_dict['cos-bucket'],
                         object_name="test-directory/test,file.txt",
                         file_path="etc/tests/resources/test-bad-requirements-elyra.txt")
    s3_setup.fput_object(bucket_name=argument_dict['cos-bucket'],
                         object_name="test-directory/test-archive.tgz",
                         file_path="etc/tests/resources/test-archive.tgz")

    with tmpdir.as_cwd():
        bootstrapper.main()
        test_file_list = ['test-archive.tgz',
                          'test-file.txt',
                          'test,file.txt',
                          'test-file/test-file-copy.txt',
                          'test-file/test,file/test,file-copy.txt',
                          'test-notebookA.ipynb',
                          'test-notebookA-output.ipynb',
                          'test-notebookA.html']
        # Ensure working directory has all the files.
        for file in test_file_list:
            assert os.path.isfile(file)
        # Ensure upload directory has all the files EXCEPT the output notebook
        # since it was it is uploaded as the input notebook (test-notebookA.ipynb)
        # (which is included in the archive at start).
        for file in test_file_list:
            if file != 'test-notebookA-output.ipynb':
                assert s3_setup.stat_object(bucket_name=argument_dict['cos-bucket'],
                                            object_name="test-directory/" + file)
                if file == "test-notebookA.html":
                    with open("test-notebookA.html") as html_file:
                        assert 'TEST_ENV_VAR1: test_env_var1' in html_file.read()


def _get_operation_instance(monkeypatch, s3_setup):
    config = {
        'cos-endpoint': 'http://' + MINIO_HOST_PORT,
        'cos-user': 'minioadmin',
        'cos-password': 'minioadmin',
        'cos-bucket': 'test-bucket',
        'filepath': 'untitled.ipynb'
    }

    op = bootstrapper.FileOpBase.get_instance(**config)

    # use the same minio instance used by the test
    # to avoid access denied errors when two minio
    # instances exist
    monkeypatch.setattr(op, "cos_client", s3_setup)

    return op


def test_main_method(monkeypatch, s3_setup, tmpdir):
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt;test,file.txt',
                     'outputs': 'test-file/test-file-copy.txt;test-file/test,file/test,file-copy.txt',
                     'user-volume-path': None}
    main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict)


def test_main_method_with_wildcard_outputs(monkeypatch, s3_setup, tmpdir):
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt;test,file.txt',
                     'outputs': 'test-file/*',
                     'user-volume-path': None}
    main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict)


def test_main_method_with_dir_outputs(monkeypatch, s3_setup, tmpdir):
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt;test,file.txt',
                     'outputs': 'test-file',  # this is the directory that contains the outputs
                     'user-volume-path': None}
    main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict)


def is_writable_dir(path):
    """Helper method determines whether 'path' is a writable directory
    """
    try:
        with TemporaryFile(mode='w', dir=path) as t:
            t.write('1')
        return True
    except Exception:
        return False


def test_process_metrics_method_no_metadata_file(monkeypatch, s3_setup, tmpdir):
    """Test for process_metrics

    Verifies that process_metrics produces a valid KFP UI metadata file if
    the node's script | notebook did not generate this metadata file.
    """
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt;test,file.txt',
                     'outputs': 'test-file/test-file-copy.txt;test-file/test,file/test,file-copy.txt',
                     'user-volume-path': None}

    output_path = Path('/tmp')
    if is_writable_dir(output_path) is False:
        pytest.skip('Not supported in this environment')

    metadata_file = output_path / 'mlpipeline-ui-metadata.json'

    os.remove(metadata_file)

    main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict)
    # process_metrics generates a file named
    # /tmp/mlpipeline-ui-metadata.json should now be present

    try:
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
            assert metadata.get('outputs') is not None
            assert isinstance(metadata['outputs'], list)
            assert len(metadata['outputs']) == 1
            assert metadata['outputs'][0]['storage'] == 'inline'
            assert metadata['outputs'][0]['type'] == 'markdown'
            assert '{}/{}/{}'.format(argument_dict['cos-endpoint'],
                                     argument_dict['cos-bucket'],
                                     argument_dict['cos-directory']) \
                in metadata['outputs'][0]['source']
            assert argument_dict['cos-dependencies-archive']\
                in metadata['outputs'][0]['source']
    except AssertionError:
        raise
    except Exception as ex:
        # Potential reasons for failures:
        # file not found, invalid JSON
        print('Validation of "{}" failed: {}'.format(str(ex), ex))
        assert False


def test_process_metrics_method_valid_metadata_file(monkeypatch, s3_setup, tmpdir):
    """Test for process_metrics

    Verifies that process_metrics produces a valid KFP UI metadata file if
    the node's script | notebook did already generate this metadata file. The
    content of that file should be preserved.
    """
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt;test,file.txt',
                     'outputs': 'test-file/test-file-copy.txt;test-file/test,file/test,file-copy.txt',
                     'user-volume-path': None}

    output_path = Path('/tmp')
    if is_writable_dir(output_path) is False:
        pytest.skip('Not supported in this environment')

    input_metadata_file = 'mlpipeline-ui-metadata.json'
    output_metadata_file = output_path / input_metadata_file

    os.remove(output_metadata_file)

    #
    # Simulate some custom metadata that the script | notebook produced
    #
    custom_metadata = {
        'some_property': 'some property value',
        'outputs': [
            {
                'source': 'gs://project/bucket/file.md',
                'type': 'markdown'
            }
        ]
    }

    with tmpdir.as_cwd():
        with open(input_metadata_file, 'w') as f:
            json.dump(custom_metadata, f)

    main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict)
    # /tmp/mlpipeline-ui-metadata.json should now have been updated

    try:
        with open(output_metadata_file, 'r') as f:
            metadata = json.load(f)
            assert metadata.get('some_property') is not None
            assert metadata['some_property'] == custom_metadata['some_property']
            assert metadata.get('outputs') is not None
            assert isinstance(metadata['outputs'], list)
            assert len(metadata['outputs']) == 2
            for output in metadata['outputs']:
                if output.get('storage') is not None:
                    assert output['storage'] == 'inline'
                    assert output['type'] == 'markdown'
                    assert '{}/{}/{}'.format(argument_dict['cos-endpoint'],
                                             argument_dict['cos-bucket'],
                                             argument_dict['cos-directory']) \
                        in output['source']
                    assert argument_dict['cos-dependencies-archive']\
                        in output['source']
                else:
                    assert output['type'] ==\
                        custom_metadata['outputs'][0]['type']
                    assert output['source'] ==\
                        custom_metadata['outputs'][0]['source']
    except AssertionError:
        raise
    except Exception as ex:
        # Potential reasons for failures:
        # file not found, invalid JSON
        print('Validation of "{}" failed: {}'.format(str(ex), ex))
        assert False


def test_process_metrics_method_invalid_metadata_file(monkeypatch, s3_setup, tmpdir):
    """Test for process_metrics

    Verifies that process_metrics produces a valid KFP UI metadata file if
    the node's script | notebook generate an invalid metadata file, which
    cannot be merged and is therefore overwritten.
    """
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt;test,file.txt',
                     'outputs': 'test-file/test-file-copy.txt;test-file/test,file/test,file-copy.txt',
                     'user-volume-path': None}

    output_path = Path('/tmp')
    if is_writable_dir(output_path) is False:
        pytest.skip('Not supported in this environment')

    input_metadata_file = 'mlpipeline-ui-metadata.json'
    output_metadata_file = output_path / input_metadata_file

    os.remove(output_metadata_file)

    #
    # Populate the metadata file with some custom data that's not JSON
    #

    with tmpdir.as_cwd():
        with open(input_metadata_file, 'w') as f:
            f.write('I am not a valid JSON data structure')
            f.write('1,2,3,4,5,6,7')

    main_method_setup_execution(monkeypatch, s3_setup, tmpdir, argument_dict)

    # process_metrics replaces the existing metadata file
    # because its content cannot be merged

    try:
        with open(output_metadata_file, 'r') as f:
            metadata = json.load(f)
            assert metadata.get('outputs') is not None
            assert isinstance(metadata['outputs'], list)
            assert len(metadata['outputs']) == 1
            assert metadata['outputs'][0]['storage'] == 'inline'
            assert metadata['outputs'][0]['type'] == 'markdown'
            assert '{}/{}/{}'.format(argument_dict['cos-endpoint'],
                                     argument_dict['cos-bucket'],
                                     argument_dict['cos-directory']) \
                in metadata['outputs'][0]['source']
            assert argument_dict['cos-dependencies-archive']\
                in metadata['outputs'][0]['source']
    except AssertionError:
        raise
    except Exception as ex:
        # Potential reasons for failures:
        # file not found, invalid JSON
        print('Validation of "{}" failed: {}'.format(str(ex), ex))
        assert False


def test_fail_bad_endpoint_main_method(monkeypatch, tmpdir):
    argument_dict = {'cos-endpoint': MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'filepath': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt',
                     'outputs': 'test-file/test-file-copy.txt',
                     'user-volume-path': None}
    monkeypatch.setattr(bootstrapper.OpUtil, "parse_arguments", lambda x: argument_dict)
    monkeypatch.setattr(bootstrapper.OpUtil, 'package_install', mock.Mock(return_value=True))

    mocked_func = mock.Mock(return_value="default", side_effect=['test-archive.tgz',
                                                                 'test-file.txt',
                                                                 'test-notebookA-output.ipynb',
                                                                 'test-notebookA.html',
                                                                 'test-file.txt'])
    monkeypatch.setattr(bootstrapper.FileOpBase, "get_object_storage_filename", mocked_func)

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    with tmpdir.as_cwd():
        with pytest.raises(minio.error.InvalidEndpointError):
            bootstrapper.main()


def test_fail_bad_notebook_main_method(monkeypatch, s3_setup, tmpdir):
    argument_dict = {'cos-endpoint': 'http://' + MINIO_HOST_PORT,
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-bad-archiveB.tgz',
                     'filepath': 'etc/tests/resources/test-bad-notebookB.ipynb',
                     'inputs': 'test-file.txt',
                     'outputs': 'test-file/test-copy-file.txt',
                     'user-volume-path': None}

    monkeypatch.setattr(bootstrapper.OpUtil, "parse_arguments", lambda x: argument_dict)
    monkeypatch.setattr(bootstrapper.OpUtil, 'package_install', mock.Mock(return_value=True))

    mocked_func = mock.Mock(return_value="default", side_effect=['test-bad-archiveB.tgz',
                                                                 'test-file.txt',
                                                                 'test-bad-notebookB-output.ipynb',
                                                                 'test-bad-notebookB.html',
                                                                 'test-file.txt'])
    monkeypatch.setattr(bootstrapper.FileOpBase, "get_object_storage_filename", mocked_func)

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3_setup.fput_object(bucket_name=argument_dict['cos-bucket'],
                         object_name="test-file.txt",
                         file_path="README.md")
    s3_setup.fput_object(bucket_name=argument_dict['cos-bucket'],
                         object_name="test-bad-archiveB.tgz",
                         file_path="etc/tests/resources/test-bad-archiveB.tgz")

    with tmpdir.as_cwd():
        with pytest.raises(papermill.exceptions.PapermillExecutionError):
            bootstrapper.main()


def test_package_installation(monkeypatch, virtualenv):
    elyra_dict = {'ipykernel': '5.3.0',
                  'ansiwrap': '0.8.4',
                  'packaging': '20.0',
                  'text-extensions-for-pandas': '2.0.0'
                  }
    to_install_dict = {'bleach': '3.1.5',
                       'ansiwrap': '0.7.0',
                       'packaging': '20.4',
                       'text-extensions-for-pandas':
                       "git+https://github.com/akchinSTC/"
                       "text-extensions-for-pandas@50d5a1688fb723b5dd8139761830d3419042fee5"
                       }
    correct_dict = {'ipykernel': '5.3.0',
                    'ansiwrap': '0.8.4',
                    'packaging': '20.4',
                    'text-extensions-for-pandas':
                    "git+https://github.com/akchinSTC/text-extensions-for-pandas@50d5a1688fb723b5dd8139761830d3419042fee5"  # noqa: E501
                    }

    mocked_func = mock.Mock(return_value="default", side_effect=[elyra_dict, to_install_dict])

    monkeypatch.setattr(bootstrapper.OpUtil, "package_list_to_dict", mocked_func)
    monkeypatch.setattr(sys, "executable", virtualenv.python)

    virtualenv.run("python3 -m pip install bleach==3.1.5")
    virtualenv.run("python3 -m pip install ansiwrap==0.7.0")
    virtualenv.run("python3 -m pip install packaging==20.4")
    virtualenv.run("python3 -m pip install git+https://github.com/akchinSTC/"
                   "text-extensions-for-pandas@50d5a1688fb723b5dd8139761830d3419042fee5")

    bootstrapper.OpUtil.package_install(user_volume_path=None)
    virtual_env_dict = {}
    output = virtualenv.run("python3 -m pip freeze", capture=True)
    for line in output.strip().split('\n'):
        if " @ " in line:
            package_name, package_version = line.strip('\n').split(sep=" @ ")
        elif "===" in line:
            package_name, package_version = line.strip('\n').split(sep="===")
        else:
            package_name, package_version = line.strip('\n').split(sep="==")
        virtual_env_dict[package_name] = package_version

    for package, version in correct_dict.items():
        assert virtual_env_dict[package] == version


def test_package_installation_with_target_path(monkeypatch, virtualenv):
    # TODO : Need to add test for direct-source e.g. ' @ '
    elyra_dict = {'ipykernel': '5.3.0',
                  'ansiwrap': '0.8.4',
                  'packaging': '20.0',
                  'text-extensions-for-pandas': '2.0.0'
                  }
    to_install_dict = {'bleach': '3.1.5',
                       'ansiwrap': '0.7.0',
                       'packaging': '20.4',
                       'text-extensions-for-pandas':
                       "git+https://github.com/akchinSTC/"
                       "text-extensions-for-pandas@50d5a1688fb723b5dd8139761830d3419042fee5"
                       }
    correct_dict = {'ipykernel': '5.3.0',
                    'ansiwrap': '0.8.4',
                    'packaging': '20.4',
                    'text-extensions-for-pandas':
                    "git+https://github.com/akchinSTC/text-extensions-for-pandas@50d5a1688fb723b5dd8139761830d3419042fee5"  # noqa: E501
                    }

    mocked_func = mock.Mock(return_value="default", side_effect=[elyra_dict, to_install_dict])

    monkeypatch.setattr(bootstrapper.OpUtil, "package_list_to_dict", mocked_func)
    monkeypatch.setattr(sys, "executable", virtualenv.python)

    virtualenv.run("python3 -m pip install --target='/tmp/lib/' bleach==3.1.5")
    virtualenv.run("python3 -m pip install --target='/tmp/lib/' ansiwrap==0.7.0")
    virtualenv.run("python3 -m pip install --target='/tmp/lib/' packaging==20.4")
    virtualenv.run("python3 -m pip install --target='/tmp/lib/' git+https://github.com/akchinSTC/"
                   "text-extensions-for-pandas@50d5a1688fb723b5dd8139761830d3419042fee5")

    bootstrapper.OpUtil.package_install(user_volume_path='/tmp/lib/')
    virtual_env_dict = {}
    output = virtualenv.run("python3 -m pip freeze --path=/tmp/lib/", capture=True)
    print("This is the output :" + output)
    for line in output.strip().split('\n'):
        if " @ " in line:
            package_name, package_version = line.strip('\n').split(sep=" @ ")
        elif "===" in line:
            package_name, package_version = line.strip('\n').split(sep="===")
        else:
            package_name, package_version = line.strip('\n').split(sep="==")
        virtual_env_dict[package_name] = package_version

    for package, version in correct_dict.items():
        assert virtual_env_dict[package] == version


def test_convert_notebook_to_html(tmpdir):
    notebook_file = os.getcwd() + "/etc/tests/resources/test-notebookA.ipynb"
    notebook_output_html_file = "test-notebookA.html"

    with tmpdir.as_cwd():
        bootstrapper.NotebookFileOp.convert_notebook_to_html(notebook_file, notebook_output_html_file)

        assert os.path.isfile(notebook_output_html_file)
        # Validate that an html file got generated from the notebook
        with open(notebook_output_html_file, 'r') as html_file:
            html_data = html_file.read()
            assert html_data.startswith("<!DOCTYPE html>")
            assert "&quot;TEST_ENV_VAR1&quot;" in html_data   # from os.getenv("TEST_ENV_VAR1")
            assert html_data.endswith("</html>\n")


def test_fail_convert_notebook_to_html(tmpdir):
    notebook_file = os.getcwd() + "/etc/tests/resources/test-bad-notebookA.ipynb"
    notebook_output_html_file = "bad-notebookA.html"
    with tmpdir.as_cwd():
        # Recent versions raising typeError due to #1130
        # https://github.com/jupyter/nbconvert/pull/1130
        with pytest.raises((TypeError, nbformat.validator.NotebookValidationError)):
            bootstrapper.NotebookFileOp.convert_notebook_to_html(notebook_file, notebook_output_html_file)


def test_get_file_object_store(monkeypatch, s3_setup, tmpdir):
    file_to_get = "README.md"
    current_directory = os.getcwd() + '/'
    bucket_name = "test-bucket"

    s3_setup.fput_object(bucket_name=bucket_name,
                         object_name=file_to_get,
                         file_path=file_to_get)

    with tmpdir.as_cwd():
        op = _get_operation_instance(monkeypatch, s3_setup)

        op.get_file_from_object_storage(file_to_get)
        assert os.path.isfile(file_to_get)
        assert hs.fileChecksum(file_to_get, "sha256") == hs.fileChecksum(current_directory + file_to_get, "sha256")


def test_fail_get_file_object_store(monkeypatch, s3_setup, tmpdir):
    file_to_get = "test-file.txt"

    with tmpdir.as_cwd():
        with pytest.raises(minio.error.NoSuchKey):
            op = _get_operation_instance(monkeypatch, s3_setup)
            op.get_file_from_object_storage(file_to_get=file_to_get)


def test_put_file_object_store(monkeypatch, s3_setup, tmpdir):
    bucket_name = "test-bucket"
    file_to_put = "LICENSE"
    current_directory = os.getcwd() + '/'

    op = _get_operation_instance(monkeypatch, s3_setup)
    op.put_file_to_object_storage(file_to_upload=file_to_put)

    with tmpdir.as_cwd():
        s3_setup.fget_object(bucket_name, file_to_put, file_to_put)
        assert os.path.isfile(file_to_put)
        assert hs.fileChecksum(file_to_put, "sha256") == hs.fileChecksum(current_directory + file_to_put, "sha256")


def test_fail_invalid_filename_put_file_object_store(monkeypatch, s3_setup):
    file_to_put = "LICENSE_NOT_HERE"

    with pytest.raises(FileNotFoundError):
        op = _get_operation_instance(monkeypatch, s3_setup)
        op.put_file_to_object_storage(file_to_upload=file_to_put)


def test_fail_bucket_put_file_object_store(monkeypatch, s3_setup):
    bucket_name = "test-bucket-not-exist"
    file_to_put = "LICENSE"

    with pytest.raises(minio.error.NoSuchBucket):
        op = _get_operation_instance(monkeypatch, s3_setup)
        monkeypatch.setattr(op, "cos_bucket", bucket_name)
        op.put_file_to_object_storage(file_to_upload=file_to_put)


def test_find_best_kernel_nb(tmpdir):
    source_nb_file = os.path.join(os.getcwd(), "etc/tests/resources/test-notebookA.ipynb")
    nb_file = os.path.join(tmpdir, "test-notebookA.ipynb")

    # "Copy" nb file to destination - this test does not update the kernel or language.
    nb = nbformat.read(source_nb_file, 4)
    nbformat.write(nb, nb_file)

    with tmpdir.as_cwd():
        kernel_name = bootstrapper.NotebookFileOp.find_best_kernel(nb_file)
        assert kernel_name == nb.metadata.kernelspec['name']


def test_find_best_kernel_lang(tmpdir, caplog):
    caplog.set_level(logging.INFO)
    source_nb_file = os.path.join(os.getcwd(), "etc/tests/resources/test-notebookA.ipynb")
    nb_file = os.path.join(tmpdir, "test-notebookA.ipynb")

    # "Copy" nb file to destination after updating the kernel name - forcing a language match
    nb = nbformat.read(source_nb_file, 4)
    nb.metadata.kernelspec['name'] = 'test-kernel'
    nb.metadata.kernelspec['language'] = 'PYTHON'  # test case-insensitivity
    nbformat.write(nb, nb_file)

    with tmpdir.as_cwd():
        kernel_name = bootstrapper.NotebookFileOp.find_best_kernel(nb_file)
        assert kernel_name == 'python3'
        assert len(caplog.records) == 1
        assert caplog.records[0].message.startswith("Matched kernel by language (PYTHON)")


def test_find_best_kernel_nomatch(tmpdir, caplog):
    source_nb_file = os.path.join(os.getcwd(), "etc/tests/resources/test-notebookA.ipynb")
    nb_file = os.path.join(tmpdir, "test-notebookA.ipynb")

    # "Copy" nb file to destination after updating the kernel name and language - forcing use of updated name
    nb = nbformat.read(source_nb_file, 4)
    nb.metadata.kernelspec['name'] = 'test-kernel'
    nb.metadata.kernelspec['language'] = 'test-language'
    nbformat.write(nb, nb_file)

    with tmpdir.as_cwd():
        kernel_name = bootstrapper.NotebookFileOp.find_best_kernel(nb_file)
        assert kernel_name == 'test-kernel'
        assert len(caplog.records) == 1
        assert caplog.records[0].message.startswith("Reverting back to missing notebook kernel 'test-kernel'")


def test_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-f', 'test-notebook.ipynb',
                 '-b', 'test-bucket',
                 '-p', '/tmp/lib']
    args_dict = bootstrapper.OpUtil.parse_arguments(test_args)

    assert args_dict['cos-endpoint'] == 'http://test.me.now'
    assert args_dict['cos-directory'] == 'test-directory'
    assert args_dict['cos-dependencies-archive'] == 'test-archive.tgz'
    assert args_dict['cos-bucket'] == 'test-bucket'
    assert args_dict['filepath'] == 'test-notebook.ipynb'
    assert args_dict['user-volume-path'] == '/tmp/lib'
    assert not args_dict['inputs']
    assert not args_dict['outputs']


def test_fail_missing_notebook_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.OpUtil.parse_arguments(test_args)


def test_fail_missing_endpoint_parse_arguments():
    test_args = ['-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-f', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.OpUtil.parse_arguments(test_args)


def test_fail_missing_archive_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-f', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.OpUtil.parse_arguments(test_args)


def test_fail_missing_bucket_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-f', 'test-notebook.ipynb']
    with pytest.raises(SystemExit):
        bootstrapper.OpUtil.parse_arguments(test_args)


def test_fail_missing_directory_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-t', 'test-archive.tgz',
                 '-f', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.OpUtil.parse_arguments(test_args)


@pytest.mark.skip(reason='leaving as informational - not sure worth checking if reqs change')
def test_requirements_file():
    requirements_file = "etc/tests/resources/test-requirements-elyra.txt"
    correct_number_of_packages = 18
    list_dict = bootstrapper.OpUtil.package_list_to_dict(requirements_file)
    assert len(list_dict) == correct_number_of_packages


def test_fail_requirements_file_bad_delimiter():
    bad_requirements_file = "etc/tests/resources/test-bad-requirements-elyra.txt"
    with pytest.raises(ValueError):
        bootstrapper.OpUtil.package_list_to_dict(bad_requirements_file)
