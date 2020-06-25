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
import subprocess
import nbformat
import papermill
import pytest
import mock
import sys
import minio
import os

sys.path.append('etc/docker-scripts/')
import bootstrapper


@pytest.fixture
def s3_setup(request):
    bucket_name = "test-bucket"
    cos_client = minio.Minio("127.0.0.1:9000",
                             access_key="minioadmin",
                             secret_key="minioadmin",
                             secure=False)
    cos_client.make_bucket(bucket_name)

    def cleanup():
        cleanup_files = cos_client.list_objects(bucket_name)
        for file in cleanup_files:
            cos_client.remove_object(bucket_name, file.object_name)
        cos_client.remove_bucket(bucket_name)

    request.addfinalizer(cleanup)

    return cos_client


def test_main_method(monkeypatch, s3_setup, tmpdir):
    argument_dict = {'cos-endpoint': 'http://127.0.0.1:9000',
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'notebook': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt',
                     'outputs': 'test-file.txt'}
    bucket_name = "test-bucket"
    monkeypatch.setattr(bootstrapper, "parse_arguments", lambda x: argument_dict)
    monkeypatch.setattr(bootstrapper, "package_install", lambda: True)
    mocked_func = mock.Mock(return_value="default", side_effect=['test-archive.tgz',
                                                                 'test-file.txt',
                                                                 'test-notebookA-output.ipynb',
                                                                 'test-notebookA.html',
                                                                 'test-file.txt'])
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", mocked_func)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3_setup.fput_object(bucket_name=bucket_name,
                         object_name="test-file.txt",
                         file_path="README.md")
    s3_setup.fput_object(bucket_name=bucket_name,
                         object_name="test-archive.tgz",
                         file_path="etc/tests/resources/test-archive.tgz")

    with tmpdir.as_cwd():
        bootstrapper.main()


def test_fail_bad_endpoint_main_method(monkeypatch, tmpdir):
    argument_dict = {'cos-endpoint': '127.0.0.1:9000',
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-archive.tgz',
                     'notebook': 'etc/tests/resources/test-notebookA.ipynb',
                     'inputs': 'test-file.txt',
                     'outputs': 'test-file.txt'}
    monkeypatch.setattr(bootstrapper, "parse_arguments", lambda x: argument_dict)
    monkeypatch.setattr(bootstrapper, "package_install", lambda: True)

    mocked_func = mock.Mock(return_value="default", side_effect=['test-archive.tgz',
                                                                 'test-file.txt',
                                                                 'test-notebookA-output.ipynb',
                                                                 'test-notebookA.html',
                                                                 'test-file.txt'])
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", mocked_func)

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    with tmpdir.as_cwd():
        with pytest.raises(minio.error.InvalidEndpointError):
            bootstrapper.main()


def test_fail_bad_notebook_main_method(monkeypatch, s3_setup, tmpdir):
    argument_dict = {'cos-endpoint': 'http://127.0.0.1:9000',
                     'cos-bucket': 'test-bucket',
                     'cos-directory': 'test-directory',
                     'cos-dependencies-archive': 'test-bad-archiveB.tgz',
                     'notebook': 'etc/tests/resources/test-bad-notebookB.ipynb',
                     'inputs': 'test-file.txt',
                     'outputs': 'test-file.txt'}
    bucket_name = "test-bucket"

    monkeypatch.setattr(bootstrapper, "parse_arguments", lambda x: argument_dict)
    monkeypatch.setattr(bootstrapper, "package_install", lambda: True)

    mocked_func = mock.Mock(return_value="default", side_effect=['test-bad-archiveB.tgz',
                                                                 'test-file.txt',
                                                                 'test-bad-notebookB-output.ipynb',
                                                                 'test-bad-notebookB.html',
                                                                 'test-file.txt'])
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", mocked_func)

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "minioadmin")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    s3_setup.fput_object(bucket_name=bucket_name,
                         object_name="test-file.txt",
                         file_path="README.md")
    s3_setup.fput_object(bucket_name=bucket_name,
                         object_name="test-bad-archiveB.tgz",
                         file_path="etc/tests/resources/test-bad-archiveB.tgz")

    with tmpdir.as_cwd():
        with pytest.raises(papermill.exceptions.PapermillExecutionError):
            bootstrapper.main()


def test_package_installation(monkeypatch):
    elyra_dict = {'ipykernel': '5.3.0',
                  'ansiwrap': '0.8.5',
                  'package': '1.0.0'}
    current_dict = {'bleach': '3.1.5',
                    'ansiwrap': '0.8.4',
                    'package': '1.0.2',
                    'text-extensions-for-pandas':
                        "git+https://github.com/test/text-extensions-for-pandas@a0dcb9196c6de6a2f58194cc49e48a25a18d099d"}
    mocked_func = mock.Mock(return_value="default", side_effect=[elyra_dict, current_dict])

    monkeypatch.setattr(bootstrapper, "package_list_to_dict", mocked_func)
    monkeypatch.setattr(subprocess, "check_call", lambda x: True)

    bootstrapper.package_install()


def test_convert_notebook_to_html(tmpdir):
    notebook_file = os.getcwd() + "/etc/tests/resources/test-notebookA.ipynb"
    notebook_output_html_file = "notebookA.html"
    with tmpdir.as_cwd():
        bootstrapper.convert_notebook_to_html(notebook_file, notebook_output_html_file)


def test_fail_convert_notebook_to_html(tmpdir):
    notebook_file = os.getcwd() + "/etc/tests/resources/test-bad-notebookA.ipynb"
    notebook_output_html_file = "bad-notebookA.html"
    with tmpdir.as_cwd():
        with pytest.raises(nbformat.validator.NotebookValidationError):
            bootstrapper.convert_notebook_to_html(notebook_file, notebook_output_html_file)


def test_get_file_object_store(monkeypatch, s3_setup, tmpdir):
    bucket_name = "test-bucket"
    file_to_get = "test-file.txt"
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", lambda x: file_to_get)

    s3_setup.fput_object(bucket_name=bucket_name,
                         object_name="test-file.txt",
                         file_path="README.md")

    with tmpdir.as_cwd():
        bootstrapper.get_file_from_object_storage(s3_setup, bucket_name, file_to_get)


def test_fail_get_file_object_store(monkeypatch, s3_setup, tmpdir):
    bucket_name = "test-bucket"
    file_to_get = "test-file.txt"
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", lambda x: file_to_get)

    with tmpdir.as_cwd():
        with pytest.raises(minio.error.NoSuchKey):
            bootstrapper.get_file_from_object_storage(s3_setup, bucket_name, file_to_get)


def test_put_file_object_store(monkeypatch, s3_setup, tmpdir):
    bucket_name = "test-bucket"
    file_to_put = "LICENSE"
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", lambda x: file_to_put)

    bootstrapper.put_file_to_object_storage(s3_setup, bucket_name, file_to_put)

    with tmpdir.as_cwd():
        s3_setup.get_object(bucket_name, file_to_put)


def test_fail_invalid_filename_put_file_object_store(monkeypatch, s3_setup):
    bucket_name = "test-bucket"
    file_to_put = "LICENSE_NOT_HERE"
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", lambda x: file_to_put)

    with pytest.raises(FileNotFoundError):
        bootstrapper.put_file_to_object_storage(s3_setup, bucket_name, file_to_put)


def test_fail_bucket_put_file_object_store(monkeypatch, s3_setup):
    bucket_name = "test-bucket-not-exist"
    file_to_put = "LICENSE"
    monkeypatch.setattr(bootstrapper, "get_object_storage_filename", lambda x: file_to_put)

    with pytest.raises(minio.error.NoSuchBucket):
        bootstrapper.put_file_to_object_storage(s3_setup, bucket_name, file_to_put)


def test_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-i', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    bootstrapper.parse_arguments(test_args)


def test_fail_missing_notebook_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.parse_arguments(test_args)


def test_fail_missing_endpoint_parse_arguments():
    test_args = ['-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-i', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.parse_arguments(test_args)


def test_fail_missing_archive_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-i', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.parse_arguments(test_args)


def test_fail_missing_bucket_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-d', 'test-directory',
                 '-t', 'test-archive.tgz',
                 '-i', 'test-notebook.ipynb']
    with pytest.raises(SystemExit):
        bootstrapper.parse_arguments(test_args)


def test_fail_missing_directory_parse_arguments():
    test_args = ['-e', 'http://test.me.now',
                 '-t', 'test-archive.tgz',
                 '-i', 'test-notebook.ipynb',
                 '-b', 'test-bucket']
    with pytest.raises(SystemExit):
        bootstrapper.parse_arguments(test_args)


def test_requirements_file():
    requirements_file = "etc/tests/resources/test-requirements-elyra.txt"
    correct_number_of_packages = 18
    list_dict = bootstrapper.package_list_to_dict(requirements_file)
    assert len(list_dict) == correct_number_of_packages


def test_fail_requirements_file_bad_delimiter():
    bad_requirements_file = "etc/tests/resources/test-bad-requirements-elyra.txt"
    with pytest.raises(ValueError):
        list_dict = bootstrapper.package_list_to_dict(bad_requirements_file)
