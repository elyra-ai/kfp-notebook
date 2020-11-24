#
# Copyright 2018-2020 Elyra Authors
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
import pytest
from kfp_notebook.pipeline import NotebookOp


@pytest.fixture
def notebook_op():
    return NotebookOp(name="test",
                      notebook="test_notebook.ipynb",
                      cos_endpoint="http://testserver:32525",
                      cos_bucket="test_bucket",
                      cos_directory="test_directory",
                      cos_dependencies_archive="test_archive.tgz",
                      image="test/image:dev")


def test_fail_without_cos_endpoint():
    with pytest.raises(TypeError):
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")


def test_fail_without_cos_bucket():
    with pytest.raises(TypeError):
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")


def test_fail_without_cos_directory():
    with pytest.raises(TypeError):
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")


def test_fail_without_cos_dependencies_archive():
    with pytest.raises(TypeError):
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   image="test/image:dev")


def test_fail_without_runtime_image():
    with pytest.raises(ValueError) as error_info:
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz")
    assert "You need to provide an image." == str(error_info.value)


def test_fail_without_notebook():
    with pytest.raises(TypeError):
        NotebookOp(name="test",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")


def test_fail_without_name():
    with pytest.raises(TypeError):
        NotebookOp(notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")


def test_fail_with_empty_string_as_name():
    with pytest.raises(ValueError):
        NotebookOp(name="",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")


def test_fail_with_empty_string_as_notebook():
    with pytest.raises(ValueError) as error_info:
        NotebookOp(name="test",
                   notebook="",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   image="test/image:dev")
    assert "You need to provide a notebook." == str(error_info.value)


def test_properly_set_notebook_name_when_in_subdirectory():
    notebook_op = NotebookOp(name="test",
                             notebook="foo/test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             image="test/image:dev")
    assert "test_notebook.ipynb" == notebook_op.notebook_name


def test_properly_set_python_script_name_when_in_subdirectory():
    notebook_op = NotebookOp(name="test",
                             notebook="foo/test.py",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             image="test/image:dev")
    assert "test.py" == notebook_op.notebook_name


def test_user_crio_volume_creation():
    notebook_op = NotebookOp(name="test",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             image="test/image:dev",
                             emptydir_volume_size='20Gi')
    assert notebook_op.emptydir_volume_size == '20Gi'
    assert notebook_op.container_work_dir_root_path == '/opt/app-root/src/'
    assert notebook_op.container.volume_mounts.__len__() == 1
    assert notebook_op.container.env.__len__() == 1


@pytest.mark.skip(reason="not sure if we should even test this")
def test_default_bootstrap_url(notebook_op):
    assert notebook_op.bootstrap_script_url == \
        'https://raw.githubusercontent.com/elyra-ai/kfp-notebook/v0.9.1/etc/docker-scripts/bootstrapper.py'


def test_override_bootstrap_url():
    notebook_op = NotebookOp(name="test",
                             bootstrap_script_url="https://test.server.com/bootscript.py",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             image="test/image:dev")
    assert notebook_op.bootstrap_script_url == "https://test.server.com/bootscript.py"


@pytest.mark.skip(reason="not sure if we should even test this")
def test_default_requirements_url(notebook_op):
    assert notebook_op.requirements_url == \
        'https://raw.githubusercontent.com/elyra-ai/kfp-notebook/v0.9.1/etc/requirements-elyra.txt'


def test_override_requirements_url():
    notebook_op = NotebookOp(name="test",
                             requirements_url="https://test.server.com/requirements.py",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             image="test/image:dev")
    assert notebook_op.requirements_url == "https://test.server.com/requirements.py"


def test_construct_with_both_pipeline_inputs_and_outputs():
    notebook_op = NotebookOp(name="test",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             pipeline_inputs=['test_input1.txt', 'test_input2.txt'],
                             pipeline_outputs=['test_output1.txt', 'test_output2.txt'],
                             image="test/image:dev")
    assert notebook_op.pipeline_inputs == ['test_input1.txt', 'test_input2.txt']
    assert notebook_op.pipeline_outputs == ['test_output1.txt', 'test_output2.txt']

    assert '--inputs "test_input1.txt;test_input2.txt"' in notebook_op.container.args[0]
    assert '--outputs "test_output1.txt;test_output2.txt"' in notebook_op.container.args[0]


def test_construct_wiildcard_outputs():
    notebook_op = NotebookOp(name="test",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             pipeline_inputs=['test_input1.txt', 'test_input2.txt'],
                             pipeline_outputs=['test_out*', 'foo.tar'],
                             image="test/image:dev")
    assert notebook_op.pipeline_inputs == ['test_input1.txt', 'test_input2.txt']
    assert notebook_op.pipeline_outputs == ['test_out*', 'foo.tar']

    assert '--inputs "test_input1.txt;test_input2.txt"' in notebook_op.container.args[0]
    assert '--outputs "test_out*;foo.tar"' in notebook_op.container.args[0]


def test_construct_with_only_pipeline_inputs():
    notebook_op = NotebookOp(name="test",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             pipeline_inputs=['test_input1.txt', 'test,input2.txt'],
                             pipeline_outputs=[],
                             image="test/image:dev")
    assert notebook_op.pipeline_inputs == ['test_input1.txt', 'test,input2.txt']
    assert '--inputs "test_input1.txt;test,input2.txt"' in notebook_op.container.args[0]


def test_construct_with_bad_pipeline_inputs():
    with pytest.raises(ValueError) as error_info:
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   pipeline_inputs=['test_input1.txt', 'test;input2.txt'],
                   pipeline_outputs=[],
                   image="test/image:dev")
    assert "Illegal character (;) found in filename 'test;input2.txt'." == str(error_info.value)


def test_construct_with_only_pipeline_outputs():
    notebook_op = NotebookOp(name="test",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             pipeline_outputs=['test_output1.txt', 'test,output2.txt'],
                             pipeline_envs={},
                             image="test/image:dev")
    assert notebook_op.pipeline_outputs == ['test_output1.txt', 'test,output2.txt']
    assert '--outputs "test_output1.txt;test,output2.txt"' in notebook_op.container.args[0]


def test_construct_with_bad_pipeline_outputs():
    with pytest.raises(ValueError) as error_info:
        NotebookOp(name="test",
                   notebook="test_notebook.ipynb",
                   cos_endpoint="http://testserver:32525",
                   cos_bucket="test_bucket",
                   cos_directory="test_directory",
                   cos_dependencies_archive="test_archive.tgz",
                   pipeline_outputs=['test_output1.txt', 'test;output2.txt'],
                   image="test/image:dev")
    assert "Illegal character (;) found in filename 'test;output2.txt'." == str(error_info.value)


def test_construct_with_env_variables():
    notebook_op = NotebookOp(name="test",
                             notebook="test_notebook.ipynb",
                             cos_endpoint="http://testserver:32525",
                             cos_bucket="test_bucket",
                             cos_directory="test_directory",
                             cos_dependencies_archive="test_archive.tgz",
                             pipeline_envs={"ENV_VAR_ONE": "1", "ENV_VAR_TWO": "2", "ENV_VAR_THREE": "3"},
                             image="test/image:dev")

    confirmation_names = ["ENV_VAR_ONE", "ENV_VAR_TWO", "ENV_VAR_THREE"]
    confirmation_values = ["1", "2", "3"]
    for env_val in notebook_op.container.env:
        assert env_val.name in confirmation_names
        assert env_val.value in confirmation_values
        confirmation_names.remove(env_val.name)
        confirmation_values.remove(env_val.value)

    # Verify confirmation values have been drained.
    assert len(confirmation_names) == 0
    assert len(confirmation_values) == 0
