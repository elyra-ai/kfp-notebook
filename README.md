<!--
{% comment %}
Copyright 2018-2019 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

KFP-Notebook is an Notebook op to enable running notebooks as part of a Kubeflow Pipeline.
 

## Building kfp-notebook

```bash
make clean install
```

## Usage

The example below can easily be added to a `python script` or `jupyter notebook` for testing purposes.

```python
import os
import kfp
from notebook.pipeline._notebook_op import NotebookOp
from kubernetes.client.models import V1EnvVar, V1SecretKeySelector

url = 'http://weakish1.fyre.ibm.com:32488/pipeline'

# configures artifact location
notebook_location = kfp.dsl.ArtifactLocation.s3(
        bucket="oscon",
        endpoint="weakish1.fyre.ibm.com:30427",
        insecure=True,
        access_key_secret=V1SecretKeySelector(name="mlpipeline-minio-artifact", key="accesskey"),
        secret_key_secret=V1SecretKeySelector(name="mlpipeline-minio-artifact", key="secretkey"))

def run_notebook_op(op_name, notebook_path):    
    op= NotebookOp(
        name=op_name,
        notebook=notebook_path,
        cos_endpoint='http://weakish1.fyre.ibm.com:30427',
        cos_user='minio',
        cos_password='minio123',
        image='lresende/notebook-kubeflow-pipeline:dev',
        artifact_location=notebook_location,
    )
    op.container.set_image_pull_policy('Always')
    
    return op
    
def demo_pipeline():
    stats_op = run_notebook_op('stats', 'generate-community-overview')
    contributions_op = run_notebook_op('contributions', 'generate-community-contributions')
    run_notebook_op('overview', 'overview').after(stats_op, contributions_op)
    
# Compile the new pipeline
kfp.compiler.Compiler().compile(demo_pipeline,'pipelines/oscon_pipeline.tar.gz')

# Upload the compiled pipeline
client = kfp.Client(host=url)
client.upload_pipeline('pipelines/oscon_pipeline.tar.gz',pipeline_name='oscon-pipeline')
#experiment = client.create_experiment(name='oscon-community-stats')
#run = client.run_pipeline(experiment.id, 'oscon-community-stats', 'pipelines/community_pipeline.tar.gz')

```

## Generated Kubeflow Pipelines

![Kubeflow Pipeline Example](docs/source/images/kfp-pipeline-example.png)

## Building and publishing a new release to IBM internal PyPi repository
Publishing a package requires the "twine" python package.

You can run: 
``` 
twine upload --repository-url https://na.artifactory.swg-devops.com/artifactory/api/pypi/dbg-aiworkspace-team-pypi-local dist/* 
``` 
and enter your IBM credentials to upload to a new release

```
$ twine upload -r local dist/* --verbose
Uploading distributions to https://na.artifactory.swg-devops.com/artifactory/api/pypi/dbg-aiworkspace-team-pypi-local
Uploading kfp_notebook-<version>-py3-none-any.whl
100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 15.2k/15.2k [00:01<00:00, 11.0kB/s]
```

Alternatively, if you want to be able to run without user input :

Edit your ~/.pypirc file to include a "local" repository to push to: 
```
[distutils]
index-servers =
        local
        pypi
        
[local]
repository: https://na.artifactory.swg-devops.com/artifactory/api/pypi/dbg-aiworkspace-team-pypi-local
username : YOURNAME@us.ibm.com
password: YOUR_ARTIFACTORY_API_TOKEN  ### This is opt. only if you don't want to enter your IBM password at publish time

[pypi]
repository: https://pypi.python.org/pypi
```
Run the following to publish the package: 
```
twine upload -r local dist/* --verbose
```

