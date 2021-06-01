#!/opt/manager/env/bin/python
"""Clean dep directories, removing files that were leftover by dep-update.

Dep-update in Cloudify pre-5.2.2 leaves a copy of the blueprint in the
deployment directory. That is unnecessary. This script removes those
files.

Only remove files that are in the blueprint, so that unrelated files that
were added afterwards by the user are unaffected.

Note that this script will only do this dep/bp file comparison by
filename, and it won't traverse subdirectories.
"""
import os
import logging
import shutil

from manager_rest import config
from manager_rest.storage import models
from manager_rest.flask_utils import setup_flask_app

os.environ.setdefault(
    'MANAGER_REST_CONFIG_PATH', '/opt/manager/cloudify-rest.conf'
)


def find_blueprint_files(bp):
    bp_dir = find_blueprint_directory(bp)
    return set(os.listdir(bp_dir))


def find_blueprint_directory(bp):
    return os.path.join('/opt', 'manager', 'resources', 'blueprints',
                        bp.tenant.name, bp.id)


def find_deployment_workdir(dep):
    return os.path.join('/opt', 'manager', 'resources', 'deployments',
                        dep.tenant.name, dep.id)


def clean_deployment_dir(dep, blueprint_files):
    """Remove files in dep's dir that are also in bp's dir"""
    dep_dir = find_deployment_workdir(dep)
    if dep.blueprint_id not in blueprint_files:
        blueprint_files[dep.blueprint_id] = find_blueprint_files(dep.blueprint)
    for fn in os.listdir(dep_dir):
        if fn in blueprint_files[dep.blueprint_id]:
            to_remove = os.path.join(dep_dir, fn)
            if os.path.isdir(to_remove):
                shutil.rmtree(to_remove)
            else:
                os.unlink(to_remove)


def clean_deployment_dirs():
    blueprint_files = {}
    deps = (
        models.Deployment.query
        .join(models.Tenant)
        .join(models.Blueprint)
        .all()
    )
    logging.info('Found %s deployments', len(deps))
    succeeded_deps = []
    failed_deps = []

    for dep in deps:
        try:
            clean_deployment_dir(dep, blueprint_files)
        except Exception as e:
            logging.error('Could not clean deployment %s: %s', dep.id, e)
            failed_deps.append(dep)
        else:
            succeeded_deps.append(dep)

    logging.info('Cleaned %s deployments, failed %s deployments',
                 len(succeeded_deps), len(failed_deps))
    if failed_deps:
        logging.info('Failed deployments: %s',
                     ', '.join(dep.id for dep in failed_deps))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    config.instance.load_configuration()
    with setup_flask_app().app_context():
        clean_deployment_dirs()
