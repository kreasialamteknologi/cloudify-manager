########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import time
import glob
import os
import shutil
import errno
import operator
import networkx as nx
from contextlib import contextmanager
from datetime import datetime
from functools import total_ordering


from retrying import retry

from cloudify.decorators import workflow
from cloudify.manager import get_rest_client
from cloudify.workflows import workflow_context

from cloudify.utils import parse_utc_datetime_relative
from cloudify_rest_client.client import CloudifyClient
from cloudify_rest_client.exceptions import CloudifyClientError
from dsl_parser import constants as dsl
from dsl_parser import tasks

from .update_handlers import DeploymentUpdateDeploymentHandler

def _parse_plan_datetime(time_expression, base_datetime):
    """
    :param time_expression: Either a string representing an absolute
        datetime, or a relative time delta, such as '+4 hours' or '+1y+1d'.
    :param base_datetime: a datetime object representing the absolute date
        and time to which we apply the time delta.
    :return: A naive datetime object, in UTC time.
    """
    time_fmt = '%Y-%m-%d %H:%M:%S'
    if time_expression.startswith('+'):
        return parse_utc_datetime_relative(time_expression, base_datetime)
    return datetime.strptime(time_expression, time_fmt)


def _create_schedules(client, deployment_id, schedules):
    base_time = datetime.utcnow()
    for name, spec in schedules.items():
        workflow_id = spec.pop('workflow')
        if 'since' in spec:
            spec['since'] = _parse_plan_datetime(spec['since'], base_time)
        if 'until' in spec:
            spec['until'] = _parse_plan_datetime(spec['until'], base_time)
        if 'workflow_parameters' in spec:
            spec['parameters'] = spec.pop('workflow_parameters')
        client.execution_schedules.create(
            name,
            deployment_id=deployment_id,
            workflow_id=workflow_id,
            **spec
        )


def _join_groups(client, deployment_id, groups):
    for group_name in groups:
        try:
            client.deployment_groups.add_deployments(
                group_name, deployment_ids=[deployment_id])
        except CloudifyClientError as e:
            if e.status_code != 404:
                raise
            client.deployment_groups.put(
                group_name, deployment_ids=[deployment_id])


def _get_deployment_labels(new_labels, plan_labels):
    labels = {tuple(label) for label in new_labels}
    for name, label_spec in plan_labels.items():
        labels |= {(name.lower(), value.lower()) for value in
                   label_spec.get('values', [])}
    return [{k: v} for k, v in labels]


@workflow
def create(ctx, labels=None, inputs=None, skip_plugins_validation=False, **_):
    client = get_rest_client(tenant=ctx.tenant_name)
    t = time.time()
    bp = client.blueprints.get(ctx.blueprint.id)
    with open('/tmp/foo.log', 'a') as f:
        f.write(f'getblu {time.time() - t}\n')
    t = time.time()
    deployment_plan = tasks.prepare_deployment_plan(
        bp.plan, client.secrets.get, inputs,
        runtime_only_evaluation=ctx.deployment.runtime_only_evaluation)
    with open('/tmp/foo.log', 'a') as f:
        f.write(f'parse {time.time() - t}\n')
    nodes = deployment_plan['nodes']
    node_instances = deployment_plan['node_instances']
    t = time.time()
    ctx.logger.info('Creating %d nodes', len(nodes))
    client.nodes.create_many(ctx.deployment.id, nodes)
    ctx.logger.info('Creating %d node-instances', len(node_instances))
    client.node_instances.create_many(ctx.deployment.id, node_instances)
    with open('/tmp/foo.log', 'a') as f:
        f.write(f'node-instances {time.time() - t}\n')
    labels_to_create = _get_deployment_labels(
        labels or [],
        deployment_plan.get('labels', {}))
    t = time.time()
    ctx.logger.info('Setting deployment attributes')
    client.deployments.set_attributes(
        ctx.deployment.id,
        description=deployment_plan['description'],
        workflows=deployment_plan['workflows'],
        inputs=deployment_plan['inputs'],
        policy_types=deployment_plan['policy_types'],
        policy_triggers=deployment_plan['policy_triggers'],
        groups=deployment_plan['groups'],
        scaling_groups=deployment_plan['scaling_groups'],
        outputs=deployment_plan['outputs'],
        capabilities=deployment_plan.get('capabilities', {}),
        labels=labels_to_create,
    )
    with open('/tmp/foo.log', 'a') as f:
        f.write(f'set-attr {time.time() - t}\n')
    t = time.time()
    deployment_settings = deployment_plan.get('deployment_settings', {})
    _join_groups(client, ctx.deployment.id,
                 deployment_settings.get('default_groups', []))
    _create_schedules(client, ctx.deployment.id,
                      deployment_settings.get('default_schedules', {}))

    ctx.logger.info('Creating deployment work directory')
    _create_deployment_workdir(
        deployment_id=ctx.deployment.id,
        tenant=ctx.tenant_name,
        logger=ctx.logger)

    new_dependencies = deployment_plan.setdefault(
        dsl.INTER_DEPLOYMENT_FUNCTIONS, {})
    if new_dependencies:
        ctx.logger.info('Creating inter-deployment dependencies')
        manager_ips = [manager.private_ip
                       for manager in client.manager.get_managers()]
        ext_client, client_config, ext_deployment_id = \
            _get_external_clients(nodes, manager_ips)
        for func_id, deployment_id_func in new_dependencies.items():
            deployment_id, deployment_func = deployment_id_func
            if ext_client:
                client.inter_deployment_dependencies.create(
                    dependency_creator=func_id,
                    source_deployment=ctx.deployment.id,
                    target_deployment=None,
                    external_target={
                        'deployment': (ext_deployment_id if
                                       ext_deployment_id else None),
                        'client_config': client_config
                    },
                )
                ext_client.inter_deployment_dependencies.create(
                    dependency_creator=func_id,
                    source_deployment=ctx.deployment.id,
                    target_deployment=deployment_id if deployment_id else ' ',
                    external_source={
                        'deployment': ctx.deployment.id,
                        'tenant': ctx.deployment.tenant_name,
                        'host': manager_ips,
                    },
                )
            else:
                # It should be safe to assume that if the target_deployment
                # is known, there's no point passing target_deployment_func.
                # Also because in this case the target_deployment_func will
                # be of type string, while REST endpoint expects a dict.
                client.inter_deployment_dependencies.create(
                    dependency_creator=func_id,
                    source_deployment=ctx.deployment.id,
                    target_deployment=deployment_id,
                    target_deployment_func=(deployment_func
                                            if not deployment_id else None),
                )
    with open('/tmp/foo.log', 'a') as f:
        f.write(f'rest {time.time() - t}\n')


@workflow
def delete(ctx, delete_logs, **_):
    ctx.logger.info('Deleting deployment environment: %s', ctx.deployment.id)
    _delete_deployment_workdir(ctx)
    if delete_logs:
        ctx.logger.info("Deleting management workers' logs for deployment %s",
                        ctx.deployment.id)
        _delete_logs(ctx)


def _delete_logs(ctx):
    log_dir = os.environ.get('AGENT_LOG_DIR')
    if log_dir:
        log_file_path = os.path.join(log_dir, 'logs',
                                     '{0}.log'.format(ctx.deployment.id))
        if os.path.exists(log_file_path):
            try:
                with open(log_file_path, 'w') as f:
                    # Truncating instead of deleting because the logging
                    # server currently holds a file descriptor open to this
                    # file. If we delete the file, the logs for new
                    # deployments that get created with the same deployment
                    # id, will get written to a stale file descriptor and
                    # will essentially be lost.
                    f.truncate()
            except IOError:
                ctx.logger.warn(
                    'Failed truncating {0}.'.format(log_file_path),
                    exc_info=True)
        for rotated_log_file_path in glob.glob('{0}.*'.format(
                log_file_path)):
            try:
                os.remove(rotated_log_file_path)
            except IOError:
                ctx.logger.exception(
                    'Failed removing rotated log file {0}.'.format(
                        rotated_log_file_path), exc_info=True)


def _retry_if_file_already_exists(exception):
    """Retry if file already exist exception raised."""
    return (
        isinstance(exception, OSError) and
        exception.errno == errno.EEXIST
    )


@workflow_context.task_config(send_task_events=False)
@retry(retry_on_exception=_retry_if_file_already_exists,
       stop_max_delay=60000,
       wait_fixed=2000)
def _create_deployment_workdir(deployment_id, logger, tenant):
    deployment_workdir = _workdir(deployment_id, tenant)
    if os.path.exists(deployment_workdir):
        # Otherwise we experience pain on snapshot restore
        return
    os.makedirs(deployment_workdir)


def _delete_deployment_workdir(ctx):
    deployment_workdir = _workdir(ctx.deployment.id, ctx.tenant_name)
    if not os.path.exists(deployment_workdir):
        return
    try:
        shutil.rmtree(deployment_workdir)
    except os.error:
        ctx.logger.warning(
            'Failed deleting directory %s. Current directory content: %s',
            deployment_workdir, os.listdir(deployment_workdir), exc_info=True)


def _workdir(deployment_id, tenant):
    return os.path.join('/opt', 'manager', 'resources', 'deployments',
                        tenant, deployment_id)


def _get_external_clients(nodes: list, manager_ips: list):
    client_config = None
    target_deployment = None
    for node in nodes:
        if node['type'] in ['cloudify.nodes.Component',
                            'cloudify.nodes.SharedResource']:
            client_config = node['properties'].get('client')
            target_deployment = node['properties'].get(
                'resource_config').get('deployment')
            break
    external_client = None
    if client_config:
        internal_hosts = ({'127.0.0.1', 'localhost'} | set(manager_ips))
        host = client_config['host']
        host = {host} if type(host) == str else set(host)
        if not (host & internal_hosts):
            external_client = CloudifyClient(**client_config)

    return external_client, client_config, \
        target_deployment.get('id') if target_deployment else None


@workflow
def update(ctx, deployment_update_id: str, preview: bool):
    client = get_rest_client(tenant=ctx.tenant_name)
    dep_up = client.deployment_updates.get(deployment_update_id)
    dep = client.deployments.get(dep_up.deployment_id)
    dep['workflows'] = {wf.name: wf for wf in dep['workflows']}

    bp = client.blueprints.get(dep_up.new_blueprint_id)
    new_deployment_plan = tasks.prepare_deployment_plan(
        bp.plan, client.secrets.get, dep_up.new_inputs,
        runtime_only_evaluation=dep_up['runtime_only_evaluation'])
    steps = list(extract_steps(client, dep, new_deployment_plan))

    with open('/tmp/foo.log', 'a') as f:
        f.write(f'steps {steps}\n')
        f.write(f'outputs {dep.outputs}\n')
    dep_handler = DeploymentUpdateDeploymentHandler(sm)


RELEVANT_DEPLOYMENT_FIELDS = ['blueprint_id', 'id', 'inputs', 'nodes',
                              'outputs', 'workflows', 'groups', 'policy_types',
                              'policy_triggers', 'description',
                              'deployment_plugins_to_install',
                              'workflow_plugins_to_install']

DEFAULT_TOPOLOGY_LEVEL = 0
NODE = 'node'
NODES = 'nodes'
OUTPUT = 'output'
OUTPUTS = 'outputs'
TYPE = 'type'
HOST_ID = 'host_id'
OPERATION = 'operation'
OPERATIONS = 'operations'
RELATIONSHIP = 'relationship'
RELATIONSHIPS = 'relationships'
TARGET_ID = 'target_id'
SOURCE_OPERATIONS = 'source_operations'
TARGET_OPERATIONS = 'target_operations'
PROPERTY = 'property'
PROPERTIES = 'properties'
WORKFLOW = 'workflow'
WORKFLOWS = 'workflows'
GROUP = 'group'
GROUPS = 'groups'
POLICY_TYPE = 'policy_type'
POLICY_TYPES = 'policy_types'
POLICY_TRIGGER = 'policy_trigger'
POLICY_TRIGGERS = 'policy_triggers'
DEPLOYMENT_PLUGINS_TO_INSTALL = 'deployment_plugins_to_install'
PLUGIN = 'plugin'
PLUGINS = 'plugins'
PLUGINS_TO_INSTALL = 'plugins_to_install'
DESCRIPTION = 'description'
CONTAINED_IN_RELATIONSHIP_TYPE = 'cloudify.relationships.contained_in'
TYPE_HIERARCHY = 'type_hierarchy'


class EntityIdBuilder(list):

    _separator = ':'

    @property
    def entity_id(self):
        return str(self)

    @contextmanager
    def extend_id(self, key):
        self.append(key)
        yield
        self.remove_last()

    @contextmanager
    def prepend_id_last_element(self, key):
        last_elem = self[-1]
        self.remove_last()
        self.append(key)
        self.append(last_elem)
        yield
        self.remove_last()
        self.remove_last()
        self.append(last_elem)

    def remove_last(self):
        del self[-1]

    def __str__(self):
        return self._separator.join(self)


class DeploymentPlan(dict):

    def __init__(self,
                 deployment,
                 nodes,
                 deployment_plugins_to_install,
                 workflow_plugins_to_install):
        """ Instantiate a deployment plan object

        This constructor gets a deployment and nodes in a format as if these
        were loaded from storage. Then the non deployment-update-relevant
        fields are filtered from the deployment, and the nodes are transformed
        from a list to a dict, were the keys are the ids of each node.
        """
        super(DeploymentPlan, self).__init__()
        filtered_deployment = {k: v
                               for k, v in deployment.items()
                               if k in RELEVANT_DEPLOYMENT_FIELDS}
        self.update(filtered_deployment)

        # update the plan with deployment_plugins_to_install and with
        # workflow_plugins_to_install
        deployment_plugins_to_install_by_name = self._transform_plugins(
            deployment_plugins_to_install)
        self.update({
            'deployment_plugins_to_install':
                deployment_plugins_to_install_by_name
        })
        workflow_plugins_to_install_by_name = self._transform_plugins(
            workflow_plugins_to_install)
        self.update({
            'workflow_plugins_to_install':
                workflow_plugins_to_install_by_name
        })
        self['nodes'] = nodes

    @classmethod
    def from_storage(cls, deployment_id, deployment_update):
        """ Create a DeploymentPlan from a stored deployment"""
        sm = get_storage_manager()
        if deployment_update:
            # get deployment and blueprint from deployment update
            deployment = deployment_update.deployment
            blueprint_plan = deployment_update.old_blueprint.plan
        else:
            # get deployment from storage
            deployment = sm.get(models.Deployment, deployment_id)
            blueprint_plan = deployment.blueprint.plan
        deployment_plugins_to_install = \
            blueprint_plan['deployment_plugins_to_install']
        workflow_plugins_to_install = \
            blueprint_plan['workflow_plugins_to_install']

        # get the nodes from the storage
        nodes = sm.list(models.Node,
                        filters={'deployment_id': [deployment_id]},
                        get_all_results=True)
        nodes = {node.id: node.to_dict() for node in nodes}
        return cls(deployment.to_dict(),
                   nodes,
                   deployment_plugins_to_install,
                   workflow_plugins_to_install)

    @classmethod
    def from_deployment_update(cls, deployment_update):
        """ Create a DeploymentPlan from a DeploymentUpdate object

        Using the processed blueprint plan (informally referred to
        as a 'deployment plan' in the resource_manager module), this method
        creates a deployment a nodes in their stored format, to pass to the
        constructor.
        """
        rm = manager_rest.resource_manager.get_resource_manager()
        deployment_plan = deployment_update.deployment_plan
        deployment_id = deployment_update.deployment_id
        new_deployment = rm.prepare_deployment_for_storage(deployment_id,
                                                           deployment_plan)
        dep_dict = new_deployment.to_dict(suppress_error=True)
        dep_dict['blueprint_id'] = deployment_update.deployment.blueprint.id
        deployment_plugins_to_install = \
            deployment_plan['deployment_plugins_to_install']
        workflow_plugins_to_install = \
            deployment_plan['workflow_plugins_to_install']
        nodes = rm.prepare_deployment_nodes_for_storage(deployment_plan)
        nodes_dict = dict()
        for node in nodes:
            node_dict = node.to_dict(suppress_error=True)
            node_dict['deployment_id'] = deployment_id
            nodes_dict[node.id] = node_dict
        return cls(dep_dict,
                   nodes_dict,
                   deployment_plugins_to_install,
                   workflow_plugins_to_install)

    @staticmethod
    def _transform_plugins(plugins):
        plugins_by_name = {plugin['name']: plugin for plugin in plugins}
        return plugins_by_name


@total_ordering
class DeploymentUpdateStep(object):
    def __init__(self,
                 action,
                 entity_type,
                 entity_id,
                 supported=True,
                 topology_order=DEFAULT_TOPOLOGY_LEVEL):
        self.action = action
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.supported = supported
        self.topology_order = topology_order

    @property
    def entity_name(self):
        return self.entity_id.split(':')[-1]

    def __hash__(self):
        return hash(self.entity_id)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        else:
            return False

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return self.__str__()

    def __lt__(self, other):
        """Is this step considered "smaller" than the other step?

        This is used for sorting the steps, ie. steps that are smaller
        come earlier, and will be executed first.
        """
        if self.action != other.action:
            # the order is 'remove' < 'add' < 'modify'
            actions = ['remove', 'add', 'modify']
            return actions.index(self.action) < actions.index(other.action)
        if self.action == 'add':
            if self.entity_type == NODE:
                if other.entity_type == RELATIONSHIP:
                    # add node before adding relationships
                    return True
                if other.entity_type == NODE:
                    # higher topology order before lower topology order
                    return self.topology_order > other.topology_order
        if self.action == 'remove':
            # remove relationships before removing nodes
            if self.entity_type == RELATIONSHIP and other.entity_type == NODE:
                return True
        return False


class StepExtractor(object):

    entity_id_segment_to_entity_type = {
        PROPERTIES: PROPERTY,
        OUTPUTS: OUTPUT,
        WORKFLOWS: WORKFLOW,
        GROUPS: GROUP,
        POLICY_TYPES: POLICY_TYPE,
        POLICY_TRIGGERS: POLICY_TRIGGER
    }

    def __init__(self, deployment_update):
        self.deployment_id = deployment_update.deployment_id
        self.deployment_update_id = deployment_update.id
        # self.deployment_update_manager = \
        #     manager_rest.deployment_update.manager.\
        #     get_deployment_updates_manager(deployment_update.preview)
        # self.sm = self.deployment_update_manager.sm
        self.entity_id_builder = EntityIdBuilder()
        self.old_deployment_plan = None
        self.new_deployment_plan = None
        self.steps = []

        # The following property determines the type of steps that will be
        # created as part of a call to self._extract_steps.

        # If it is False, the diff process (which is composed of two calls to
        # self.extract_steps) will detect entities that were not in
        # `old_deployment_plan` but exist in `new_deployment_plan`.
        # That is, the diff will detect all the entities that were added as
        # part of the deployment update process.
        # Furthermore, if the diff perspective is not inversed, the diff will
        # also detect all the entities that were modified. That is, entities
        # that exist both in `old_deployment_plan` and in
        # `new_deployment_plan`, but that their values are different.

        # If _inverted_diff_perspective is True, the diff process will detect
        # all entities that were in `old_deployment_plan`, but aren't a part
        # of `new_deployment_plan`. That is, the diff will detect all the
        # entities that were removed as part of the deployment update process.
        self._inverted_diff_perspective = False

    def extract_steps(self):
        old = self.old_deployment_plan
        new = self.new_deployment_plan
        self._extract_steps_from_description(old[DESCRIPTION],
                                             new[DESCRIPTION])
        self._extract_host_agent_plugins_steps(old[NODES], new[NODES])

        self._extract_steps(new, old)
        self._inverted_diff_perspective = True
        self._extract_steps(old, new)
        supported_steps = [step for step in self.steps if step.supported]
        self._sort_supported_steps(supported_steps)
        unsupported_steps = [step for step in self.steps if not step.supported]
        return supported_steps, unsupported_steps

    @staticmethod
    def _extract_added_nodes_names(supported_steps):
        add_node_steps = [step for step in supported_steps
                          if step.action == 'add' and step.entity_type == NODE]
        added_nodes_names = [step.entity_name for step in add_node_steps]
        return added_nodes_names

    def _create_added_nodes_graph(self, supported_steps):
        """ create a graph representing the added nodes and relationships
        involving them in the deployment update blueprint

        :rtype: nx.Digraph
        """
        added_nodes_names = self._extract_added_nodes_names(supported_steps)
        added_nodes_graph = nx.DiGraph()
        added_nodes_graph.add_nodes_from(added_nodes_names)
        nodes = self.new_deployment_plan[NODES]
        for node in nodes:
            node_name = node['id']
            if node_name in added_nodes_names:
                for relationship in node[RELATIONSHIPS]:
                    if relationship[TARGET_ID] in added_nodes_names:
                        added_nodes_graph.add_edge(node_name,
                                                   relationship[TARGET_ID])
        return added_nodes_graph

    def _update_topology_order_of_add_node_steps(
            self, supported_steps, topologically_sorted_added_nodes):
        for i, node_name in enumerate(topologically_sorted_added_nodes):
            # Get the corresponding 'add node' step for this node name,
            # and assign it its topology_order order
            for step in supported_steps:
                if step.action == 'add' and step.entity_type == NODE \
                        and step.entity_name == node_name:
                    step.topology_order = i

    def _sort_supported_steps(self, supported_steps):
        added_nodes_graph = self._create_added_nodes_graph(supported_steps)
        topologically_sorted_added_nodes = nx.topological_sort(
            added_nodes_graph)
        self._update_topology_order_of_add_node_steps(
            supported_steps, topologically_sorted_added_nodes)
        supported_steps.sort()

    def _extract_steps_from_description(self,
                                        old_description,
                                        new_description):
        with self.entity_id_builder.extend_id(DESCRIPTION):
            if old_description is None:
                if new_description is not None:
                    self._create_step(DESCRIPTION)
            else:
                if new_description is None:
                    self._inverted_diff_perspective = True
                    self._create_step(DESCRIPTION)
                    self._inverted_diff_perspective = False
                else:
                    if old_description != new_description:
                        self._create_step(DESCRIPTION, modify=True)


    @staticmethod
    def _find_matching_plugin(new_plugin, old_plugins):
        for old_plugin in old_plugins:
            if (old_plugin['name'] == new_plugin['name'] and
                    old_plugin['executor'] == new_plugin['executor']):
                return old_plugin
        return None

    @staticmethod
    def _get_matching_relationship(relationship, relationships):
        r_type = relationship[TYPE]
        target_id = relationship[TARGET_ID]
        for rel_index, other_relationship in enumerate(relationships):
            other_r_type = other_relationship[TYPE]
            other_target_id = other_relationship[TARGET_ID]
            if r_type == other_r_type and other_target_id == target_id:
                return other_relationship, rel_index
        return None, None

    def _extract_steps_from_relationships(self,
                                          relationships,
                                          old_relationships):
        with self.entity_id_builder.extend_id(RELATIONSHIPS):
            for relationship_index, relationship in enumerate(relationships):
                with self.entity_id_builder.extend_id(
                        '[{0}]'.format(relationship_index)):
                    matching_relationship, old_rel_index = \
                        self._get_matching_relationship(relationship,
                                                        old_relationships)
                    if matching_relationship:
                        # relationship has been reordered to a different index
                        if old_rel_index != relationship_index:
                            with self.entity_id_builder.\
                                    prepend_id_last_element(
                                    '[{0}]'.format(old_rel_index)):
                                self._create_step(RELATIONSHIP,
                                                  modify=True)
                        for field_name in relationship:
                            if field_name in [SOURCE_OPERATIONS,
                                              TARGET_OPERATIONS]:
                                self._extract_steps_from_operations(
                                    operations_type_name=field_name,
                                    operations=relationship[field_name],
                                    old_operations=matching_relationship[
                                        field_name]
                                )
                            elif field_name == PROPERTIES:
                                # modifying relationship properties is not
                                # supported yet
                                self._extract_steps_from_entities(
                                    entities_name=PROPERTIES,
                                    new_entities=relationship[PROPERTIES],
                                    old_entities=matching_relationship[
                                        PROPERTIES],
                                    supported=False
                                )
                    else:
                        self._create_step(RELATIONSHIP)

    def _extract_steps_from_operations(self,
                                       operations_type_name,
                                       operations,
                                       old_operations):
        with self.entity_id_builder.extend_id(operations_type_name):
            for operation_name in operations:
                with self.entity_id_builder.extend_id(operation_name):
                    if operation_name in old_operations:
                        old_operation = old_operations[operation_name]
                        if old_operation != operations[operation_name]:
                            self._create_step(
                                OPERATION, modify=True)
                    else:
                        self._create_step(OPERATION)

    def _extract_steps_from_nodes(self,
                                  new_nodes,
                                  old_nodes):
        old_nodes = {node['id']: node for node in old_nodes}
        with self.entity_id_builder.extend_id(NODES):
            for node in new_nodes:
                node_name = node['id']
                with self.entity_id_builder.extend_id(node_name):
                    if node_name in old_nodes:
                        old_node = old_nodes[node_name]
                        if node[TYPE] != old_node[TYPE] or \
                                _is_contained_in_changed(node, old_node):
                            # a node that changed its type or its host_id
                            # counts as a different node
                            self._create_step(NODE,
                                              supported=False,
                                              modify=True)
                            # Since the node was classified as added or
                            # removed, there is no need to compare its other
                            # fields.
                            continue
                        self._extract_steps_from_operations(
                            operations_type_name=OPERATIONS,
                            operations=node[OPERATIONS],
                            old_operations=old_node[OPERATIONS]
                        )
                        self._extract_steps_from_relationships(
                            relationships=node[RELATIONSHIPS],
                            old_relationships=old_node[RELATIONSHIPS]
                        )
                        self._extract_steps_from_entities(
                            PROPERTIES,
                            new_entities=node[PROPERTIES],
                            old_entities=old_node[PROPERTIES]
                        )
                    else:
                        # add (remove) node step
                        self._create_step(NODE)

    def _extract_steps_from_entities(self,
                                     entities_name,
                                     new_entities,
                                     old_entities,
                                     supported=True):
        with self.entity_id_builder.extend_id(entities_name):
            for entity_name in new_entities:
                with self.entity_id_builder.extend_id(entity_name):
                    if entity_name in old_entities:
                        if self._has_changed(
                            entities_name,
                            old_entities[entity_name],
                            new_entities[entity_name]
                        ):
                            self._create_step(
                                self.entity_id_segment_to_entity_type[
                                    entities_name],
                                supported,
                                modify=True
                            )
                    else:
                        self._create_step(
                            self.entity_id_segment_to_entity_type[
                                entities_name],
                            supported
                        )

    def _has_changed(self, entities_name, old, new):
        """Is old different than new, if both are of kind entity_type?

        Some entity kinds need a more complex comparison than just !=.
        """
        if entities_name == GROUPS:
            # group's members is a list, but it should ignore order
            old_clone = old.copy()
            new_clone = new.copy()
            old_members = set(old_clone.pop('members', ()))
            new_members = set(new_clone.pop('members', ()))
            return old_members != new_members or old_clone != new_clone
        return old != new

    def _extract_steps(self, new, old):
        for entities_name, new_entities in new.items():
            old_entities = old.get(entities_name, {})
            if entities_name == NODES:
                self._extract_steps_from_nodes(new_entities, old_entities)
            elif entities_name == OUTPUTS:
                self._extract_steps_from_entities(OUTPUTS,
                                                  new_entities,
                                                  old_entities)
            elif entities_name == WORKFLOWS:
                self._extract_steps_from_entities(WORKFLOWS,
                                                  new_entities,
                                                  old_entities)
            elif entities_name == POLICY_TYPES:
                self._extract_steps_from_entities(POLICY_TYPES,
                                                  new_entities,
                                                  old_entities,
                                                  supported=False)
            elif entities_name == POLICY_TRIGGERS:
                self._extract_steps_from_entities(POLICY_TRIGGERS,
                                                  new_entities,
                                                  old_entities,
                                                  supported=False)
            elif entities_name == GROUPS:
                self._extract_steps_from_entities(GROUPS,
                                                  new_entities,
                                                  old_entities,
                                                  supported=False)

    def _determine_step_action(self, modify):
        """ Determine if the step will be of type 'add', 'remove', 'modify' or
        no type at all

        Notice that you can identify an entity modification ('modify' action)
        whether you compare the old plan to the new plan, or whether you
        compare new plan to the old plan, but you only need to create one
        'modify' step representing this modification.
        We chose, arbitrarily, to create the 'modify' step when we compare the
        new to the old (_inverted_diff_perspective == False).
        """
        if modify and not self._inverted_diff_perspective:
            return 'modify'
        elif modify:
            return None
        elif self._inverted_diff_perspective:
            return 'remove'
        else:
            return 'add'

    def _create_step(self, entity_type, supported=True, modify=False):
        action = self._determine_step_action(modify)
        entity_id = self.entity_id_builder.entity_id
        if action:
            step = DeploymentUpdateStep(action,
                                        entity_type,
                                        entity_id,
                                        supported)
            self.steps.append(step)


def extract_steps(client, deployment, new_plan):
    nodes = client.nodes.list(deployment_id=deployment.id)
    if new_plan[DESCRIPTION] != deployment.description:
        yield DeploymentUpdateStep(
            action='modify',
            entity_type=DESCRIPTION,
            entity_id=DESCRIPTION,
        )
    yield from _extract_host_agent_plugins_steps(new_plan[NODES], nodes)
    yield from _diff_nodes(new_plan[NODES], nodes)
    for action, key in _diff_dicts(deployment.outputs, new_plan[OUTPUTS]):
        yield DeploymentUpdateStep(
            action=action,
            entity_type=OUTPUT,
            entity_id=f'{OUTPUTS}:{key}'
        )
    for action, key in _diff_dicts(deployment.workflows, new_plan[WORKFLOWS]):
        yield DeploymentUpdateStep(
            action=action,
            entity_type=WORKFLOW,
            entity_id=f'{WORKFLOWS}:{key}'
        )
    for action, key in _diff_dicts(
                deployment[POLICY_TYPES], new_plan[POLICY_TYPES]):
        yield DeploymentUpdateStep(
            action=action,
            entity_type=POLICY_TYPE,
            entity_id=f'{POLICY_TYPES}:{key}',
            supported=False
        )
    for action, key in _diff_dicts(
                deployment[POLICY_TRIGGERS], new_plan[POLICY_TRIGGERS]):
        yield DeploymentUpdateStep(
            action=action,
            entity_type=POLICY_TYPE,
            entity_id=f'{POLICY_TYPES}:{key}',
            supported=False
        )


def _extract_host_agent_plugins_steps(new_nodes, old_nodes):
    # We want to update both the node's `plugins_to_install` and `plugins`
    old_nodes = {node['id']: node for node in old_nodes}
    for entity_type in (PLUGINS_TO_INSTALL, PLUGINS):
        for new_node in new_nodes:
            new_node_name = new_node['id']
            new_plugins = new_node.get(entity_type) or []

            # If it's a new node, the plugin will be installed anyway
            if not new_plugins or new_node_name not in old_nodes:
                continue

            old_node = old_nodes[new_node_name]
            old_plugins = old_node.get(entity_type) or []
            if old_node.get(entity_type) == new_node.get(entity_type):
                continue
            for new_plugin in new_plugins:
                old_plugin = _find_matching_plugin(new_plugin, old_plugins)
                if old_plugin == new_plugin:
                    continue

                entity_id =\
                    f'{entity_type}:{new_node_name}:{new_plugin["name"]}'
                action = 'add' if not old_plugin else 'modify'
                yield DeploymentUpdateStep(
                    action=action,
                    entity_type=PLUGIN,
                    entity_id=entity_id,
                )


def _find_matching_plugin(new_plugin, old_plugins):
    for old_plugin in old_plugins:
        if (old_plugin['name'] == new_plugin['name'] and
                old_plugin['executor'] == new_plugin['executor']):
            return old_plugin
    return None


def _diff_nodes(new_nodes, old_nodes):
    old_nodes = {node['id']: node for node in old_nodes}
    # with self.entity_id_builder.extend_id(NODES):
    for node in new_nodes:
        node_name = node['id']
        # with self.entity_id_builder.extend_id(node_name):
        if node_name not in old_nodes:
            yield DeploymentUpdateStep(
                action='add',
                entity_type=NODE,
                entity_id=f'{NODES}:{node_name}',
            )
            continue
        old_node = old_nodes[node_name]
        if node[TYPE] != old_node[TYPE] or \
                _is_contained_in_changed(node, old_node):
            # a node that changed its type or its host_id
            # counts as a different node
            yield DeploymentUpdateStep(
                action='modify',
                entity_type=NODE,
                entity_id=f'{NODES}:{node_name}',
                supported=False
            )
            # Since the node was classified as added or
            # removed, there is no need to compare its other
            # fields.
            continue
        yield from _diff_node(node, old_node)


def _diff_node(new_node, old_node):
    node_name = new_node['id']
    for action, key in _diff_dicts(new_node[OPERATIONS], old_node[OPERATIONS]):
        yield DeploymentUpdateStep(
            action=action,
            entity_type=OPERATION,
            entity_id=f'{NODES}:{node_name}:{OPERATIONS}:{key}'
        )

    for rel_index, relationship in enumerate(new_node[RELATIONSHIPS]):
        entity_id_base = f'{NODES}:{node_name}:{RELATIONSHIPS}'
        old_relationship, old_rel_index = \
            _get_matching_relationship(relationship, old_node[RELATIONSHIPS])
        if old_relationship is None:
            yield DeploymentUpdateStep(
                action='add',
                entity_type=RELATIONSHIP,
                entity_id=f'{entity_id_base}:[{rel_index}]',
            )
            continue

        if old_rel_index != rel_index:
            yield DeploymentUpdateStep(
                action='modify',
                entity_type=RELATIONSHIP,
                entity_id=f'{entity_id_base}:[{old_rel_index}]:[{rel_index}]',
            )

        for op_type in [SOURCE_OPERATIONS, TARGET_OPERATIONS]:
            for action, key in _diff_dicts(
                    relationship[op_type], old_relationship[op_type]):
                yield DeploymentUpdateStep(
                    action=action,
                    entity_type=OPERATION,
                    entity_id=f'{entity_id_base}:[{rel_index}]:{op_type}:{key}'
                )
        for action, key in _diff_dicts(
                relationship[PROPERTIES], old_relationship[PROPERTIES]):
            # modifying relationship properties is not  supported yet
            yield DeploymentUpdateStep(
                action=action,
                entity_type=PROPERTY,
                entity_id=f'{entity_id_base}:[{rel_index}]:{PROPERTIES}:{key}',
                supported=False,
            )
    for rel_index, relationship in old_node[RELATIONSHIPS]:
        entity_id_base = f'{NODES}:{node_name}:{RELATIONSHIPS}'
        matching_relationship, _ = \
            _get_matching_relationship(relationship, new_node[RELATIONSHIPS])
        if matching_relationship is None:
            yield DeploymentUpdateStep(
                action='remove',
                entity_type=RELATIONSHIP,
                entity_id=f'{entity_id_base}:[{rel_index}]',
            )

    for action, key in _diff_dicts(new_node[PROPERTIES], old_node[PROPERTIES]):
        yield DeploymentUpdateStep(
            action=action,
            entity_type=PROPERTY,
            entity_id=f'{NODES}:{node_name}:{PROPERTIES}:{key}'
        )


def _diff_operations(entity_id_base, operations, old_operations):
    for op_name in set(operations) | set(old_operations):
        new_operation = operations.get(op_name)
        old_operation = old_operations.get(op_name)
        if new_operation is not None and old_operation is None:
            action = 'add'
        elif new_operation is None and old_operation is not None:
            action = 'remove'
        elif new_operation != old_operation:
            action = 'modify'
        else:
            continue
        yield DeploymentUpdateStep(
            action=action,
            entity_type=OPERATION,
            entity_id=f'{entity_id_base}:{op_name}',
        )


def _diff_dicts(new, old, compare=operator.eq):
    for key in set(new) | set(old):
        if key in new and key not in old:
            yield 'add', key
        elif key in old and key not in new:
            yield 'remove', key
        elif not compare(new[key], old[key]):
            yield 'modify', key


def _extract_steps_from_operations(operations_type_name,
                                   operations,
                                   old_operations):
    # with self.entity_id_builder.extend_id(operations_type_name):
    for operation_name in operations:
        # with self.entity_id_builder.extend_id(operation_name):
        if operation_name in old_operations:
            old_operation = old_operations[operation_name]
            if old_operation != operations[operation_name]:
                self._create_step(
                    OPERATION, modify=True)
        else:
            self._create_step(OPERATION)


def _get_matching_relationship(relationship, relationships):
    r_type = relationship[TYPE]
    target_id = relationship[TARGET_ID]
    for rel_index, other_relationship in enumerate(relationships):
        other_r_type = other_relationship[TYPE]
        other_target_id = other_relationship[TARGET_ID]
        if r_type == other_r_type and other_target_id == target_id:
            return other_relationship, rel_index
    return None, None


def _extract_steps_from_relationships(self,
                                      relationships,
                                      old_relationships):
    with self.entity_id_builder.extend_id(RELATIONSHIPS):
        for relationship_index, relationship in enumerate(relationships):
            with self.entity_id_builder.extend_id(
                    '[{0}]'.format(relationship_index)):
                matching_relationship, old_rel_index = \
                    self._get_matching_relationship(relationship,
                                                    old_relationships)
                if matching_relationship:
                    # relationship has been reordered to a different index
                    if old_rel_index != relationship_index:
                        with self.entity_id_builder.\
                                prepend_id_last_element(
                                '[{0}]'.format(old_rel_index)):
                            self._create_step(RELATIONSHIP,
                                              modify=True)
                    for field_name in relationship:
                        if field_name in [SOURCE_OPERATIONS,
                                          TARGET_OPERATIONS]:
                            self._extract_steps_from_operations(
                                operations_type_name=field_name,
                                operations=relationship[field_name],
                                old_operations=matching_relationship[
                                    field_name]
                            )
                        elif field_name == PROPERTIES:
                            # modifying relationship properties is not
                            # supported yet
                            self._extract_steps_from_entities(
                                entities_name=PROPERTIES,
                                new_entities=relationship[PROPERTIES],
                                old_entities=matching_relationship[
                                    PROPERTIES],
                                supported=False
                            )
                else:
                    self._create_step(RELATIONSHIP)


def _is_contained_in_changed(node, other_node):
    node_container = next(
        (r['target_id'] for r in node['relationships']
         if CONTAINED_IN_RELATIONSHIP_TYPE in r[TYPE_HIERARCHY]), None)
    other_node_container = next(
        (r['target_id'] for r in other_node['relationships']
         if CONTAINED_IN_RELATIONSHIP_TYPE in r[TYPE_HIERARCHY]), None)
    return node_container != other_node_container
