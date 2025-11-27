import os
import kopf
import kubernetes
import yaml
import json
import argparse
import hashlib
import protocol.leshan as leshan
import protocol.nextgengw as nextgengw

from threading import Lock

@kopf.on.create('faredgeroute')
def on_route_created(spec, name, namespace, logger, memo: kopf.Memo, **kwargs):
    logger.info(f'Far Edge route created {name}: {spec}')

    if spec['route_type'] != 'single':
        raise kopf.PermanentError('Only "single" route_type is supported for now')

    if 'pod_labels' in spec['egress'] or 'pod_labels' in spec['ingress']:
        raise kopf.PermanentError('"pod_labels" not supported for now')

    if 'pod_name' not in spec['egress'] or 'pod_name' not in spec['ingress']:
        raise kopf.PermanentError('"pod_name" is required for now')

    logger.info(f'Deploying Far Edge route {name}...')

    core_api = kubernetes.client.CoreV1Api()

    # Collect egress Pods
    field_selector = 'metadata.name=' + spec['egress']['pod_name']
    egress_pods = core_api.list_namespaced_pod(
        watch=False, 
        namespace=memo.namespace, 
        field_selector=field_selector
    )

    if len(egress_pods.items) != 1:
        raise kopf.TemporaryError('egress pod not found', delay=30)

    if egress_pods.items[0].status.phase != 'Running':
        raise kopf.TemporaryError('egress pod is being deployed, retrying in a bit', delay=10)

    if 'embserve.fhp.pt/runtime' not in egress_pods.items[0].metadata.annotations or egress_pods.items[0].metadata.annotations['embserve.fhp.pt/runtime'] != 'embserve':
        raise kopf.PermanentError('egress pod is not an embserve node')

    # Collect ingress Pods
    field_selector = 'metadata.name=' + spec['ingress']['pod_name']
    ingress_pods = core_api.list_namespaced_pod(
        watch=False, 
        namespace=memo.namespace, 
        field_selector=field_selector
    )

    if len(ingress_pods.items) != 1:
        raise kopf.TemporaryError('ingress pod not found', delay=30)

    if ingress_pods.items[0].status.phase != 'Running':
        raise kopf.TemporaryError('ingress pod is being deployed, retrying in a bit', delay=10)

    if 'embserve.fhp.pt/runtime' not in ingress_pods.items[0].metadata.annotations or ingress_pods.items[0].metadata.annotations['embserve.fhp.pt/runtime'] != 'embserve':
        raise kopf.PermanentError('egress pod is not an embserve node')

    # Create a small route name since embServe has a maximum len per segment (16)
    # Note that this will need to be unique per route
    route_name = hashlib.sha256(name.encode('utf-8')).hexdigest()[:8]

    # Collect first route information (egress)
    egress_route = {
        'pod_name': egress_pods.items[0].metadata.name,
        'node_name': egress_pods.items[0].spec.node_name,
        'address': egress_pods.items[0].metadata.annotations['embserve.fhp.pt/address'],
        'service_id': -1,
        'node_id': egress_pods.items[0].metadata.annotations['embserve.fhp.pt/nodeId'],
        'origin_uri': '',
        # TODO: If the route is internal, this may not work since the destination is the device itself
        'destination_uri': f'coap://{ingress_pods.items[0].metadata.annotations["embserve.fhp.pt/address"]}/k8s/{route_name}'
    }

    services = json.loads(egress_pods.items[0].metadata.annotations['embserve.fhp.pt/serviceMetadata'])
    logger.info(f'egress services: {services}')
    service_output = None
    
    for service in services:
        if service['name'] == spec['egress']['service_name']:
            for io in service['outputs']:
                if io['key'] == spec['egress']['service_key']:
                    service_output = io
                    break

            if service_output != None:
                egress_route['service_id'] = service['id']
                egress_route['origin_uri'] = f'local://{service["id"]}/{service_output["key"]}'
                break

    if service_output == None:
        raise kopf.PermanentError('failed to find service for egress')

    # Collect second route information (ingress)
    ingress_route = {
        'pod_name': ingress_pods.items[0].metadata.name,
        'node_name': ingress_pods.items[0].spec.node_name,
        'address': ingress_pods.items[0].metadata.annotations['embserve.fhp.pt/address'],
        'service_id': -1,
        'node_id': ingress_pods.items[0].metadata.annotations['embserve.fhp.pt/nodeId'],
        # TODO: If the route is internal, this may not work since the origin is the device itself
        'origin_uri': f'coap://k8s/{route_name}',
        'destination_uri': ''
    }
    services = json.loads(ingress_pods.items[0].metadata.annotations['embserve.fhp.pt/serviceMetadata'])
    logger.info(f'ingress services: {services}')
    service_input = None

    for service in services:
        if service['name'] == spec['ingress']['service_name']:
            for io in service['inputs']:
                if io['key'] == spec['ingress']['service_key']:
                    service_input = io
                    break

            if service_input != None:
                ingress_route['service_id'] = service['id']
                ingress_route['destination_uri'] = f'local://{service["id"]}/{service_input["key"]}'
                break

    if service_input == None:
        raise kopf.PermanentError('failed to find service for ingress')

    if service_output['type'] != service_input['type']:
        raise kopf.PermanentError(f'input type ({service_input["type"]}) differs from output type ({service_output["type"]})')

    logger.info(f'Will create egress route: {egress_route}')
    logger.info(f'Will create ingress route: {ingress_route}')

    # Create egress_route
    result = False
    try:
        memo.lock.acquire()

        if memo.server_type == 'leshan':
            result = leshan.create_route(f'http://{memo.server_url}:{memo.server_port}', egress_route['node_id'], egress_route['origin_uri'],  egress_route['destination_uri'], logger)
        else:
            result = nextgengw.create_route(egress_route['node_id'], egress_route['origin_uri'],  egress_route['destination_uri'], logger)
    except:
        pass

    finally:
        memo.lock.release()

    if not result:
        raise kopf.TemporaryError('failed to create egress route in device', delay=30)

    # Create ingress_route
    result = False
    try:
        memo.lock.acquire()

        if memo.server_type == 'leshan':
            result = leshan.create_route(f'http://{memo.server_url}:{memo.server_port}', ingress_route['node_id'], ingress_route['origin_uri'],  ingress_route['destination_uri'], logger)
        else:
            result = nextgengw.create_route(ingress_route['node_id'], ingress_route['origin_uri'],  ingress_route['destination_uri'], logger)
    except:
        pass

    finally:
        memo.lock.release()

    if not result:
        try:
            # Rollback egress route
            if memo.server_type == 'leshan':
                leshan.delete_route(f'http://{memo.server_url}:{memo.server_port}', egress_route['node_id'], egress_route['origin_uri'],  egress_route['destination_uri'], logger)
            else:
                nextgengw.delete_route(egress_route['node_id'], egress_route['origin_uri'],  egress_route['destination_uri'], logger)
        except:
            pass
        
        raise kopf.TemporaryError('failed to create ingress route in device', delay=30)

    logger.info(f'Far Edge route {name} deployed')

    return {
        'egress_routes': [egress_route],
        'ingress_routes': [ingress_route],
    }

@kopf.on.delete('faredgeroute')
def on_route_deleted(spec, name, status, namespace, logger, memo: kopf.Memo, **kwargs):
    logger.info(f'on_route_deleted')

    if 'on_route_created' not in status:
        logger.warn('on_route_created is not populated')
        return

    if 'egress_routes' not in status['on_route_created'] or 'ingress_routes' not in status['on_route_created']:
        logger.warn('egress_routes or ingress_routes is None')
        return

    logger.info(f'Deleting Far Edge route {name}...')

    for route in status['on_route_created']['egress_routes']:
        logger.info(f'Deleting egress route {route}...')

        try:
            if memo.server_type == 'leshan':
                leshan.delete_route(f'http://{memo.server_url}:{memo.server_port}', route['node_id'], route['origin_uri'], route['destination_uri'], logger)
            else:
                nextgengw.delete_route(route['node_id'], route['origin_uri'], route['destination_uri'], logger)
        except Exception as e:
            # This may fail if node disappeared
            continue

    for route in status['on_route_created']['ingress_routes']:
        logger.info(f'Deleting ingress route {route}...')
       
        try:
            if memo.server_type == 'leshan':
                leshan.delete_route(f'http://{memo.server_url}:{memo.server_port}', route['node_id'], route['origin_uri'],  route['destination_uri'], logger)
            else:
                nextgengw.delete_route(route['node_id'], route['origin_uri'], route['destination_uri'], logger)
        except Exception as e:
            # This may fail if node disappeared
            logger.error(e)
            logger.info('Ignoring....')
            continue

    logger.info(f'Far Edge route {name} deleted')

@kopf.on.delete('v1', 'pods', 
    annotations={'embserve.fhp.pt/runtime': 'embserve'})
def on_pod_deleted(spec, name, namespace, logger, memo: kopf.Memo, **kwargs):
    logger.info(f'on_pod_deleted')

    routes_to_delete = []
    api = kubernetes.client.CustomObjectsApi()
    routes = api.list_namespaced_custom_object('embserve.fhp.pt', 'v1', memo.namespace, 'faredgeroutes')

    for route in routes['items']:
        try:
            for egress_route in route['status']['on_route_created']['egress_routes']:
                if egress_route['pod_name'] == name and route['metadata']['name'] not in routes_to_delete:
                    routes_to_delete.append(route['metadata']['name'])
                    break

            for ingress_route in route['status']['on_route_created']['ingress_routes']:
                if ingress_route['pod_name'] == name and route['metadata']['name'] not in routes_to_delete:
                    routes_to_delete.append(route['metadata']['name'])
                    break
        except Exception as e:
            logger.error(e)
            logger.info('Ignoring....')
            continue

    for route_name in routes_to_delete:
        logger.info(f'Deleting Far Edge route {route_name} since Pod {name} was deleted')
        api.delete_namespaced_custom_object('embserve.fhp.pt', 'v1', memo.namespace, 'faredgeroutes', route_name)

@kopf.on.startup()
def on_startup(logger, memo: kopf.Memo, settings: kopf.OperatorSettings, **kwargs):
    settings.persistence.finalizer = 'fecp.operator.fhp.pt/finalizer'
    settings.persistence.progress_storage = kopf.MultiProgressStorage([
        kopf.AnnotationsProgressStorage(prefix='fecp.operator.fhp.pt'),
        kopf.StatusProgressStorage(field='status.fecp'),
    ])
    settings.persistence.diffbase_storage = kopf.AnnotationsDiffBaseStorage(
        prefix='fecp.operator.fhp.pt',
        key='last-handled-configuration',
    )

    memo.server_type = os.environ.get('FAR_EDGE_SERVER_TYPE', 'leshan')
    memo.server_url = os.environ.get('FAR_EDGE_SERVER_URL', 'localhost')
    memo.server_port = os.environ.get('FAR_EDGE_SERVER_PORT', '5432')
    memo.namespace = os.environ.get('NAMESPACE', 'default')

    memo.lock = Lock()

    if memo.server_type == 'nextgengw':
        nextgengw.connect(memo.server_url, memo.server_port)
