import requests
import json
import time

def create_route(url, device, origin_uri, destination_uri, logger):
    endpoint = f'{url}/api/clients/{device}?timeout=5&format=TLV'
    node = requests.get(endpoint).json()
    logger.debug(f'{device}: {node}')
    
    route_id = 1000

    # Previous routes configured
    if '35001' in node['availableInstances']:
        # Get all routes and check if the route already exists
        route_ids = []
        endpoint = f'{url}/api/clients/{device}/35001?timeout=5&format=TLV'
        response = requests.get(endpoint)

        for route in response.json()['content']['instances']:
            route_ids.append(route['id'])
            route_origin_uri = next((resource['value'] for resource in route['resources'] if resource['id'] == 27200), "")
            route_destination_uri = next((resource['value'] for resource in route['resources'] if resource['id'] == 27201), "")

            if route_origin_uri == origin_uri and route_destination_uri == destination_uri:
                # Route already exists
                logger.error("Route already exists")
                return True

        
        # Route does not exist, check id
        while route_id in route_ids:
            route_id += 1

    # Post new route
    endpoint = f'{url}/api/clients/{device}/35001?timeout=5&format=TLV'
    headers =  {'Content-Type':'application/json'}
    route = {
        'id': route_id, 
        'kind': 'instance', 
        'resources': [
            {
                'id': 27200,
                'kind': 'singleResource',
                'value': origin_uri,
                'type': 'string'
            },
            {
                'id':27201,
                'kind':'singleResource',
                'value': destination_uri,
                'type':'string'
            }
        ]
    }
    
    logger.debug(f'Deploying: {route}')

    response = requests.post(endpoint, data=json.dumps(route), headers=headers)
    if not response.json()['success']:
        return False

    # Wait for Leshan to update internal database
    timestamp = time.time()
    while True:
        if time.time() - timestamp > 5:
            raise TimeoutError('Timeout waiting for Leshan')

        time.sleep(0.1)

        endpoint = f'{url}/api/clients/{device}?timeout=5&format=TLV'
        node = requests.get(endpoint).json()

        if '35001' in node['availableInstances']:
            if route_id in node['availableInstances']['35001']:
                break
        
    return True

def delete_route(url, device, origin_uri, destination_uri, logger):
    endpoint = f'{url}/api/clients/{device}?timeout=5&format=TLV'
    node = requests.get(endpoint).json()

    if '35001' in node['availableInstances']:
        endpoint = f'{url}/api/clients/{device}/35001?timeout=5&format=TLV'
        response = requests.get(endpoint)

        for route in response.json()['content']['instances']:
            route_origin_uri = next((resource['value'] for resource in route['resources'] if resource['id'] == 27200), "")
            route_destination_uri = next((resource['value'] for resource in route['resources'] if resource['id'] == 27201), "")

            if route_origin_uri == origin_uri and route_destination_uri == destination_uri:
                endpoint = f'{url}/api/clients/{device}/35001/{str(route["id"])}?timeout=5'
                response = requests.delete(endpoint)
                return response.json()['success']

    logger.error("Route not found")
    return True