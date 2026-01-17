import os
import random
import logging
from flask import Flask, request, Response
import requests

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Environment variables
PORT = int(os.getenv('PORT', 8000))
MONOLITH_URL = os.getenv('MONOLITH_URL', 'http://localhost:8080')
MOVIES_SERVICE_URL = os.getenv('MOVIES_SERVICE_URL', 'http://localhost:8081')
EVENTS_SERVICE_URL = os.getenv('EVENTS_SERVICE_URL', 'http://localhost:8082')
GRADUAL_MIGRATION = os.getenv('GRADUAL_MIGRATION', 'false').lower() == 'true'
MOVIES_MIGRATION_PERCENT = int(os.getenv('MOVIES_MIGRATION_PERCENT', 0))

logger.info(f"Proxy Service Configuration:")
logger.info(f"  Port: {PORT}")
logger.info(f"  Monolith URL: {MONOLITH_URL}")
logger.info(f"  Movies Service URL: {MOVIES_SERVICE_URL}")
logger.info(f"  Events Service URL: {EVENTS_SERVICE_URL}")
logger.info(f"  Gradual Migration: {GRADUAL_MIGRATION}")
logger.info(f"  Movies Migration Percent: {MOVIES_MIGRATION_PERCENT}%")


def should_route_to_new_service():
    """
    Determines if request should be routed to new movies microservice
    based on migration percentage (Strangler Fig pattern)
    """
    if not GRADUAL_MIGRATION:
        logger.debug("GRADUAL_MIGRATION is disabled, routing to monolith")
        return False

    if MOVIES_MIGRATION_PERCENT <= 0:
        logger.debug("MOVIES_MIGRATION_PERCENT is 0 or not set, routing to monolith")
        return False

    # Random selection based on migration percentage
    random_value = random.randint(0, 100)
    should_migrate = random_value <= MOVIES_MIGRATION_PERCENT

    logger.debug(f"Migration decision: random={random_value}, threshold={MOVIES_MIGRATION_PERCENT}, migrate={should_migrate}")
    return should_migrate


def proxy_request(target_url, path):
    """
    Proxies the incoming request to the target service
    """
    # Build the full URL
    url = f"{target_url}{path}"

    # Copy query parameters
    if request.query_string:
        url = f"{url}?{request.query_string.decode('utf-8')}"

    # Prepare headers (remove host header to avoid conflicts)
    headers = {key: value for key, value in request.headers if key.lower() != 'host'}

    try:
        # Forward the request to the target service
        logger.info(f"Proxying {request.method} request to: {url}")

        if request.method == 'GET':
            resp = requests.get(url, headers=headers, timeout=30)
        elif request.method == 'POST':
            resp = requests.post(url, headers=headers, data=request.get_data(), timeout=30)
        elif request.method == 'PUT':
            resp = requests.put(url, headers=headers, data=request.get_data(), timeout=30)
        elif request.method == 'DELETE':
            resp = requests.delete(url, headers=headers, timeout=30)
        else:
            return Response("Method not allowed", status=405)

        # Return the response from the target service
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [(name, value) for name, value in resp.raw.headers.items()
                          if name.lower() not in excluded_headers]

        return Response(resp.content, resp.status_code, response_headers)

    except requests.exceptions.RequestException as e:
        logger.error(f"Error proxying request to {url}: {str(e)}")
        return Response(f"Proxy error: {str(e)}", status=502)


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return {'status': True}, 200


@app.route('/api/movies', methods=['GET', 'POST'])
@app.route('/api/movies/', methods=['GET', 'POST'])
def movies_proxy():
    """
    Proxy for movies endpoint with gradual migration support (Strangler Fig pattern)
    """
    # Determine target service based on migration strategy
    if should_route_to_new_service():
        target_url = MOVIES_SERVICE_URL
        service_name = "movies-service"
    else:
        target_url = MONOLITH_URL
        service_name = "monolith"

    logger.info(f"Routing /api/movies request to {service_name}")
    return proxy_request(target_url, '/api/movies')


@app.route('/api/movies/health', methods=['GET'])
def movies_health():
    """Health check for movies service"""
    return proxy_request(MOVIES_SERVICE_URL, '/api/movies/health')


@app.route('/api/events', methods=['GET', 'POST'])
@app.route('/api/events/', methods=['GET', 'POST'])
def events_proxy():
    """Proxy for events endpoint"""
    logger.info(f"Routing /api/events request to events-service")
    return proxy_request(EVENTS_SERVICE_URL, '/api/events')


@app.route('/api/events/health', methods=['GET'])
def events_health():
    """Health check for events service"""
    return proxy_request(EVENTS_SERVICE_URL, '/api/events/health')


@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def catch_all(path):
    """
    Catch-all route for all other endpoints
    Routes everything else to the monolith
    """
    logger.info(f"Routing /{path} request to monolith")
    return proxy_request(MONOLITH_URL, f'/{path}')


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=False)
