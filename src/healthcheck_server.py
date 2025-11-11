#!/usr/bin/env python3
"""
Simple HTTP server that provides a health check endpoint.
Only exposes /health/ endpoint that returns 200 OK.
"""

import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from config import Config, ConfigurationError
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HealthCheckHandler(BaseHTTPRequestHandler):
    """HTTP request handler that only responds to /health/ endpoint."""
    
    def log_message(self, format, *args):
        """Override to use logging instead of printing to stderr."""
        logger.info("%s - %s" % (self.address_string(), format % args))
    
    def do_GET(self):
        """Handle GET requests."""
        if self.path == '/health/' or self.path == '/health':
            # Return 200 OK for health check
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
        else:
            # Return 404 for any other path
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'Not Found')
    
    def do_HEAD(self):
        """Handle HEAD requests (same as GET but without body)."""
        if self.path == '/health/' or self.path == '/health':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()


def start_healthcheck_server(port=8080, host='0.0.0.0'):
    """Start the healthcheck HTTP server."""
    server_address = (host, port)
    httpd = HTTPServer(server_address, HealthCheckHandler)
    logger.info(f"Healthcheck server started on {host}:{port}")
    logger.info(f"Health endpoint available at http://{host}:{port}/health/")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Healthcheck server shutting down...")
        httpd.shutdown()
        sys.exit(0)
