#!/usr/bin/env python3
"""
Main entry point for the application.
Runs both the healthcheck server and metering processor.
"""

import logging
import multiprocessing
import signal
import sys
import os

# Add parent directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_healthcheck_server():
    """Run the healthcheck server in a separate process."""
    # Ensure proper path is set in spawned process
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.healthcheck_server import start_healthcheck_server
    from src.config import Config
    config = Config()
    start_healthcheck_server(port=config.healthcheck_port)


def run_metering_processor():
    """Run the metering processor in a separate process."""
    # Ensure proper path is set in spawned process
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from src.metering_processor import main
    main()


def main():
    """Start both services and wait for them."""
    from src.config import Config
    config = Config()
    
    logger.info("=" * 80)
    logger.info("Starting services")
    logger.info(f"- Healthcheck server on port {config.healthcheck_port}")
    logger.info("- Metering processor (runs every 5 minutes)")
    logger.info("Press Ctrl+C to stop")
    logger.info("=" * 80)
    
    # Start both services as separate processes
    healthcheck_process = multiprocessing.Process(
        target=run_healthcheck_server,
        name="healthcheck-server"
    )
    processor_process = multiprocessing.Process(
        target=run_metering_processor,
        name="metering-processor"
    )
    
    # Handle graceful shutdown
    def signal_handler(signum, frame):
        logger.info("Received shutdown signal. Stopping services...")
        healthcheck_process.terminate()
        processor_process.terminate()
        healthcheck_process.join(timeout=5)
        processor_process.join(timeout=5)
        sys.exit(0)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    # Start processes
    healthcheck_process.start()
    logger.info(f"Healthcheck server started (PID: {healthcheck_process.pid})")
    
    processor_process.start()
    logger.info(f"Metering processor started (PID: {processor_process.pid})")
    
    # Wait for processes to complete
    try:
        healthcheck_process.join()
        processor_process.join()
    except KeyboardInterrupt:
        logger.info("Interrupted. Shutting down...")
        signal_handler(None, None)


if __name__ == '__main__':
    main()
