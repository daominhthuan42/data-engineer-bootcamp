from utils.logger import logger
from kafka.producer import create_producer
from simulator.journey import simulate_journey

def main():
    producer = create_producer(logger=logger)
    simulate_journey(producer, "Vehicle-ThuanDao-123")

if __name__ == "__main__":
    try:
        logger.info("Starting Smart City simulation...")
        main()
    except KeyboardInterrupt:
        logger.warning("Simulation ended by user.")
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
