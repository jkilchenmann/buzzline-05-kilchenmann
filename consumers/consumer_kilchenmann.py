'''
consumer_kilchenmann.py

Kafka consumer that:
- Processes messages from Kafka.
- When a message's 'keyword_mentioned' is "Python", it writes the message to a CSV file,
  inserts it into a SQLite database, and updates a count for the author.
- Updates a bar chart dynamically showing how many times each author mentioned "Python".
'''

#####################################
# Import Modules
#####################################

# Standard library imports
import json
import csv
import os
import sys
from collections import defaultdict
import pathlib
import matplotlib.pyplot as plt

# External modules
from kafka import KafkaConsumer

# Ensure the parent directory is in sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from consumers.db_sqlite_case import init_db, insert_message
import db_sqlite_case as db_sqlite
import utils.utils_config as config
from utils.utils_logger import logger

#####################################
# Define File Paths
#####################################

CSV_FILE_PATH = "python_messages.csv"
BAR_CHART_PATH = "python_mentions_by_author.png"

try:
    BASE_DATA_PATH: pathlib.Path = config.get_base_data_path()
except Exception as e:
    logger.error(f"ERROR: Could not get base data path: {e}")
    sys.exit(1)
DB_FILE_PATH = BASE_DATA_PATH / "streamed_messages.sqlite"

#####################################
# Helper Functions
#####################################

def generate_live_bar_chart(author_counts):
    """
    Generate and update a bar chart dynamically.
    """
    plt.ion()
    plt.clf()
    authors = list(author_counts.keys())
    counts = [author_counts[author] for author in authors]

    plt.bar(authors, counts, color="skyblue")
    plt.xlabel("Author")
    plt.ylabel("Number of 'Python' Mentions")
    plt.title("Live Mentions of 'Python' by Author")
    plt.ylim(0, max(counts) + 1 if counts else 1)

    plt.draw()
    plt.pause(0.1)

#####################################
# Main Consumer Function
#####################################

def main() -> None:
    logger.info("Starting Kafka consumer with real-time chart updates.")

    try:
        kafka_server: str = config.get_kafka_broker_address()
        topic: str = config.get_kafka_topic()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    db_sqlite.init_db(DB_FILE_PATH)
    author_counts = defaultdict(int)

    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_server,
            auto_offset_reset="earliest",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        logger.info(f"Connected to Kafka topic '{topic}' at {kafka_server}.")
    except Exception as e:
        logger.error(f"ERROR: Kafka consumer connection failed: {e}")
        sys.exit(2)

    try:
        for msg in consumer:
            message = msg.value
            logger.info(f"Received message: {message}")

            if message.get("keyword_mentioned") == "Python":
                db_sqlite.insert_message(message, DB_FILE_PATH)

                author = message.get("author", "Unknown")
                author_counts[author] += 1
                logger.info(f"Updated count for author '{author}': {author_counts[author]}")

                generate_live_bar_chart(author_counts)
            else:
                logger.debug("Message does not mention 'Python'; skipping.")

    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")
        plt.ioff()

if __name__ == "__main__":
    main()
