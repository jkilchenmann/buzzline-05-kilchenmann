"""
consumer_kilchenmann.py

Kafka consumer that:
- Processes messages from Kafka.
- When a message's 'keyword_mentioned' is "Python", it writes the message to a CSV file,
  inserts it into a SQLite database, and updates a count for the author.
- Upon shutdown, generates a bar chart showing how many times each author mentioned "Python".
"""

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

# External modules
from kafka import KafkaConsumer
import matplotlib.pyplot as plt

# Local modules
import utils.utils_config as config
from utils.utils_logger import logger
import db_sqlite_case as db_sqlite  # Our SQLite helper module

#####################################
# Define File Paths
#####################################

# CSV file path for Python messages
CSV_FILE_PATH = "python_messages.csv"

# Bar chart output path
BAR_CHART_PATH = "python_mentions_by_author.png"

# SQLite Database file path (using config base data path)
try:
    BASE_DATA_PATH: pathlib.Path = config.get_base_data_path()  # Assuming this returns a Path
except Exception as e:
    logger.error(f"ERROR: Could not get base data path: {e}")
    sys.exit(1)
DB_FILE_PATH = BASE_DATA_PATH / "streamed_messages.sqlite"

#####################################
# Helper Functions
#####################################

def initialize_csv(file_path: str) -> None:
    """
    Create the CSV file with a header if it does not exist.
    """
    if not os.path.exists(file_path):
        with open(file_path, mode="w", newline="") as csvfile:
            fieldnames = [
                "message",
                "author",
                "timestamp",
                "category",
                "sentiment",
                "keyword_mentioned",
                "message_length",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        logger.info(f"CSV file initialized with header at: {file_path}")

def append_to_csv(file_path: str, message: dict) -> None:
    """
    Append a single message as a row in the CSV file.
    """
    with open(file_path, mode="a", newline="") as csvfile:
        fieldnames = [
            "message",
            "author",
            "timestamp",
            "category",
            "sentiment",
            "keyword_mentioned",
            "message_length",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(message)
    logger.info(f"Appended message from author '{message.get('author')}' to CSV.")

def generate_bar_chart(author_counts: dict, output_path: str) -> None:
    """
    Generate and save a bar chart from the author counts.
    """
    if not author_counts:
        logger.warning("No data to generate a bar chart.")
        return

    authors = list(author_counts.keys())
    counts = [author_counts[author] for author in authors]

    plt.figure(figsize=(8, 6))
    bars = plt.bar(authors, counts, color="skyblue")
    plt.xlabel("Author")
    plt.ylabel("Number of 'Python' Mentions")
    plt.title("Mentions of 'Python' by Author")
    plt.ylim(0, max(counts) + 1)

    # Add count labels above each bar
    for bar in bars:
        height = bar.get_height()
        plt.text(
            bar.get_x() + bar.get_width() / 2,
            height,
            str(height),
            ha="center",
            va="bottom",
        )

    plt.tight_layout()
    plt.savefig(output_path)
    logger.info(f"Bar chart saved to {output_path}.")

#####################################
# Main Consumer Function
#####################################

def main() -> None:
    logger.info("Starting Kafka consumer for 'Python' messages with SQLite integration.")

    # Read environment variables for Kafka
    try:
        kafka_server: str = config.get_kafka_broker_address()
        topic: str = config.get_kafka_topic()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # Initialize CSV file
    initialize_csv(CSV_FILE_PATH)

    # Initialize SQLite database
    db_sqlite.init_db(DB_FILE_PATH)

    # Dictionary to count the number of "Python" mentions per author
    author_counts = defaultdict(int)

    # Create Kafka consumer
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

    logger.info("Consuming messages. Press Ctrl+C to exit and generate the bar chart.")

    try:
        for msg in consumer:
            message = msg.value
            logger.info(f"Received message: {message}")

            # Process only messages that mention "Python"
            if message.get("keyword_mentioned") == "Python":
                # Append the message to CSV
                append_to_csv(CSV_FILE_PATH, message)

                # Insert the message into the SQLite database
                db_sqlite.insert_message(message, DB_FILE_PATH)

                # Update the count for the author
                author = message.get("author", "Unknown")
                author_counts[author] += 1
                logger.info(f"Updated count for author '{author}': {author_counts[author]}")
            else:
                logger.debug("Message does not mention 'Python'; skipping CSV and DB insert.")
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected error in consumer: {e}")
    finally:
        consumer.close()
        logger.info("Kafka consumer closed.")
        # Generate the bar chart using collected counts
        generate_bar_chart(author_counts, BAR_CHART_PATH)

if __name__ == "__main__":
    main()
