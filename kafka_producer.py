import argparse
import json
import logging
import random
import string
import sys
import time
from datetime import datetime

from kafka import KafkaProducer

from kafka_config import KAFKA_CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - kafka_producer - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka_producer")


def _random_id(prefix, n=8):
    return f"{prefix}_" + "".join(random.choices(string.ascii_letters + string.digits, k=n))


def _build_sample_payload(topic, i):
    now = datetime.utcnow().isoformat() + "Z"
    # Échantillons minimaux selon nos topics connus; generique sinon
    if topic == "transaction_stream":
        return {
            "transaction_id": _random_id("tx"),
            "user_id": random.randint(1, 10000),
            "amount": round(random.uniform(1.0, 500.0), 2),
            "currency": random.choice(["EUR", "USD", "GBP"]),
            "timestamp": now,
            "status": random.choice(["approved", "declined"]),
        }
    elif topic == "transaction_flattened":
        return {
            "transaction_id": _random_id("tx"),
            "user_country": random.choice(["FR", "DE", "ES", "IT"]),
            "payment_method": random.choice(["card", "paypal", "apple_pay"]),
            "amount": round(random.uniform(1.0, 500.0), 2),
            "timestamp": now,
        }
    elif topic == "transaction_stream_anonymized":
        return {
            "hash_user": _random_id("h", 16),
            "amount_bucket": random.choice(["0-10", "10-50", "50-100", ">100"]),
            "timestamp": now,
        }
    elif topic == "transaction_stream_blacklisted":
        return {
            "transaction_id": _random_id("tx"),
            "user_id": random.randint(1, 10000),
            "reason": random.choice(["stolen_card", "fraud_pattern", "velocity"]),
            "timestamp": now,
        }
    elif topic in {
        "user_transaction_summary",
        "user_transaction_summary_eur",
        "payment_method_totals",
        "product_purchase_counts",
    }:
        return {
            "user_id": random.randint(1, 10000),
            "total_amount": round(random.uniform(10.0, 5000.0), 2),
            "transaction_count": random.randint(1, 100),
            "avg_amount": round(random.uniform(1.0, 200.0), 2),
            "snapshot_date": datetime.utcnow().date().isoformat(),
            "snapshot_version": 1,
        }
    else:
        # Générique
        return {"id": _random_id("evt"), "topic": topic, "i": i, "timestamp": now}


def produce_messages(topic, num_messages, rate_per_sec):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG.get("bootstrap_servers", ["localhost:9092"]),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=5,
        acks="1",
    )
    logger.info(
        f"Démarrage de l'envoi: topic={topic}, messages={num_messages}, débit={rate_per_sec}/s"
    )

    interval = 1.0 / max(rate_per_sec, 1)
    sent = 0
    start = time.time()
    try:
        for i in range(num_messages):
            payload = _build_sample_payload(topic, i)
            producer.send(topic, payload)
            sent += 1
            if rate_per_sec > 0:
                time.sleep(interval)
        producer.flush()
    finally:
        producer.close()
    elapsed = time.time() - start
    logger.info(f"Terminé. Messages envoyés: {sent} en {elapsed:.2f}s (~{sent/max(elapsed,1e-6):.0f}/s)")


def main():
    parser = argparse.ArgumentParser(description="Kafka Producer configurable")
    parser.add_argument("--topic", required=True, help="Topic Kafka cible")
    parser.add_argument(
        "--messages", type=int, default=100, help="Nombre de messages à envoyer"
    )
    parser.add_argument(
        "--rate", type=int, default=50, help="Débit en messages/seconde"
    )

    args = parser.parse_args()
    produce_messages(args.topic, args.messages, args.rate)


if __name__ == "__main__":
    main()
