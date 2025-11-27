from kafka import KafkaConsumer

bootstrap = "77.81.230.104:9092"

try:
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        security_protocol="PLAINTEXT",
        request_timeout_ms=5000,
        api_version_auto_timeout_ms=5000,
    )

    print("Підключення до Kafka успішне.")
    print("Список топіків:")

    topics = consumer.topics()
    for t in topics:
        print(" -", t)

except Exception as e:
    print("Помилка при підключенні до Kafka:")
    print(e)
