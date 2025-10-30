FILENAME = "docker-compose.yaml"

CLIENTS = 2
YEAR_FILTERS = 3
HOUR_FILTERS = 4
AMOUNT_FILTERS = 2
Q2_CATEGORIZERS = 3
Q3_CATEGORIZERS = 2 # NOTE: This will always be <= Q3_SEMESTERS Count
Q4_CATEGORIZERS = 3
BIRTHDAY_MAPPERS = 3

Q3_SEMESTERS = [
    "semester.2024-1",
    # "semester.2024-2",
    # "semester.2023-2",
    "semester.2025-1",
]
TARGET_Q3_CATEGORIZERS = min(len(Q3_SEMESTERS), Q3_CATEGORIZERS)

TAB = "  "

def generate_compose_file():
    with open(FILENAME, "w") as f:
        def wln(level, text):
            f.write(f"{TAB * level}{text}\n")

        # HEADER
        wln(0, 'version: "3.9"')
        f.write("\n")
        wln(0, "services:")

        # ====================
        # Gateway
        # ====================
        wln(1, "gateway:")
        wln(2, "build:")
        wln(3, "context: .")
        wln(3, "dockerfile: gateway/Dockerfile")
        wln(2, "container_name: gateway")
        wln(2, "environment:")
        wln(3, f"- QUERY_1_TOTAL_WORKERS={AMOUNT_FILTERS}")
        wln(3, f"- QUERY_2_TOTAL_WORKERS={Q2_CATEGORIZERS}")
        wln(3, f"- QUERY_3_TOTAL_WORKERS={TARGET_Q3_CATEGORIZERS}")
        wln(3, f"- QUERY_4_TOTAL_WORKERS={1}")
        wln(3, f"- NUMBER_OF_YEAR_WORKERS={YEAR_FILTERS}")
        wln(3, f"- NUMBER_OF_BIRTHDAY_MAPPERS={BIRTHDAY_MAPPERS}")
        wln(2, "ports:")
        wln(3, '- "5000:5000"')
        wln(2, "depends_on:")
        wln(3, "- rabbitmq_server")
        wln(2, "volumes:")
        wln(3, "- ./data/received:/app/data/received")
        f.write("\n")

        # ====================
        # RabbitMQ
        # ====================
        wln(1, "rabbitmq_server:")
        wln(2, "image: rabbitmq:3-management")
        wln(2, "container_name: rabbitmq_server")
        wln(2, "ports:")
        wln(3, '- "5672:5672"')
        wln(3, '- "15672:15672"')
        wln(2, "healthcheck:")
        wln(3, 'test: ["CMD", "rabbitmq_server-diagnostics", "ping"]')
        wln(3, "interval: 30s")
        wln(3, "timeout: 10s")
        wln(3, "retries: 5")
        f.write("\n")

        # ====================
        # Clients
        # ====================
        for i in range(1, CLIENTS + 1):
            wln(1, f"client_{i}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: client/Dockerfile")
            wln(2, f"container_name: client_{i}")
            wln(2, "environment:")
            wln(3, f"- CLIENT_ID=client_{i}")
            wln(2, "depends_on:")
            wln(3, "- gateway")
            wln(2, "volumes:")
            wln(3, "- ./data/input:/data/input")
            f.write("\n")

        # ====================
        # Filter by Year
        # ====================
        for i in range(YEAR_FILTERS):
            wln(1, f"filter_by_year_worker_{i}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: workers/filter_by_year/Dockerfile")
            wln(2, f"container_name: filter_by_year_worker_{i}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, "- RECEIVER_EXCHANGE_T=filter_by_year_transactions_exchange")
            wln(3, "- RECEIVER_EXCHANGE_T_ITEMS=filter_by_year_transaction_items_exchange")
            wln(3, f"- WORKER_INDEX={i}")
            wln(3, "- HOUR_FILTER_EXCHANGE=filter_by_hour_exchange")
            wln(3, f"- NUMBER_OF_HOUR_WORKERS={HOUR_FILTERS}")
            wln(3, f"- NUMBER_OF_YEAR_WORKERS={YEAR_FILTERS}")
            wln(3, "- ITEM_CATEGORIZER_QUEUE=categorizer_q2_receiver_queue")
            wln(3, "- STORE_USER_CATEGORIZER_QUEUE=store_user_categorizer_queue")
            wln(3, "- FILTER_YEAR=2024,2025")
            wln(3, "- TOPIC_EXCHANGE=categorizer_q2_topic_exchange")
            wln(3, "- FANOUT_EXCHANGE=categorizer_q2_fanout_exchange")
            wln(3, "- CATEGORIZER_Q4_TOPIC_EXCHANGE=categorizer_q4_topic_exchange")
            wln(3, "- CATEGORIZER_Q4_FANOUT_EXCHANGE=categorizer_q4_fanout_exchange")
            wln(3, f"- CATEGORIZER_Q4_WORKERS={Q4_CATEGORIZERS}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            f.write("\n")

        # ====================
        # Filter by Hour
        # ====================
        for i in range(HOUR_FILTERS):
            wln(1, f"filter_by_hour_worker_{i}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: workers/filter_by_hour/Dockerfile")
            wln(2, f"container_name: filter_by_hour_worker_{i}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, "- RECEIVER_EXCHANGE=filter_by_hour_exchange")
            wln(3, f"- WORKER_INDEX={i}")
            wln(3, "- START_HOUR=6")
            wln(3, "- END_HOUR=22")
            wln(3, "- FILTER_BY_AMOUNT_EXCHANGE=filter_by_amount_exchange")
            wln(3, "- CATEGORIZER_Q3_EXCHANGE=categorizer_q3_exchange")
            wln(3, "- CATEGORIZER_Q3_FANOUT_EXCHANGE=categorizer_q3_fanout_exchange")
            wln(3, f"- NUMBER_OF_AMOUNT_WORKERS={AMOUNT_FILTERS}")
            wln(3, f"- NUMBER_OF_HOUR_WORKERS={HOUR_FILTERS}")
            wln(3, f"- NUMBER_OF_YEAR_WORKERS={YEAR_FILTERS}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            f.write("\n")

        # ====================
        # Filter by Amount
        # ====================
        for i in range(AMOUNT_FILTERS):
            wln(1, f"filter_by_amount_worker_{i}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: workers/filter_by_amount/Dockerfile")
            wln(2, f"container_name: filter_by_amount_worker_{i}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, "- RECEIVER_EXCHANGE=filter_by_amount_exchange")
            wln(3, f"- WORKER_INDEX={i}")
            wln(3, "- MIN_AMOUNT=75.0")
            wln(3, "- RESULT_QUEUE=query1_result_receiver_queue")
            wln(3, f"- NUMBER_OF_HOUR_WORKERS={HOUR_FILTERS}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            f.write("\n")

        # ====================
        # Categorizer Q2
        # ====================
        for i in range(Q2_CATEGORIZERS):
            wln(1, f"categorizer_q2_worker_{i + 1}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: workers/categorizer_q2/Dockerfile")
            wln(2, f"container_name: categorizer_q2_worker_{i + 1}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, "- ITEMS_QUEUE=categorizer_q2_items_queue")
            wln(3, f"- RECEIVER_QUEUE=categorizer_q2_receiver_queue_{i + 1}")
            wln(3, "- GATEWAY_QUEUE=query2_result_receiver_queue")
            wln(3, "- TOPIC_EXCHANGE=categorizer_q2_topic_exchange")
            wln(3, "- FANOUT_EXCHANGE=categorizer_q2_fanout_exchange")
            wln(3, "- ITEMS_FANOUT_EXCHANGE=categorizer_q2_items_fanout_exchange")
            wln(3, f"- WORKER_INDEX={i}")
            wln(3, f"- TOTAL_WORKERS={Q2_CATEGORIZERS}")
            wln(3, f"- NUMBER_OF_YEAR_WORKERS={YEAR_FILTERS}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            wln(3, "- gateway")
            f.write("\n")

        # ====================
        # Categorizer Q3
        # ====================
        for i in range(TARGET_Q3_CATEGORIZERS):
            wln(1, f"categorizer_q3_worker_{i + 1}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: workers/categorizer_q3/Dockerfile")
            wln(2, f"container_name: categorizer_q3_worker_{i + 1}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, "- RECEIVER_EXCHANGE=categorizer_q3_exchange")
            wln(3, "- FANOUT_EXCHANGE=categorizer_q3_fanout_exchange")
            wln(3, f"- WORKER_INDEX={i}")
            wln(3, "- GATEWAY_QUEUE=query3_result_receiver_queue")
            wln(3, f"- NUMBER_OF_HOUR_WORKERS={HOUR_FILTERS}")
            assigned_semesters = ",".join(semester for s_i, semester in enumerate(Q3_SEMESTERS) if s_i % TARGET_Q3_CATEGORIZERS == i)
            wln(3, f"- ASSIGNED_SEMESTERS={assigned_semesters}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            wln(2, "volumes:")
            wln(3, "- ./data/received:/app/data/received")
            f.write("\n")

        # ====================
        # Birthday Dictionary Workers
        # ====================
        for i in range(BIRTHDAY_MAPPERS):
            wln(1, f"birthday_dictionary_worker_{i}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: ./workers/birthday_dictionary/Dockerfile")
            wln(2, f"container_name: birthday_dictionary_worker_{i}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, f"- RECEIVER_QUEUE=birthday_dictionary_worker_{i}_queue")
            wln(3, "- RECEIVER_EXCHANGE=birthday_dictionary_exchange")
            wln(3, "- GATEWAY_REQUEST_QUEUE=birthday_dictionary_client_request_queue")
            wln(3, "- GATEWAY_CLIENT_DATA_QUEUE=gateway_client_data_queue")
            wln(3, "- QUERY4_ANSWER_QUEUE=query4_answer_queue")
            wln(3, f"- AMOUNT_OF_WORKERS={BIRTHDAY_MAPPERS}")
            wln(3, f"- CATEGORIZER_Q4_WORKERS={Q4_CATEGORIZERS}")
            wln(3, f"- WORKER_INDEX={i}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            wln(3, "- gateway")
            f.write("\n")

        # ====================
        # Categorizer Q4
        # ====================
        for i in range(Q4_CATEGORIZERS):
            wln(1, f"categorizer_q4_worker_{i + 1}:")
            wln(2, "build:")
            wln(3, "context: .")
            wln(3, "dockerfile: ./workers/categorizer_q4/Dockerfile")
            wln(2, f"container_name: categorizer_q4_worker_{i + 1}")
            wln(2, "environment:")
            wln(3, "- RABBITMQ_HOST=rabbitmq_server")
            wln(3, f"- RECEIVER_QUEUE=store_user_categorizer_queue_{i + 1}")
            wln(3, "- TOPIC_EXCHANGE=categorizer_q4_topic_exchange")
            wln(3, "- FANOUT_EXCHANGE=categorizer_q4_fanout_exchange")
            wln(3, f"- WORKER_INDEX={i}")
            wln(3, f"- AMOUNT_OF_WORKERS={Q4_CATEGORIZERS}")
            wln(3, f"- BIRTHDAY_MAPPERS={BIRTHDAY_MAPPERS}")
            wln(3, "- BIRTHDAY_DICT_QUEUE=birthday_dictionary_queue")
            wln(3, f"- NUMBER_OF_YEAR_WORKERS={YEAR_FILTERS}")
            wln(2, "depends_on:")
            wln(3, "- rabbitmq_server")
            wln(3, "- gateway")
            f.write("\n")


if __name__ == "__main__":
    generate_compose_file()
    print(f"Archivo '{FILENAME}' generado correctamente.")
