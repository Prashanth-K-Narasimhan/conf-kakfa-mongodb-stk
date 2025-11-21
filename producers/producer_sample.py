from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

messages = [
    {
        "did":"981",
        "timestamp":1761559635000,
        "name":"odometer",
        "value_type":"0",
        "float_value":101.50,
        "string_value":"",
        "int_value":"0",
        "eventTime":"2025-10-27T10:07:15.000Z",
        "receivedTime":"2025-10-27T10:07:15.875Z"
    },
    {
        "did":"981",
        "timestamp":1761559635234,
        "name":"soc",
        "value_type":"1",
        "float_value":0,
        "string_value":"",
        "int_value":100,
        "eventTime":"2025-10-27T10:07:15.234Z",
        "receivedTime":"2025-10-27T10:07:16.875Z"
    },
    {
        "did":"981",
        "timestamp":1761559636061,
        "name":"speed",
        "value_type":"1",
        "float_value":0,
        "string_value":"",
        "int_value":25,
        "eventTime":"2025-10-27T10:07:16.061Z",
        "receivedTime":"2025-10-27T10:07:16.567Z"
    },
    {
        "did":"981",
        "timestamp":1761559636985,
        "name":"ignition_status",
        "value_type":"1",
        "float_value":0,
        "string_value":"",
        "int_value":1,
        "eventTime":"2025-10-27T10:07:16.985Z",
        "receivedTime":"2025-10-27T10:07:17.324Z"
    }
]

for msg in messages:
    producer.send("telemetry", key=msg["did"], value=msg)

producer.flush()
print("Sent!")
