from setuptools import setup, find_packages

setup(
    name="telemetry_pipeline",
    version="0.1.0",
    description="Modular Kafka-KSQL-MongoDB telemetry pipeline",
    packages=find_packages(include=["pipeline", "pipeline.*"]),
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "telemetry-pipeline=pipeline.main:main"
        ]
    },
    install_requires=[
        "kafka-python>=2.0.2",
        "pymongo>=4.0",
        "requests>=2.28",
        "confluent-kafka==2.2.0",
        "tabulate"
    ],
    python_requires=">=3.8"
)
