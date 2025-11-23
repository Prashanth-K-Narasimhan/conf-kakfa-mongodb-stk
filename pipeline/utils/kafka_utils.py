from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from pipeline.config import KAFKA_BOOTSTRAP

def ensure_topics(topic_names):
    """
    Creates topics if they don't exist. 
    """
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    existing = admin.list_topics()
    
    to_create = []
    for t in topic_names:
        if t not in existing:
            # Compact vehicle state, delete others
            cleanup = "compact" if "latest" in t else "delete"
            to_create.append(NewTopic(t, num_partitions=1, replication_factor=1, topic_configs={"cleanup.policy": cleanup}))
    
    if to_create:
        try:
            admin.create_topics(to_create)
            print(f"Created topics: {[t.name for t in to_create]}")
        except TopicAlreadyExistsError:
            # Race condition, whatever
            pass
        except Exception as e:
            print(f"Failed to create topics: {e}")
    
    admin.close()