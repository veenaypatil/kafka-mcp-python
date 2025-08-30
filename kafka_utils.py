import json
import os
import re
import subprocess
import time
import threading
import csv

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, ConfigResource, ConfigResourceType

# Global cache for topic consumers with thread safety
_topic_consumers_cache = {
    'data': None,  # Will store the topic_consumers map
    'last_updated': 0,
    'is_building': False,
    'lock': threading.Lock()
}

# Cache configuration
CACHE_TTL_SECONDS = int(os.getenv("TOPIC_CONSUMERS_CACHE_TTL", "600"))  # Default 10 minutes


def is_uuid_consumer_group(group_id: str) -> bool:
    """Check if consumer group has UUID suffix or is entirely a UUID (to ignore)"""
    # Pattern for UUID: 8-4-4-4-12 hexadecimal digits
    uuid_pattern = r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'

    # Pattern for UUID suffix (like your original example)
    uuid_suffix_pattern = r'-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$'

    return bool(re.search(uuid_pattern, group_id, re.IGNORECASE) or
                re.search(uuid_suffix_pattern, group_id, re.IGNORECASE))


def build_topic_consumers_cache(bootstrap_servers: str) -> dict:
    """
    Build the topic consumers map. This is the expensive operation.
    Returns a dict mapping topic -> [list of consumer groups]
    """
    admin_client = KafkaAdminClient(
        bootstrap_servers=[bootstrap_servers],
        client_id='mcp-cache-builder-client'
    )

    try:
        # Get all consumer groups (excluding UUID ones)
        group_list = admin_client.list_consumer_groups()
        group_ids = [g[0] for g in group_list if not is_uuid_consumer_group(g[0])]

        topic_consumers = {}  # Map of topic -> list of consumer groups

        for group_id in group_ids:
            try:
                offsets = admin_client.list_consumer_group_offsets(group_id)
                for tp in offsets.keys():
                    if tp.topic not in topic_consumers:
                        topic_consumers[tp.topic] = []
                    if group_id not in topic_consumers[tp.topic]:  # Avoid duplicates
                        topic_consumers[tp.topic].append(group_id)
            except Exception:
                # Skip groups we can't get offsets for
                continue

        return topic_consumers

    finally:
        admin_client.close()


def get_cached_topic_consumers(bootstrap_servers: str) -> tuple[dict, bool, str]:
    """
    Get topic consumers from cache or build it if needed.
    Returns: (topic_consumers_map, is_from_cache, status_message)
    """
    global _topic_consumers_cache

    current_time = time.time()

    with _topic_consumers_cache['lock']:
        # Check if we have valid cached data
        if (_topic_consumers_cache['data'] is not None and
                (current_time - _topic_consumers_cache['last_updated']) < CACHE_TTL_SECONDS):
            return _topic_consumers_cache['data'], True, "Served from cache"

        # Check if another thread is already building the cache
        if _topic_consumers_cache['is_building']:
            # Wait for the other thread to finish (with timeout)
            pass  # For now, proceed to build anyway to avoid complex waiting logic

        # Mark that we're building the cache
        _topic_consumers_cache['is_building'] = True

    try:
        # Build the cache (this is the expensive operation)
        topic_consumers = build_topic_consumers_cache(bootstrap_servers)

        # Update the cache
        with _topic_consumers_cache['lock']:
            _topic_consumers_cache['data'] = topic_consumers
            _topic_consumers_cache['last_updated'] = current_time
            _topic_consumers_cache['is_building'] = False

        return topic_consumers, False, "Cache built successfully"

    except Exception as e:
        with _topic_consumers_cache['lock']:
            _topic_consumers_cache['is_building'] = False
        raise e


def clear_cache():
    """Clear the topic consumers cache"""
    global _topic_consumers_cache

    with _topic_consumers_cache['lock']:
        _topic_consumers_cache['data'] = None
        _topic_consumers_cache['last_updated'] = 0
        _topic_consumers_cache['is_building'] = False


def get_cache_info() -> dict:
    """Get cache status information"""
    global _topic_consumers_cache

    current_time = time.time()

    with _topic_consumers_cache['lock']:
        cache_age = int(current_time - _topic_consumers_cache['last_updated']) if _topic_consumers_cache[
                                                                                      'last_updated'] > 0 else None
        is_valid = (_topic_consumers_cache['data'] is not None and
                    cache_age is not None and cache_age < CACHE_TTL_SECONDS)

        total_topics = len(_topic_consumers_cache['data']) if _topic_consumers_cache['data'] else 0
        total_mappings = sum(len(consumers) for consumers in _topic_consumers_cache['data'].values()) if \
        _topic_consumers_cache['data'] else 0

        return {
            "cache_exists": _topic_consumers_cache['data'] is not None,
            "cache_valid": is_valid,
            "cache_building": _topic_consumers_cache['is_building'],
            "cache_age_seconds": cache_age,
            "cache_ttl_seconds": CACHE_TTL_SECONDS,
            "total_topics_cached": total_topics,
            "total_topic_consumer_mappings": total_mappings,
            "last_updated": _topic_consumers_cache['last_updated'],
            "status": "Valid" if is_valid else (
                "Building" if _topic_consumers_cache['is_building'] else "Expired/Empty")
        }


def consume_kafka_messages(topic: str, max_messages: int, offset_strategy: str, timestamp: int, bootstrap_servers: str):
    """Helper function to consume messages from Kafka"""
    # Determine auto_offset_reset based on strategy
    if offset_strategy == "latest":
        auto_offset_reset = 'latest'
    elif offset_strategy == "earliest":
        auto_offset_reset = 'earliest'
    elif offset_strategy == "timestamp":
        if timestamp is None:
            raise ValueError("timestamp parameter is required when using 'timestamp' offset strategy")
        auto_offset_reset = 'earliest'  # We'll seek to timestamp manually
    else:
        raise ValueError(f"Invalid offset_strategy '{offset_strategy}'. Use 'latest', 'earliest', or 'timestamp'")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
        group_id=None,  # Use None to avoid consumer group management
        consumer_timeout_ms=10000,  # 10 second timeout
        value_deserializer=lambda x: x.decode('utf-8') if x else None
    )

    # Handle timestamp-based seeking
    if offset_strategy == "timestamp":
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            topic_partitions = [TopicPartition(topic, p) for p in partitions]
            consumer.assign(topic_partitions)

            # Seek to timestamp
            timestamp_dict = {tp: timestamp for tp in topic_partitions}
            offsets = consumer.offsets_for_times(timestamp_dict)

            for tp, offset_and_timestamp in offsets.items():
                if offset_and_timestamp is not None:
                    consumer.seek(tp, offset_and_timestamp.offset)

    messages = []
    for i, message in enumerate(consumer):
        if i >= max_messages:
            break
        messages.append({
            'offset': message.offset,
            'partition': message.partition,
            'value': message.value,
            'timestamp': message.timestamp,
            'key': message.key.decode('utf-8') if message.key else None
        })

    consumer.close()
    return messages


def produce_kafka_message(topic: str, message: str, bootstrap_servers: str):
    """Helper function to produce a message to Kafka"""
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: x.encode('utf-8')
    )

    future = producer.send(topic, message)
    result = future.get(timeout=10)
    producer.close()

    return result


def get_topic_partition_count(topic: str, bootstrap_servers: str):
    """Helper function to get partition count for a topic"""
    consumer = KafkaConsumer(bootstrap_servers=[bootstrap_servers])
    partitions = consumer.partitions_for_topic(topic)
    consumer.close()

    if not partitions:
        return None
    return len(partitions)


def count_topic_messages(topic: str, hours: int, bootstrap_servers: str):
    """Helper function to count messages in a topic for the last X hours"""
    # Calculate timestamp for X hours ago (in milliseconds)
    current_time_ms = int(time.time() * 1000)
    hours_ago_ms = current_time_ms - (hours * 60 * 60 * 1000)

    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_servers],
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=5000,
        value_deserializer=None  # We don't need to deserialize values for counting
    )

    # Get all partitions for the topic
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return None

    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)

    # Get offsets for the timestamp (X hours ago)
    timestamp_dict = {tp: hours_ago_ms for tp in topic_partitions}
    start_offsets = consumer.offsets_for_times(timestamp_dict)

    # Get latest offsets for all partitions
    end_offsets = consumer.end_offsets(topic_partitions)

    total_messages = 0
    partition_details = []

    for tp in topic_partitions:
        start_offset_info = start_offsets.get(tp)
        end_offset = end_offsets.get(tp, 0)

        if start_offset_info is not None:
            start_offset = start_offset_info.offset
        else:
            # If no messages found for the timestamp, start from beginning
            start_offset = 0

        messages_in_partition = max(0, end_offset - start_offset)
        total_messages += messages_in_partition

        partition_details.append({
            'partition': tp.partition,
            'start_offset': start_offset,
            'end_offset': end_offset,
            'message_count': messages_in_partition
        })

    consumer.close()

    return {
        'total_messages': total_messages,
        'partition_details': partition_details,
        'start_timestamp': hours_ago_ms,
        'end_timestamp': current_time_ms
    }


def calculate_topic_size(topic: str, bootstrap_servers: str):
    """Helper function to calculate topic size using kafka-log-dirs"""
    # FIXME: For now Using kafka-log-dirs CLI command, fix this using admin client log dirs output
    # admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    #
    # cluster_metadata = admin_client.describe_cluster()
    #
    # # Get log directories information
    # log_dirs_result = admin_client.describe_log_dirs()


    cmd = [
        "kafka-log-dirs",
        "--bootstrap-server", bootstrap_servers,
        "--describe",
        "--topic-list", topic
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=30,
        check=True
    )

    # Skip the first 2 lines to get to the JSON content
    output_lines = result.stdout.strip().splitlines()

    if len(output_lines) < 3:
        raise ValueError("kafka-log-dirs output is too short to contain JSON data.")

    # Join the lines from the 3rd line onwards
    json_output = "\n".join(output_lines[2:])

    total_size = 0
    partition_sizes = {}

    # Parse the JSON output
    data = json.loads(json_output)

    # Navigate through the JSON structure
    if "brokers" in data:
        for broker in data["brokers"]:
            if "logDirs" in broker:
                for log_dir in broker["logDirs"]:
                    if "partitions" in log_dir:
                        for partition_info in log_dir["partitions"]:
                            partition_name = partition_info.get("partition", "")

                            # Check if this partition belongs to our topic
                            if partition_name.startswith(f"{topic}-"):
                                size = partition_info.get("size", 0)
                                total_size += size

                                # Extract partition number from partition name
                                try:
                                    partition_id = int(partition_name.split("-")[-1])
                                    if partition_id not in partition_sizes:
                                        partition_sizes[partition_id] = 0
                                    partition_sizes[partition_id] += size
                                except (ValueError, IndexError):
                                    if partition_name not in partition_sizes:
                                        partition_sizes[partition_name] = 0
                                    partition_sizes[partition_name] += size

    return {
        'total_size': total_size,
        'partition_sizes': partition_sizes
    }


def describe_kafka_consumer_group(group_id: str, bootstrap_servers: str):
    """Helper function to describe a consumer group"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=[bootstrap_servers],
        client_id='mcp-admin-client'
    )

    try:
        # Get consumer group description
        group_descriptions = admin_client.describe_consumer_groups([group_id])
        if not group_descriptions or group_id not in group_descriptions[0].group:
            return None
        group_info = group_descriptions[0]

        # Get committed offsets
        try:
            committed_offsets = admin_client.list_consumer_group_offsets(group_id)
        except Exception:
            committed_offsets = {}

        group_offsets = {}
        if committed_offsets:
            consumer = KafkaConsumer(bootstrap_servers=[bootstrap_servers])
            end_offsets = consumer.end_offsets(list(committed_offsets.keys()))
            consumer.close()

            # Group offsets by topic
            topic_partitions_map = {}
            for tp, offset_metadata in committed_offsets.items():
                if tp.topic not in topic_partitions_map:
                    topic_partitions_map[tp.topic] = []
                topic_partitions_map[tp.topic].append((tp, offset_metadata))

            for topic, tps_with_offsets in topic_partitions_map.items():
                partition_details = []
                total_lag = 0
                for tp, offset_metadata in tps_with_offsets:
                    # Extract the actual offset from OffsetAndMetadata object
                    committed_offset = offset_metadata.offset if hasattr(offset_metadata, 'offset') else offset_metadata
                    end_offset = end_offsets.get(tp, 0)
                    lag = max(0, end_offset - committed_offset)
                    total_lag += lag
                    partition_details.append({
                        'partition': tp.partition,
                        'committed_offset': committed_offset,
                        'log_end_offset': end_offset,
                        'lag': lag
                    })

                if partition_details:
                    group_offsets[topic] = {
                        'partition_count': len(partition_details),
                        'total_lag': total_lag,
                        'partitions': sorted(partition_details, key=lambda p: p['partition'])
                    }

        # Prepare member information with defensive attribute access
        members_info = []
        if group_info.members:
            for member in group_info.members:
                member_info = {
                    'member_id': getattr(member, 'member_id', 'unknown'),
                    'client_id': getattr(member, 'client_id', 'unknown'),
                    'host': getattr(member, 'host', 'unknown'),
                }

                # Handle assignment count safely
                if hasattr(member, 'assignment') and member.assignment:
                    if hasattr(member.assignment, 'topic_partitions'):
                        member_info['assignment_count'] = len(member.assignment.topic_partitions)
                    else:
                        member_info['assignment_count'] = 0
                else:
                    member_info['assignment_count'] = 0

                members_info.append(member_info)

        return {
            'group_info': group_info,
            'group_offsets': group_offsets,
            'members_info': members_info
        }

    finally:
        admin_client.close()


def calculate_consumer_group_lag(group_id: str, bootstrap_servers: str):
    """Helper function to calculate consumer group lag"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=[bootstrap_servers],
        client_id='mcp-lag-client'
    )

    try:
        # Verify consumer group exists
        group_descriptions = admin_client.describe_consumer_groups([group_id])
        if group_id not in group_descriptions:
            return None

        group_info = group_descriptions[group_id]

        consumer = KafkaConsumer(
            bootstrap_servers=[bootstrap_servers],
            group_id=group_id,
            enable_auto_commit=False
        )

        # Get all topics to check which ones this group consumes
        all_topics = consumer.topics()

        total_lag = 0
        topic_lags = {}
        partition_details = []
        topics_with_lag = 0

        for topic in all_topics:
            partitions = consumer.partitions_for_topic(topic)
            if partitions:
                topic_partitions = [TopicPartition(topic, p) for p in partitions]
                try:
                    committed_offsets = consumer.committed_offsets(topic_partitions)

                    # Only process topics where this group has committed offsets
                    topic_has_offsets = any(offset is not None for offset in committed_offsets.values())

                    if topic_has_offsets:
                        end_offsets = consumer.end_offsets(topic_partitions)
                        topic_lag = 0
                        topic_partition_details = []

                        for tp in topic_partitions:
                            committed_offset = committed_offsets.get(tp)
                            end_offset = end_offsets.get(tp, 0)

                            if committed_offset is not None:
                                lag = max(0, end_offset - committed_offset)
                                topic_lag += lag
                                total_lag += lag

                                topic_partition_details.append({
                                    'partition': tp.partition,
                                    'committed_offset': committed_offset,
                                    'log_end_offset': end_offset,
                                    'lag': lag
                                })

                        if topic_lag > 0:
                            topics_with_lag += 1
                            topic_lags[topic] = {
                                'total_lag': topic_lag,
                                'partition_count': len(topic_partition_details),
                                'partitions': topic_partition_details
                            }
                            partition_details.extend([{
                                'topic': topic,
                                'partition': p['partition'],
                                'lag': p['lag'],
                                'committed_offset': p['committed_offset'],
                                'log_end_offset': p['log_end_offset']
                            } for p in topic_partition_details])

                except Exception:
                    # Skip topics we can't get offsets for
                    continue

        consumer.close()

        # Sort topics by lag (highest first)
        sorted_topic_lags = dict(sorted(topic_lags.items(), key=lambda x: x[1]['total_lag'], reverse=True))

        # Sort partition details by lag (highest first)
        sorted_partition_details = sorted(partition_details, key=lambda x: x['lag'], reverse=True)

        return {
            'group_info': group_info,
            'total_lag': total_lag,
            'topics_with_lag': topics_with_lag,
            'topic_lags': sorted_topic_lags,
            'partition_details': sorted_partition_details
        }

    finally:
        admin_client.close()
        consumer.close()


def describe_kafka_topic(topic: str, bootstrap_servers: str):
    """Helper function to describe a topic with comprehensive information"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=[bootstrap_servers],
        client_id='mcp-topic-describe-client'
    )

    try:
        # Get topic metadata
        metadata = admin_client.describe_topics([topic])
        if metadata is None:
            return None

        topic_metadata = metadata[0]

        # Get topic configuration
        try:
            configs = admin_client.describe_configs(config_resources=[ConfigResource(ConfigResourceType.TOPIC, topic)])
            topic_config = {}
            for c in configs[0].resources[0][4]:
                config_name = c[0]
                config_value = c[1]
                topic_config[config_name] = config_value
        except Exception:
            return None

        # Extract important configuration values with safe access
        retention_ms = topic_config.get('retention.ms', 'Not set')
        segment_ms = topic_config.get('segment.ms', 'Not set')
        cleanup_policy = topic_config.get('cleanup.policy', 'Not set')

        # Convert retention to human readable format
        retention_readable = "Not set"
        if retention_ms != 'Not set':
            try:
                retention_value = retention_ms.value if hasattr(retention_ms, 'value') else str(retention_ms)
                if retention_value != '-1':
                    retention_hours = int(retention_value) / (1000 * 60 * 60)
                    retention_days = retention_hours / 24
                    if retention_days >= 1:
                        retention_readable = f"{retention_days:.1f} days ({retention_hours:.1f} hours)"
                    else:
                        retention_readable = f"{retention_hours:.1f} hours"
                else:
                    retention_readable = "Infinite (log compacted)"
            except (ValueError, AttributeError):
                retention_readable = str(retention_ms)

        # Build partition details table
        partition_details = []
        replica_factor = 0

        for partition in topic_metadata.get('partitions'):
            partition_info = {
                'partition_id': partition['partition'],
                'leader': partition['leader'],
                'replicas': sorted(partition['replicas']),
                'isr': sorted(partition['isr']),
                'replica_count': len(partition['replicas'])
            }
            partition_details.append(partition_info)

            # Get replication factor from first partition
            if replica_factor == 0:
                replica_factor = len(partition['replicas'])

        # Get topic size (handle failures gracefully)
        try:
            size_data = calculate_topic_size(topic, bootstrap_servers)
            total_size = size_data['total_size']
            size_mb = total_size / (1024 * 1024)
            size_gb = total_size / (1024 * 1024 * 1024)
        except Exception:
            total_size = 0
            size_mb = 0
            size_gb = 0

        return {
            'topic_name': topic,
            'partition_count': len(partition_details),
            'replication_factor': replica_factor,
            'retention_policy': {
                'retention_ms': retention_ms.value if hasattr(retention_ms, 'value') else str(retention_ms),
                'retention_readable': retention_readable,
                'cleanup_policy': cleanup_policy.value if hasattr(cleanup_policy, 'value') else str(cleanup_policy),
                'segment_ms': segment_ms.value if hasattr(segment_ms, 'value') else str(segment_ms)
            },
            'disk_usage': {
                'total_size_bytes': total_size,
                'total_size_mb': round(size_mb, 2),
                'total_size_gb': round(size_gb, 3)
            },
            'partition_details': sorted(partition_details, key=lambda x: x['partition_id'])
        }

    finally:
        admin_client.close()


def generate_topic_consumers_csv_report(bootstrap_servers: str, output_file_path: str = None) -> dict:
    """
    Generate a CSV report of all topics and their consumers using cached data.

    Args:
        bootstrap_servers: Kafka bootstrap servers
        output_file_path: Path where to save the CSV file (optional, defaults to Downloads directory)

    Returns:
        Dict with report details and file path
    """
    # Get cached data (will build cache if needed)
    topic_consumers, is_from_cache, cache_status = get_cached_topic_consumers(bootstrap_servers)

    # Set default output path if not provided or if it's just a filename
    if output_file_path is None or not os.path.dirname(output_file_path):
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        downloads_dir = os.path.expanduser("~/Downloads")

        if output_file_path and not os.path.dirname(output_file_path):
            # If user provided just a filename, use it in Downloads
            filename = output_file_path if output_file_path.endswith('.csv') else f"{output_file_path}.csv"
        else:
            # Default filename with timestamp
            filename = f"topic_consumers_report_{timestamp}.csv"

        output_file_path = os.path.join(downloads_dir, filename)
    else:
        # User provided full path, ensure directory exists
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

    # Prepare data for CSV
    csv_data = []
    total_topics = 0
    total_consumer_groups = 0
    topics_with_consumers = 0

    for topic_name, consumer_groups in topic_consumers.items():
        total_topics += 1
        consumer_count = len(consumer_groups)
        total_consumer_groups += consumer_count

        if consumer_count > 0:
            topics_with_consumers += 1

        # Join consumer groups with semicolon for CSV
        consumer_groups_str = "; ".join(consumer_groups) if consumer_groups else "No consumers"

        csv_data.append({
            'Topic Name': topic_name,
            'Number of Consumers': consumer_count,
            'Consumer Groups List': consumer_groups_str
        })

    # Sort by topic name for consistent output
    csv_data.sort(key=lambda x: x['Topic Name'])

    # Write to CSV file
    with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['Topic Name', 'Number of Consumers', 'Consumer Groups List']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header
        writer.writeheader()

        # Write data rows
        for row in csv_data:
            writer.writerow(row)

    # Generate summary stats
    cache_info = get_cache_info()

    return {
        'file_path': output_file_path,
        'total_topics': total_topics,
        'topics_with_consumers': topics_with_consumers,
        'topics_without_consumers': total_topics - topics_with_consumers,
        'total_consumer_groups': total_consumer_groups,
        'average_consumers_per_topic': round(total_consumer_groups / total_topics, 2) if total_topics > 0 else 0,
        'report_generated_at': time.strftime("%Y-%m-%d %H:%M:%S"),
        'cache_info': {
            'served_from_cache': is_from_cache,
            'cache_status': cache_status,
            'cache_age_seconds': cache_info['cache_age_seconds'],
            'cache_valid': cache_info['cache_valid']
        }
    }