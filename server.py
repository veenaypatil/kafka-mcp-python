import json
import os

from mcp.server.fastmcp import FastMCP

from kafka_utils import (
    get_cached_topic_consumers, clear_cache, get_cache_info,
    consume_kafka_messages, produce_kafka_message, get_topic_partition_count,
    count_topic_messages, calculate_topic_size, describe_kafka_consumer_group,
    calculate_consumer_group_lag, describe_kafka_topic, generate_topic_consumers_csv_report
)

# Initialize the FastMCP server
mcp = FastMCP("Kafka MCP Server")


@mcp.tool()
def consume_messages(topic: str, max_messages: int = 10, offset_strategy: str = "latest", timestamp: int = None,
                     bootstrap_servers: str = None) -> str:
    """
    Consume messages from a Kafka topic with flexible offset options.

    Args:
        topic: Kafka topic name
        max_messages: Number of messages to consume (default 10)
        offset_strategy: 'latest', 'earliest', or 'timestamp' (default 'latest')
        timestamp: Unix timestamp in milliseconds (required when offset_strategy='timestamp')
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        messages = consume_kafka_messages(topic, max_messages, offset_strategy, timestamp, bootstrap_servers)

        result = {
            'topic': topic,
            'offset_strategy': offset_strategy,
            'timestamp_used': timestamp if offset_strategy == "timestamp" else None,
            'messages_count': len(messages),
            'messages': messages
        }

        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error consuming messages: {str(e)}"


@mcp.tool()
def produce_message(topic: str, message: str, bootstrap_servers: str = None) -> str:
    """Produce a message to a Kafka topic"""
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        result = produce_kafka_message(topic, message, bootstrap_servers)
        return f"Message sent successfully to topic '{topic}'. Partition: {result.partition}, Offset: {result.offset}"
    except Exception as e:
        return f"Error producing message: {str(e)}"


@mcp.tool()
def get_topic_partitions(topic: str, bootstrap_servers: str = None) -> str:
    """Return only topic name and number of partitions."""
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    try:
        partition_count = get_topic_partition_count(topic, bootstrap_servers)
        if partition_count is None:
            return f"Topic '{topic}' not found"
        return json.dumps({
            "topic": topic,
            "partition_count": partition_count
        }, indent=2)
    except Exception as e:
        return f"Error getting partitions for topic '{topic}': {str(e)}"


@mcp.tool()
def count_messages_last_hours(topic: str, hours: int, bootstrap_servers: str = None) -> str:
    """
    Count the number of messages received in a topic in the last X hours.

    Args:
        topic: Kafka topic name
        hours: Number of hours to look back
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        result_data = count_topic_messages(topic, hours, bootstrap_servers)
        if result_data is None:
            return f"Topic '{topic}' not found"

        result = {
            'topic': topic,
            'hours': hours,
            'time_period_hours': hours,
            'start_timestamp': result_data['start_timestamp'],
            'end_timestamp': result_data['end_timestamp'],
            'total_message_count': result_data['total_messages'],
            'messagesInLastHours': result_data['total_messages'],
            'partition_details': result_data['partition_details'],
            'summary': f"{result_data['total_messages']} messages received in topic '{topic}' in last {hours} hour(s)"
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return f"Error counting messages for topic '{topic}': {str(e)}"


@mcp.tool()
def get_topic_size(topic: str, bootstrap_servers: str = None) -> str:
    """
    Calculate the total size of a Kafka topic using kafka-log-dirs CLI command.

    Args:
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        size_data = calculate_topic_size(topic, bootstrap_servers)

        # Convert to human-readable formats
        total_size = size_data['total_size']
        partition_sizes = size_data['partition_sizes']

        size_mb = total_size / (1024 * 1024)
        size_gb = total_size / (1024 * 1024 * 1024)

        # Sort partitions by ID for consistent output
        sorted_partitions = []
        for partition_id in sorted(partition_sizes.keys()):
            partition_mb = partition_sizes[partition_id] / (1024 * 1024)
            sorted_partitions.append({
                "partition": partition_id,
                "size_bytes": partition_sizes[partition_id],
                "size_mb": round(partition_mb, 2)
            })

        result = {
            "topic": topic,
            "total_size_bytes": total_size,
            "total_size_mb": round(size_mb, 2),
            "total_size_gb": round(size_gb, 3),
            "partition_count": len(partition_sizes),
            "partitions": sorted_partitions,
            "method": "kafka-log-dirs CLI",
            "summary": f"Topic '{topic}' total size: {total_size:,} bytes ({size_mb:.2f} MB, {size_gb:.3f} GB) across {len(partition_sizes)} partitions"
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({
            "topic": topic,
            "error": f"Failed to calculate topic size: {str(e)}"
        }, indent=2)


@mcp.tool()
def describe_consumer_group(group_id: str, bootstrap_servers: str = None) -> str:
    """
    Describe a consumer group with detailed offset information.

    Args:
        group_id: Consumer group ID
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        group_data = describe_kafka_consumer_group(group_id, bootstrap_servers)
        if group_data is None:
            return json.dumps({"error": f"Consumer group '{group_id}' not found or could not be described."}, indent=2)

        result = {
            'group_id': group_id,
            'state': group_data['group_info'].state,
            'protocol_type': group_data['group_info'].protocol_type,
            'protocol': group_data['group_info'].protocol,
            'members': {
                'count': len(group_data['members_info']),
                'details': group_data['members_info']
            },
            'topic_offsets': group_data['group_offsets'],
            'topics_consuming': sorted(list(group_data['group_offsets'].keys())),
            'total_topics': len(group_data['group_offsets'])
        }
        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps(
            {"error": f"An unexpected error occurred while describing consumer group '{group_id}': {str(e)}"}, indent=2)


@mcp.tool()
def get_consumer_group_lag(group_id: str, bootstrap_servers: str = None) -> str:
    """
    Calculate the total lag of a consumer group across all topics and partitions.

    Args:
        group_id: Consumer group ID
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        lag_data = calculate_consumer_group_lag(group_id, bootstrap_servers)
        if lag_data is None:
            return json.dumps({
                "group_id": group_id,
                "error": f"Consumer group '{group_id}' not found"
            }, indent=2)

        result = {
            "group_id": group_id,
            "group_state": lag_data['group_info'].state,
            "total_lag": lag_data['total_lag'],
            "topics_with_lag": lag_data['topics_with_lag'],
            "total_topics_consuming": len(lag_data['topic_lags']),
            "topic_lags": lag_data['topic_lags'],
            "highest_lag_partitions": lag_data['partition_details'][:10],  # Top 10 lagging partitions
            "summary": f"Consumer group '{group_id}' has total lag of {lag_data['total_lag']:,} messages across {lag_data['topics_with_lag']} topics",
            "lag_status": "HIGH" if lag_data['total_lag'] > 10000 else "MEDIUM" if lag_data[
                                                                                       'total_lag'] > 1000 else "LOW"
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({
            "group_id": group_id,
            "error": f"Failed to calculate consumer group lag: {str(e)}"
        }, indent=2)


@mcp.tool()
def get_topic_consumers(topic: str, bootstrap_servers: str = None, include_inactive: bool = False,
                        force_refresh: bool = False) -> str:
    """
    Get a list of consumer groups consuming from a specific topic using cached data for faster responses.

    Args:
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
        include_inactive: Whether to include inactive consumer groups (default False)
        force_refresh: Force refresh the cache (default False)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        # Clear cache if force refresh is requested
        if force_refresh:
            clear_cache()

        # Get topic consumers from cache
        topic_consumers, is_from_cache, cache_status = get_cached_topic_consumers(bootstrap_servers)

        # Get consumers for the requested topic
        consumers = topic_consumers.get(topic, [])

        # Prepare cache info for user
        cache_info = get_cache_info()
        cache_info.update({
            "served_from_cache": is_from_cache,
            "cache_status": cache_status
        })

        result = {
            "topic": topic,
            "consumer_groups": consumers,
            "consumer_count": len(consumers),
            "cache_info": cache_info,
            "summary": f"Topic '{topic}' has {len(consumers)} consumer group(s)"
        }

        if len(consumers) == 0:
            result["message"] = f"No consumer groups found for topic '{topic}'"

        if not is_from_cache:
            result[
                "performance_note"] = "First request took longer to build cache. Subsequent requests will be much faster!"

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({"error": f"An unexpected error occurred: {str(e)}"}, indent=2)


@mcp.tool()
def clear_topic_consumers_cache() -> str:
    """
    Clear the topic consumers cache to force a fresh rebuild on next request.
    """
    clear_cache()

    return json.dumps({
        "status": "success",
        "message": "Topic consumers cache cleared successfully",
        "note": "Next get_topic_consumers request will rebuild the cache"
    }, indent=2)


@mcp.tool()
def get_cache_status() -> str:
    """
    Get the current status of the topic consumers cache.
    """
    result = get_cache_info()
    return json.dumps(result, indent=2)


@mcp.tool()
def describe_topic(topic: str, bootstrap_servers: str = None) -> str:
    """
    Describe a Kafka topic with comprehensive information including partitions, retention, and disk usage.

    Args:
        topic: Kafka topic name
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        topic_data = describe_kafka_topic(topic, bootstrap_servers)
        if topic_data is None:
            return json.dumps({"error": f"Topic '{topic}' not found or could not be described."}, indent=2)

        # Format partition details in a tabular structure
        result = {
            'topic_name': topic_data['topic_name'],
            'overview': {
                'partition_count': topic_data['partition_count'],
                'replication_factor': topic_data['replication_factor']
            },
            'retention_policy': {
                'retention_period': topic_data['retention_policy']['retention_readable'],
                'cleanup_policy': topic_data['retention_policy']['cleanup_policy'],
                'retention_ms': topic_data['retention_policy']['retention_ms'],
                'segment_ms': topic_data['retention_policy']['segment_ms']
            },
            'disk_usage': {
                'total_size': f"{topic_data['disk_usage']['total_size_bytes']:,} bytes",
                'size_mb': f"{topic_data['disk_usage']['total_size_mb']} MB",
                'size_gb': f"{topic_data['disk_usage']['total_size_gb']} GB"
            },
            'summary': f"Topic '{topic}' has {topic_data['partition_count']} partitions with replication factor {topic_data['replication_factor']}, retention period {topic_data['retention_policy']['retention_readable']}, and occupies {topic_data['disk_usage']['total_size_gb']} GB of disk space"
        }

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({
            "topic": topic,
            "error": f"Failed to describe topic: {str(e)}"
        }, indent=2)


@mcp.tool()
def generate_topic_consumers_report(bootstrap_servers: str = None, output_file_path: str = None) -> str:
    """
    Generate a CSV report of all topics and their consumers using cached data.

    Args:
        bootstrap_servers: Kafka bootstrap servers (uses env var if not provided)
        output_file_path: Custom path for the CSV file (optional, defaults to reports directory with timestamp)
    """
    if bootstrap_servers is None:
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    try:
        report_data = generate_topic_consumers_csv_report(bootstrap_servers, output_file_path)

        result = {
            "status": "success",
            "message": "Topic consumers report generated successfully",
            "report_details": {
                "file_path": report_data['file_path'],
                "total_topics": report_data['total_topics'],
                "topics_with_consumers": report_data['topics_with_consumers'],
                "topics_without_consumers": report_data['topics_without_consumers'],
                "total_consumer_groups": report_data['total_consumer_groups'],
                "average_consumers_per_topic": report_data['average_consumers_per_topic'],
                "report_generated_at": report_data['report_generated_at']
            },
            "cache_info": report_data['cache_info'],
            "csv_headers": ["Topic Name", "Number of Consumers", "Consumer Groups List"],
            "summary": f"Generated CSV report with {report_data['total_topics']} topics. {report_data['topics_with_consumers']} topics have consumers, {report_data['topics_without_consumers']} topics have no consumers. Report saved to: {report_data['file_path']}"
        }

        if not report_data['cache_info']['served_from_cache']:
            result[
                "performance_note"] = "Cache was built during this request. Subsequent report generations will be faster!"

        return json.dumps(result, indent=2)

    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": f"Failed to generate topic consumers report: {str(e)}"
        }, indent=2)


if __name__ == "__main__":
    mcp.run(transport='stdio')
