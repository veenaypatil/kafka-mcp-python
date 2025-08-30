# Kafka MCP Server

## Purpose

This MCP (Model Context Protocol) server connects to in-house Kafka to answer plain-text questions about topics,  consumers

## Setup

### Prerequisites

- **Python 3.10+** (Required for MCP package)
- Access to Kafka cluster
- Schema Registry (optional)
- Kafka Connect (optional)

### Installation

#### Option 1: Direct Installation (Python 3.10+)
```bash
pip install mcp kafka-python aiohttp
```

#### Option 2: Using conda/miniconda
```bash
conda create -n kafka-mcp python=3.10
conda activate kafka-mcp
pip install mcp kafka-python aiohttp
```

#### Option 3: Using pyenv with built-in venv
```bash
pyenv install 3.10.12 && pyenv local 3.10.12 && python -m venv kafka-mcp && source kafka-mcp/bin/activate && pip install --upgrade pip && pip install mcp kafka-python aiohttp
```

### IntelliJ / PyCharm Interpreter Note
If the IDE still shows Python 3.9 after activating 3.10:
- Open Settings > Project > Python Interpreter.
- Click gear > Add > Existing and select: ./kafka-mcp/bin/python
- Apply & reindex.  
Terminal activation does not automatically update the IDE interpreter.

## Environment Variables
Set before starting MCP server (Claude/other client):
```
KAFKA_BOOTSTRAP_SERVERS=host1:9092[,host2:9092]
TOPIC_CONSUMERS_CACHE_TTL=600                     # seconds (optional)
```

## Available MCP Tools (Kafka)
| Tool | Purpose | Key Params |
|------|---------|------------|
| consume_messages | Fetch messages with strategies latest / earliest / timestamp | topic, max_messages, offset_strategy, timestamp |
| produce_message | Send a message | topic, message |
| get_topic_partitions | Get partition count only | topic |
| count_messages_last_hours | Count messages produced in last X hours | topic, hours |
| get_topic_size | Disk usage via kafka-log-dirs | topic |
| describe_consumer_group | Members, offsets, lag per topic/partition | group_id |
| get_consumer_group_lag | Aggregated lag view | group_id |
| get_topic_consumers | List consumer groups per topic (cached) | topic, force_refresh |
| clear_topic_consumers_cache | Reset cache | — |
| get_cache_status | Cache diagnostics | — |
| describe_topic | Partitions (leader/replicas/ISR), retention, replication factor, disk size | topic |
| generate_topic_consumers_report | CSV of topics → consumers | output_file_path (optional) |

## Consumer Groups Cache
First call to get_topic_consumers builds a global in‑process cache (may take ~1 minute on large clusters). Subsequent calls are fast until TTL expires (default 600s). Use force_refresh=true to rebuild early. clear_topic_consumers_cache resets manually.

## Topic Consumers CSV Report
generate_topic_consumers_report writes a CSV to ~/Downloads by default (or a provided filename placed there). Columns:
1. Topic Name
2. Number of Consumers
3. Consumer Groups List (semicolon separated)

## Demo


## Retention & Disk Notes
describe_topic computes retention (retention.ms → human readable) and queries size via kafka-log-dirs (skipping initial header lines). Failures in size lookup return zero gracefully.

## Troubleshooting
- Topic not found: confirm spelling and cluster (KAFKA_BOOTSTRAP_SERVERS).
- Slow first consumer lookup: expected (cache build). Check get_cache_status.
- Empty consumers but known active: cache may be stale → force_refresh=true.
- Large clusters: increase TOPIC_CONSUMERS_CACHE_TTL to reduce rebuild frequency.

## MCP Tool Available

- **ask_kafka_question**: Accepts plain text questions about Kafka topics and returns relevant information
