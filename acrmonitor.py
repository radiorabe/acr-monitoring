"""acr-monitoring: ingest ACR CloudEvents from Kafka and send to Zabbix."""

import logging
import signal
import sys
from datetime import datetime, timezone

import urllib3
from cloudevents.core.bindings.kafka import KafkaMessage, from_kafka_event
from configargparse import ArgumentParser
from kafka import KafkaConsumer
from minio import Minio
from pyzabbix import ZabbixMetric, ZabbixSender

logger = logging.getLogger(__name__)


def app(
    sender: ZabbixSender,
    kafka_config: dict,
    minio_config: dict,
    zabbix_config: dict,
) -> None:  # pragma: no cover
    """Consume Kafka CloudEvent messages and process them."""
    consumer = KafkaConsumer(
        kafka_config["consumer_topic"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        group_id=kafka_config["consumer_group"],
        auto_offset_reset=kafka_config["consumer_auto_offset_reset"],
        ssl_cafile=kafka_config["tls_cafile"],
        ssl_certfile=kafka_config["tls_certfile"],
        ssl_keyfile=kafka_config["tls_keyfile"],
        enable_auto_commit=False,
    )

    def on_sigint(*_: object) -> None:
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, on_sigint)

    mc = Minio(
        minio_config["minio_url"],
        minio_config["minio_access_key"],
        minio_config["minio_secret_key"],
        secure=minio_config["minio_secure"],
        http_client=urllib3.PoolManager(
            cert_reqs=minio_config["minio_cert_reqs"],
            ca_certs=minio_config["minio_ca_certs"],
        ),
    )
    for msg in consumer:
        read_from_event(
            msg,
            mc,
            sender,
            zabbix_config["zabbix_host"],
            zabbix_config["zabbix_key"],
        )


def read_from_event(
    msg: object,
    mc: Minio,
    sender: ZabbixSender,
    zabbix_host: str,
    zabbix_key: str,
) -> None:
    """Convert Kafka event to CloudEvent and process matching records."""
    ce = from_kafka_event(
        KafkaMessage(
            key=msg.key,
            value=msg.value,
            headers=msg.headers or {},
        ),
    )
    if (
        ce.get_source() == "minio:s3..acrcloud.raw"
        and ce.get_type() == "com.amazonaws.s3.s3:ObjectCreated:Put"
    ):
        data_obj = ce.get_data() or {}
        bucket = data_obj.get("s3", {}).get("bucket", {}).get("name")
        name = data_obj.get("s3", {}).get("object", {}).get("key")
        obj = mc.get_object(bucket, name)
        for data in obj.json():
            send_from_data(sender, zabbix_host, zabbix_key, data)


def send_from_data(
    sender: ZabbixSender,
    zabbix_host: str,
    zabbix_key: str,
    data: dict,
) -> None:
    """Send single payload record as Zabbix metric."""
    meta = data.get("metadata", {})
    # convert from 2023-09-24 20:08:40
    record_date = datetime.timestamp(
        datetime.strptime(meta.get("timestamp_utc"), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc,
        ),
    )
    message = f"{record_date},1"
    logger.info("Sending %s", message)
    sender.send(
        [
            ZabbixMetric(
                host=zabbix_host,
                key=zabbix_key,
                value=meta.get("acrid"),
                clock=record_date,
            ),
        ],
    )


def main() -> None:  # pragma: no cover
    """Parse CLI arguments and start app."""
    parser = ArgumentParser(__name__)
    parser.add(
        "--kafka-bootstrap-servers",
        required=True,
        env_var="KAFKA_BOOTSTRAP_SERVERS",
    )
    parser.add(
        "--kafka-security-protocol",
        default="PLAINTEXT",
        env_var="KAFKA_SECURITY_PROTOCOL",
    )
    parser.add(
        "--kafka-tls-cafile",
        default=None,
        env_var="KAFKA_TLS_CAFILE",
    )
    parser.add(
        "--kafka-tls-certfile",
        default=None,
        env_var="KAFKA_TLS_CERTFILE",
    )
    parser.add(
        "--kafka-tls-keyfile",
        default=None,
        env_var="KAFKA_TLS_KEYFILE",
    )
    parser.add(
        "--kafka-consumer-topic",
        default="cloudevents",
        env_var="KAFKA_CONSUMER_TOPIC",
    )
    parser.add(
        "--kafka-consumer-group",
        default=__name__,
        env_var="KAFKA_CONSUMER_GROUP",
    )
    parser.add(
        "--kafka-consumer-auto-offset-reset",
        default="latest",
        env_var="KAFKA_CONSUMER_AUTO_OFFSET_RESET",
    )
    parser.add(
        "--minio-url",
        default="minio.service.int.rabe.ch:9000",
        env_var="MINIO_HOST",
        help="MinIO Hostname",
    )
    parser.add(
        "--minio-secure",
        default=True,
        env_var="MINIO_SECURE",
        help="MinIO Secure param",
    )
    parser.add(
        "--minio-cert-reqs",
        default="CERT_REQUIRED",
        env_var="MINIO_CERT_REQS",
        help="cert_reqs for urlib3.PoolManager used by MinIO",
    )
    parser.add(
        "--minio-ca-certs",
        default="/etc/pki/ca-trust/extracted/openssl/ca-bundle.trust.crt",
        env_var="MINIO_CA_CERTS",
        help="ca_certs for urlib3.PoolManager used by MinIO",
    )
    parser.add(
        "--minio-bucket-raw",
        default="acrcloud.raw",
        env_var="MINIO_BUCKET_RAW",
        help="MinIO Bucket with raw ACRCloud data",
    )
    parser.add(
        "--minio-bucket-music",
        default="acrcloud.music",
        env_var="MINIO_BUCKET_MUSIC",
        help="MinIO Bucket with music ACRCloud data",
    )
    parser.add(
        "--minio-access-key",
        default=None,
        env_var="MINIO_ACCESS_KEY",
        help="MinIO Access Key",
    )
    parser.add(
        "--minio-secret-key",
        default=None,
        env_var="MINIO_SECRET_KEY",
        help="MinIO Secret Key",
    )
    parser.add(
        "--zabbix-server",
        required=True,
        env_var="ZABBIX_SERVER",
    )
    parser.add(
        "--zabbix-port",
        default=10051,
        env_var="ZABBIX_PORT",
    )
    parser.add(
        "--zabbix-host",
        required=True,
        env_var="ZABBIX_HOST",
    )
    parser.add(
        "--zabbix-key",
        default="acrmonitor[last_file]",
        env_var="ZABBIX_KEY",
    )
    parser.add(
        "--quiet",
        "-q",
        default=False,
        action="store_true",
        env_var="ACRMONITOR_QUIET",
    )

    options = parser.parse_args()

    if not options.quiet:
        logging.basicConfig(level=logging.INFO)
    logger.info("Starting %s...", __name__)

    app(
        sender=ZabbixSender(
            zabbix_server=options.zabbix_server,
            zabbix_port=options.zabbix_port,
        ),
        kafka_config={
            "bootstrap_servers": options.kafka_bootstrap_servers,
            "security_protocol": options.kafka_security_protocol,
            "tls_cafile": options.kafka_tls_cafile,
            "tls_certfile": options.kafka_tls_certfile,
            "tls_keyfile": options.kafka_tls_keyfile,
            "consumer_topic": options.kafka_consumer_topic,
            "consumer_group": options.kafka_consumer_group,
            "consumer_auto_offset_reset": options.kafka_consumer_auto_offset_reset,
        },
        minio_config={
            "minio_url": options.minio_url,
            "minio_access_key": options.minio_access_key,
            "minio_secret_key": options.minio_secret_key,
            "minio_secure": options.minio_secure,
            "minio_cert_reqs": options.minio_cert_reqs,
            "minio_ca_certs": options.minio_ca_certs,
        },
        zabbix_config={
            "zabbix_host": options.zabbix_host,
            "zabbix_key": options.zabbix_key,
        },
    )


if __name__ == "__main__":  # pragma: no cover
    main()
