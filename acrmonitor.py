import logging
import signal
import sys
from datetime import datetime

import urllib3
from cloudevents.kafka import from_structured
from cloudevents.kafka.conversion import KafkaMessage
from configargparse import ArgumentParser
from kafka import KafkaConsumer
from minio import Minio
from pyzabbix import ZabbixMetric, ZabbixSender

logger = logging.getLogger(__name__)


def app(
    sender: ZabbixSender,
    bootstrap_servers: list[str],
    security_protocol: str,
    tls_cafile: str,
    tls_certfile: str,
    tls_keyfile: str,
    consumer_topic: str,
    consumer_group: str,
    consumer_auto_offset_reset: str,
    minio_url: str,
    minio_access_key: str,
    minio_secret_key: str,
    minio_bucket_raw: str,
    minio_bucket_music: str,
    minio_secure: bool,
    minio_cert_reqs: str,
    minio_ca_certs: str,
    zabbix_host: str,
    zabbix_key: str,
):  # pragma: no cover
    consumer = KafkaConsumer(
        consumer_topic,
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol,
        group_id=consumer_group,
        auto_offset_reset=consumer_auto_offset_reset,
        ssl_cafile=tls_cafile,
        ssl_certfile=tls_certfile,
        ssl_keyfile=tls_keyfile,
        enable_auto_commit=False,
    )

    def on_sigint(*_):
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, on_sigint)

    mc = Minio(
        minio_url,
        minio_access_key,
        minio_secret_key,
        secure=minio_secure,
        http_client=urllib3.PoolManager(
            cert_reqs=minio_cert_reqs, ca_certs=minio_ca_certs
        ),
    )
    for msg in consumer:
        read_from_event(
            msg,
            mc,
            sender,
            zabbix_host,
            zabbix_key,
        )


def read_from_event(
    msg,
    mc,
    sender,
    zabbix_host,
    zabbix_key,
):
    ce = from_structured(
        message=KafkaMessage(
            key=msg.key,
            value=msg.value,
            headers=msg.headers if msg.headers else {},
        )
    )

    if (
        ce["source"] == "minio:s3..acrcloud.raw"
        and ce["type"] == "com.amazonaws.s3.s3:ObjectCreated:Put"
    ):
        bucket = ce.data.get("s3", {}).get("bucket", {}).get("name")
        name = ce.data.get("s3", {}).get("object", {}).get("key")
        obj = mc.get_object(bucket, name)
        for data in obj.json():
            send_from_data(sender, zabbix_host, zabbix_key, data)


def send_from_data(sender, zabbix_host, zabbix_key, data):
    meta = data.get("metadata", {})
    # convert from 2023-09-24 20:08:40
    record_date = datetime.timestamp(
        datetime.strptime(meta.get("timestamp_utc"), "%Y-%m-%d %H:%M:%S")
    )
    message = f"{record_date},1"
    logger.info(f"Sending {message=}")
    sender.send(
        [
            ZabbixMetric(
                host=zabbix_host,
                key=zabbix_key,
                value=meta.get("acrid"),
                clock=record_date,
            )
        ]
    )


def main():  # pragma: no cover
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
    logger.info(f"Starting {__name__}...")

    app(
        sender=ZabbixSender(
            zabbix_server=options.zabbix_server,
            zabbix_port=options.zabbix_port,
        ),
        bootstrap_servers=options.kafka_bootstrap_servers,
        security_protocol=options.kafka_security_protocol,
        tls_cafile=options.kafka_tls_cafile,
        tls_certfile=options.kafka_tls_certfile,
        tls_keyfile=options.kafka_tls_keyfile,
        consumer_topic=options.kafka_consumer_topic,
        consumer_group=options.kafka_consumer_group,
        consumer_auto_offset_reset=options.kafka_consumer_auto_offset_reset,
        minio_url=options.minio_url,
        minio_access_key=options.minio_access_key,
        minio_secret_key=options.minio_secret_key,
        minio_bucket_raw=options.minio_bucket_raw,
        minio_bucket_music=options.minio_bucket_music,
        minio_secure=options.minio_secure,
        minio_cert_reqs=options.minio_cert_reqs,
        minio_ca_certs=options.minio_ca_certs,
        zabbix_host=options.zabbix_host,
        zabbix_key=options.zabbix_key,
    )


if __name__ == "__main__":  # pragma: no cover
    main()
