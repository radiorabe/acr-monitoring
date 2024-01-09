from io import BytesIO
from unittest.mock import patch

from kafka.consumer.fetcher import ConsumerRecord
from urllib3.response import HTTPResponse

from acrmonitor import read_from_event, send_from_data


@patch("acrmonitor.send_from_data")
@patch("acrmonitor.Minio")
@patch("acrmonitor.ZabbixSender")
def test_read_from_event(sender, mc, send_from_data):
    msg = ConsumerRecord(
        topic="topic",
        partition="partition",
        offset=0,
        timestamp=0,
        timestamp_type="",
        key="key",
        value='{"source":"minio:s3..acrcloud.raw","type":"com.amazonaws.s3.s3:ObjectCreated:Put","data":{}}',
        headers={},
        checksum="",
        serialized_key_size=0,
        serialized_value_size=0,
        serialized_header_size=0,
    )
    fp = BytesIO(b'[{"my":"data"}]')
    mc.get_object.return_value = HTTPResponse(fp)
    read_from_event(
        msg,
        mc,
        sender,
        zabbix_host="zabbix.example.org",
        zabbix_key="key.example.org",
    )
    send_from_data.assert_called_with(
        sender,
        "zabbix.example.org",
        "key.example.org",
        {"my": "data"},
    )


@patch("acrmonitor.ZabbixSender")
@patch("acrmonitor.ZabbixMetric")
def test_send_from_data(metric, sender):
    data = {
        "metadata": {
            "timestamp_utc": "1970-01-01 13:12:00",
            "acrid": "1234567890",
        }
    }
    send_from_data(
        sender=sender,
        zabbix_host="zabbix.example.com",
        zabbix_key="key.example.com",
        data=data,
    )
    metric.assert_called_with(
        host="zabbix.example.com",
        key="key.example.com",
        value="1234567890",
        clock=11520.0,
    )
    sender.send.assert_called_with([metric()])
