from time import sleep
from opcua import Client
from datetime import datetime
import pytz
import pymssql
######################################################################## Error Log
import logging
from logging.handlers import TimedRotatingFileHandler
log_handler = TimedRotatingFileHandler(
    filename="./log_files/errorLog",           # 확장자 없이!
    when="midnight",                 # 자정마다 분리
    interval=1,
    backupCount=30,                  # 보관할 최대 일수 (30일치)
    encoding="utf-8",
    utc=False,                       # 한국 시간 기준이면 False
    # atTime=datetime.time(16, 0)    # 자정이 아닌 다른 시간을 기준으로 롤오버(분리) 가능
)
log_handler.suffix = "%Y%m%d"        # 파일명 날짜 형식

# 로거 설정
logger = logging.getLogger("MyLogger")
logger.setLevel(logging.ERROR)
logger.addHandler(log_handler)

# 포맷터 설정 (옵션)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

# 서울 타임존
seoul_tz = pytz.timezone('Asia/Seoul')

import re

class SubHandler:
    def __init__(self, cursor):
        self.cursor = cursor
        self._seen = set()

    def _sanitize_device(self, device: str) -> str:
        """
        'ns=2;s=Device1' 또는 '2;s=Device1' 같은 프리픽스를 제거.
        """
        if not device:
            return device
        # 공백 제거
        device = device.strip()
        # 'ns=2;s=' 또는 '2;s=' 같은 형태 지우기
        device = re.sub(r"^(?:ns=\d+;)?s=", "", device)
        device = re.sub(r"^\d+;s=", "", device)
        return device

    def _unwrap_stringnode(self, s: str) -> str:
        # "StringNodeId(ns=2;s=Device1.MC3-2)" -> "ns=2;s=Device1.MC3-2"
        if s.startswith("StringNodeId(") and s.endswith(")"):
            return s[len("StringNodeId("):-1]
        return s

    def _parse_device_tag(self, node) -> tuple[str, str]:
        nid_obj = getattr(node, "nodeid", node)

        ident = getattr(nid_obj, "Identifier", None)
        if ident:
            core = str(ident)  # 보통 'Device1.MC3-2'
            if "." in core:
                device, tag = core.split(".", 1)
                return self._sanitize_device(device), tag

        #문자열 파싱
        s = nid_obj.to_string() if hasattr(nid_obj, "to_string") else str(nid_obj)
        s = self._unwrap_stringnode(s)

        m = re.search(r"s=([^. \t\r\n]+)\.([^) \t\r\n]+)", s)
        if m:
            device = self._sanitize_device(m.group(1))
            tag = m.group(2)
            return device, tag

        try:
            after_s = s.split("s=", 1)[1].rstrip(")")
            if "." in after_s:
                device, tag = after_s.split(".", 1)
                return self._sanitize_device(device), tag
        except Exception:
            pass

        return "", s

    def datachange_notification(self, node, val, data):
        # 첫 통지(현재값) 1회는 스킵
        try:
            handle = getattr(data.monitored_item, "ClientHandle", None)
        except Exception:
            handle = None
        key = handle if handle is not None else str(getattr(node, "nodeid", node))
        if key not in self._seen:
            self._seen.add(key)
            return

        # 서버 타임스탬프 -> 서울시간
        try:
            server_ts = data.monitored_item.Value.ServerTimestamp
            if server_ts:
                if server_ts.tzinfo is None:
                    server_ts = pytz.utc.localize(server_ts)
                server_ts_seoul = server_ts.astimezone(seoul_tz)
            else:
                server_ts_seoul = None
        except Exception:
            server_ts_seoul = None

        # device/tag 파싱
        device, tag = self._parse_device_tag(node)

        # 디버그 출력
        try:
            nid_obj = getattr(node, "nodeid", node)
            nodeid_str = nid_obj.to_string() if hasattr(nid_obj, "to_string") else str(nid_obj)
        except Exception:
            nodeid_str = str(getattr(node, "nodeid", node))

        print(f"[DATA] node={nodeid_str}, device={device}, tag={tag}, value={val}, server_ts={server_ts_seoul}")

        # DB INSERT (오토커밋)
        try:
            sql = """
                INSERT INTO dbo.TA_EquipDataAcquisition (Device, Tag, Value, InsertDate)
                VALUES (%s, %s, %s, GETDATE())
            """
            self.cursor.execute(sql, (device, tag, val))
            # autocommit=True 이므로 commit() 불필요
        except Exception as ex:
            logger.error("DB INSERT 실패 : " + str(ex))


def do_con():
    try:
        conn = pymssql.connect(
            server="sql16ssd-010.localnet.kr",
            user="yksyb_sys1",
            password="yksyb5509@@",
            database="yksyb_sys1",
            charset='UTF-8',
            tds_version="7.0",
            autocommit=True,
        )
        print("DB 연결 성공")
        return conn
    except Exception as ex:
        print("DB 연결 실패")
        logger.error("DB 연결 실패 : " + str(ex))
        sleep(10)
        return do_con()

def main(db_cursor):
    url = "opc.tcp://192.168.10.103:52250"
    client = None
    try:
        client = Client(url)
        # 필요 시 서버가 None 보안 허용이면 아래 주석 해제
        # client.set_security_string("None")
        client.connect()
        print("Connected.")

        # 구독 생성 (퍼블리싱 주기 2000ms = 2초)
        subscription = client.create_subscription(2000, SubHandler(db_cursor))

        # 모니터링할 노드들
        nodeids = [
            "ns=2;s=Device1.MC1-1", "ns=2;s=Device1.MC1-2", "ns=2;s=Device1.MC1-3",
            "ns=2;s=Device1.MC2-1", "ns=2;s=Device1.MC2-2",
            "ns=2;s=Device1.MC3-1", "ns=2;s=Device1.MC3-2",
            "ns=2;s=Device2.MC4-1", "ns=2;s=Device2.MC4-2", "ns=2;s=Device2.MC4-3", "ns=2;s=Device2.MC4-4",
            "ns=2;s=Device2.MC5-1", "ns=2;s=Device2.MC5-2",
            "ns=2;s=Device2.MC6-1", "ns=2;s=Device2.MC6-2",
        ]
        nodes = [client.get_node(nid) for nid in nodeids]

        # 각 노드를 개별적으로 구독하면서 샘플링 주기 0.5초 설정
        from opcua import ua
        for idx, node in enumerate(nodes):
            params = ua.MonitoredItemCreateRequest()
            params.ItemToMonitor.NodeId = node.nodeid
            params.ItemToMonitor.AttributeId = ua.AttributeIds.Value
            params.MonitoringMode = ua.MonitoringMode.Reporting
            params.RequestedParameters.ClientHandle = idx + 1
            params.RequestedParameters.SamplingInterval = 500  # 0.5초
            params.RequestedParameters.QueueSize = 1
            params.RequestedParameters.DiscardOldest = True

            subscription.create_monitored_items([params])

        # 무한 루프로 계속 수신 대기 (Ctrl+C로 종료)
        print("Monitoring started. Press Ctrl+C to stop...")
        while True:
            sleep(1)

    except KeyboardInterrupt:
        print("\nStopping by user request...")
    except Exception as e:
        print("cannot connect OPC UA! " + str(e))
        logger.error("cannot connect OPC UA! " + str(e))
    finally:
        if client is not None:
            try:
                if 'subscription' in locals():
                    subscription.delete()
                client.disconnect()
            except Exception:
                pass
        print("Disconnected.")

if __name__ == "__main__":
    conn = do_con()
    cursor = conn.cursor()
    # 필요 시: conn.autocommit(True) 도 가능
    main(cursor)
