from time import sleep
from opcua import Client
from opcua import ua
from datetime import datetime
import pytz
import pymssql
import math
import re

######################################################################## Error Log
import logging
from logging.handlers import TimedRotatingFileHandler

log_handler = TimedRotatingFileHandler(
    filename="./log_files/errorLog",
    when="midnight",
    interval=1,
    backupCount=30,
    encoding="utf-8",
    utc=False,
)
log_handler.suffix = "%Y%m%d"

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.ERROR)
logger.addHandler(log_handler)

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler.setFormatter(formatter)

# 서울 타임존
seoul_tz = pytz.timezone('Asia/Seoul')


class SubHandler:
    def __init__(self, cursor):
        self.cursor = cursor
        self._seen = set()

    # "ns=2;s=" 또는 "2;s=" 같은 프리픽스 제거
    def _sanitize_device(self, device: str) -> str:
        if not device:
            return device
        device = device.strip()
        device = re.sub(r"^(?:ns=\d+;)?s=", "", device)
        device = re.sub(r"^\d+;s=", "", device)
        return device

    def _unwrap_stringnode(self, s: str) -> str:
        if s.startswith("StringNodeId(") and s.endswith(")"):
            return s[len("StringNodeId("):-1]
        return s

    # node에서 device와 tag 분리
    def _parse_device_tag(self, node):
        nid_obj = getattr(node, "nodeid", node)

        ident = getattr(nid_obj, "Identifier", None)
        if ident:
            core = str(ident)  # e.g., 'Device1.MC3-2'
            if "." in core:
                device, tag = core.split(".", 1)
                return self._sanitize_device(device), tag

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

    def _is_nullish(self, v) -> bool:
        if v is None:
            return True
        if isinstance(v, float) and math.isnan(v):
            return True
        if isinstance(v, str) and v.strip() == "":
            return True
        return False

    def _to_float(self, v):
        try:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, str):
                vs = v.strip()
                if vs == "":
                    return None
                return float(vs)
            return float(v)
        except Exception:
            return None

    def _read_float_from_node(self, node_obj):
        try:
            dv: ua.DataValue = node_obj.get_data_value()
            sc = getattr(dv, "StatusCode", None)
            if isinstance(sc, ua.StatusCode) and not sc.is_good():
                return None
            # dv.Value는 Variant. 실제 값은 dv.Value.Value 에 들어있을 수 있음
            raw = None
            if hasattr(dv, "Value"):
                raw = getattr(dv.Value, "Value", None)
            raw = raw if raw is not None else dv  # fallback 방지
            return self._to_float(raw)
        except Exception:
            return None

    def datachange_notification(self, node, val, data):
        # 첫 통지(현재값) 1회 스킵
        try:
            handle = getattr(data.monitored_item, "ClientHandle", None)
        except Exception:
            handle = None
        key = handle if handle is not None else str(getattr(node, "nodeid", node))
        if key not in self._seen:
            self._seen.add(key)
            return

        # Good 이외(Status Bad/Uncertain)는 무시
        try:
            dv = data.monitored_item.Value
            sc = getattr(dv, "StatusCode", None)
            if isinstance(sc, ua.StatusCode) and not sc.is_good():
                return
        except Exception:
            pass

        # 값이 NULL/NaN/빈문자열이면 무시
        if self._is_nullish(val):
            return

        # device/tag 파싱
        device, tag = self._parse_device_tag(node)

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

        # --- 조건: MC4-2, MC4-3은 값이 0 초과일 때만 처리 ---
        if device == "Device2" and tag in ("MC4-2", "MC4-3"):
            v = self._to_float(val)
            if v is None or v <= 0:
                return
        
        # --- 조건: MC1-1 DB 이전 값과 비교 후 같지않을 때만 처리 ---
        if tag == "MC1-1" :
            sql = """
                SELECT TOP(1)
                    Value
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC1-1'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return
        
        # --- 조건: MC1-2 DB 이전 RawValue와 현재 VAL이 같지않을 때만 처리 ---
        if tag == "MC1-2" :
            sql = """
                SELECT TOP(1)
                    RawValue
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC1-2'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return

        # --- 조건: MC1-3 DB 이전 Value와 현재 VAL이 같지않고 1-1이 0이 아니거나 현재 시간과 이전 데이터 시간의 차가 적을 경우만 처리  ---
        # --- + 이전 데이터가 없으면 처리
        if tag == "MC1-3" :
            sql = """
                SELECT TOP(1)
                    Value, InsertDate
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC1-3'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            
            if not row:
                pass
            else:
                db_time = row[1]
                if db_time.tzinfo is None:
                    db_time = seoul_tz.localize(db_time)
                
                time_diff_sec = (server_ts_seoul - db_time).total_seconds()
                condition_1 = (val == row[0])
                condition_2 = (val == 0)
                condition_3 = (val > time_diff_sec)
                if (condition_1 and (condition_2 or condition_3)):
                    return

        # --- 조건: MC2-1 DB 이전 값과 비교 후 같지않을 때만 처리 ---
        if tag == "MC2-1" :
            sql = """
                SELECT TOP(1)
                    Value
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC2-1'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return

        # --- 조건: MC2-2 DB 이전 RawValue와 현재 VAL이 같지않을 때만 처리 ---
        if tag == "MC2-2" :
            sql = """
                SELECT TOP(1)
                    RawValue
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC2-2'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return

        # --- 조건: MC3-1 DB 이전 Value와 현재 VAL이 같지않을 때만 처리,  ---
        if tag == "MC3-1" :
            sql = """
                SELECT TOP(1)
                    Value, InsertDate
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC3-1'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            
            if not row:
                pass
            else:
                db_time = row[1]
                if db_time.tzinfo is None:
                    db_time = seoul_tz.localize(db_time)
                
                time_diff_sec = (server_ts_seoul - db_time).total_seconds()
                condition_1 = (val == row[0])
                condition_2 = (val == 0)
                condition_3 = (val > time_diff_sec)
                if (condition_1 and (condition_2 or condition_3)):
                    return
                
        # --- 조건: MC4-1 DB 이전 RawValue와 현재 VAL이 같지않을 때만 처리 ---
        if tag == "MC4-1" :
            sql = """
                SELECT TOP(1)
                    RawValue
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC4-1'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return

        # --- 조건: MC4-4 DB 이전 Value와 현재 VAL이 같지않을 때만 처리,  ---
        if tag == "MC4-4" :
            sql = """
                SELECT TOP(1)
                    Value, InsertDate
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC4-4'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            
            if not row:
                pass
            else:
                db_time = row[1]
                if db_time.tzinfo is None:
                    db_time = seoul_tz.localize(db_time)
                
                time_diff_sec = (server_ts_seoul - db_time).total_seconds()
                condition_1 = (val == row[0])
                condition_2 = (val == 0)
                condition_3 = (val > time_diff_sec)
                if (condition_1 and (condition_2 or condition_3)):
                    return
                
        # --- 조건: MC5-1 DB 이전 Value와 현재 VAL이 같지않을 때만 처리,  ---
        if tag == "MC5-1" :
            sql = """
                SELECT TOP(1)
                    Value, InsertDate
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC5-1'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            
            if not row:
                pass
            else:
                db_time = row[1]
                if db_time.tzinfo is None:
                    db_time = seoul_tz.localize(db_time)
                
                time_diff_sec = (server_ts_seoul - db_time).total_seconds()
                condition_1 = (val == row[0])
                condition_2 = (val == 0)
                condition_3 = (val > time_diff_sec)
                if (condition_1 and (condition_2 or condition_3)):
                    return
                
        # --- 조건: MC5-2 DB 이전 RawValue와 현재 VAL이 같지않을 때만 처리 ---
        if tag == "MC5-2" :
            sql = """
                SELECT TOP(1)
                    RawValue
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC5-2'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return

        # --- 조건: MC6-1 DB 이전 값과 비교 후 같지않을 때만 처리 ---
        if tag == "MC6-1" :
            sql = """
                SELECT TOP(1)
                    Value
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC6-1'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return
        
        # --- 조건: MC6-2 DB 이전 RawValue와 현재 VAL이 같지않을 때만 처리 ---
        if tag == "MC6-2" :
            sql = """
                SELECT TOP(1)
                    RawValue
                FROM [dbo].[TA_EquipDataAcquisition]
                WHERE Tag = 'MC6-2'
                ORDER BY InsertDate DESC
            """
            self.cursor.execute(sql, tag)
            row = self.cursor.fetchone()
            if val == row[0] : return
        

        # 디버그 출력
        try:
            nid_obj = getattr(node, "nodeid", node)
            nodeid_str = nid_obj.to_string() if hasattr(nid_obj, "to_string") else str(nid_obj)
        except Exception:
            nodeid_str = str(getattr(node, "nodeid", node))


        # DB INSERT (특정 태그는 Value + RawValue 따로 저장)
        try:
            if tag in ("MC1-2", "MC2-2", "MC4-1", "MC5-2", "MC6-2"):
                sql = """
                    SELECT TOP(1)
                        RawValue
                    FROM dbo.TA_EquipDataAcquisition
                    WHERE Tag = %s
                    ORDER BY InsertDate DESC
                """
                self.cursor.execute(sql, tag)
                row = self.cursor.fetchone()
                pre_rawvalue = 0
                if row and row[0] is not None:
                    pre_rawvalue = row[0]
                    sql = """
                        INSERT INTO dbo.TA_EquipDataAcquisition (Device, Tag, Value, InsertDate, RawValue)
                        VALUES (%s, %s, %s, GETDATE(), %s)
                    """
                    self.cursor.execute(sql, (device, tag, (val - pre_rawvalue), val))
                else :
                    sql = """
                        INSERT INTO dbo.TA_EquipDataAcquisition (Device, Tag, Value, InsertDate, RawValue)
                        VALUES (%s, %s, %s, GETDATE(), %s)
                    """
                    self.cursor.execute(sql, (device, tag, val, val))
                
            else:
                sql = """
                    INSERT INTO dbo.TA_EquipDataAcquisition (Device, Tag, Value, InsertDate)
                    VALUES (%s, %s, %s, GETDATE())
                """
                self.cursor.execute(sql, (device, tag, val))
            
            print(f"[DATA] node={nodeid_str}, device={device}, tag={tag}, value={val}, server_ts={server_ts_seoul}")
        except Exception as ex:
            logger.error("DB INSERT 실패 : " + str(ex) + "/" + str(device) + "." + str(tag) + " : "+ str(val))


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
    subscription_fast = None   # 2초 퍼블리싱(기본)
    subscription_slow = None   # 10초 퍼블리싱(MC3-2 전용)

    try:
        client = Client(url)
        # client.set_security_string("None")
        client.connect()
        print("Connected.")

        # 구독 생성 (핸들러에 node_mc31 전달)
        handler_fast = SubHandler(db_cursor)
        handler_slow = SubHandler(db_cursor)

        subscription_fast = client.create_subscription(2000, handler_fast)
        subscription_slow = client.create_subscription(10000, handler_slow)

        slow_nodeid = [
            "ns=2;s=Device1.MC3-2", 
            "ns=2;s=Device2.MC4-2", "ns=2;s=Device2.MC4-3",
            "ns=2;s=Device2.MC6-1",
        ]
        fast_nodeids = [
            "ns=2;s=Device1.MC1-1", "ns=2;s=Device1.MC1-2", "ns=2;s=Device1.MC1-3",
            "ns=2;s=Device1.MC2-1", "ns=2;s=Device1.MC2-2",
            "ns=2;s=Device1.MC3-1",
            "ns=2;s=Device2.MC4-1", "ns=2;s=Device2.MC4-4",
            "ns=2;s=Device2.MC5-1", "ns=2;s=Device2.MC5-2",
            "ns=2;s=Device2.MC6-2",
        ]

        # 빠른 구독: 샘플링 0.5초
        for idx, nid in enumerate(fast_nodeids, start=1):
            node = client.get_node(nid)
            params = ua.MonitoredItemCreateRequest()
            params.ItemToMonitor.NodeId = node.nodeid
            params.ItemToMonitor.AttributeId = ua.AttributeIds.Value
            params.MonitoringMode = ua.MonitoringMode.Reporting
            params.RequestedParameters.ClientHandle = idx
            params.RequestedParameters.SamplingInterval = 500  # 0.5초
            params.RequestedParameters.QueueSize = 1
            params.RequestedParameters.DiscardOldest = True
            subscription_fast.create_monitored_items([params])

        # 느린 구독(10초): MC3-2만
        for idx, nid in enumerate(slow_nodeid, start=1):
            node = client.get_node(nid)
            params = ua.MonitoredItemCreateRequest()
            params.ItemToMonitor.NodeId = node.nodeid
            params.ItemToMonitor.AttributeId = ua.AttributeIds.Value
            params.MonitoringMode = ua.MonitoringMode.Reporting
            params.RequestedParameters.ClientHandle = idx
            params.RequestedParameters.SamplingInterval = 500  # 0.5초
            params.RequestedParameters.QueueSize = 1
            params.RequestedParameters.DiscardOldest = True
            subscription_slow.create_monitored_items([params])

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
                if subscription_fast is not None:
                    subscription_fast.delete()
                if subscription_slow is not None:
                    subscription_slow.delete()
                client.disconnect()
            except Exception:
                pass
        print("Disconnected.")


if __name__ == "__main__":
    conn = do_con()
    cursor = conn.cursor()
    main(cursor)