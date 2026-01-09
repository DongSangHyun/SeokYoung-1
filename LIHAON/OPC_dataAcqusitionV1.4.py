from time import sleep
from opcua import Client
from opcua import ua
from datetime import datetime, date, timedelta
import pytz
import math
import re
import os
import logging
from logging.handlers import TimedRotatingFileHandler
import queue
import threading
import time 
import oracledb   

# 파워쉘 실행파일 만들기 
# & "C:\Users\LIHAON\AppData\Local\Programs\Python\Python314\python.exe" -m pip install pyinstaller
# Set-Location "C:\Users\LIHAON\.vscode\workspace\Lihaon_OPC\LIHAON"
# & "C:\Users\LIHAON\AppData\Local\Programs\Python\Python314\python.exe" -m PyInstaller --onefile --hidden-import=getpass OPC_dataAcqusitionV1.3.py

# ──────────────────────────────────────────────────────────────
# 로깅
# ────────────────────────────────────────────────
# 실행파일(.exe) 또는 .py 기준 경로 구하기
import sys
from pathlib import Path
def get_base_dir() -> Path:
    if getattr(sys, "frozen", False):  # PyInstaller로 빌드된 경우
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

# ────────────────────────────────────────────────
# log_file 폴더 생성
base_dir = get_base_dir()
log_dir = base_dir / "log_file"
log_dir.mkdir(exist_ok=True)

# ────────────────────────────────────────────────
# 로그 핸들러 설정
log_handler = TimedRotatingFileHandler(
    filename=log_dir / "errorLog.log",
    when="midnight",      # 자정마다 로그 파일 교체
    interval=1,           # 1일마다
    backupCount=7,        # 7일치 백업
    encoding="utf-8",
    delay=False,
    utc=False,
)

# ────────────────────────────────────────────────
# 로깅 기본 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[log_handler]
)


logging.info("로그 파일 초기화 완료")

# ──────────────────────────────────────────────────────────────
# 설비 - Tag 정보
press_list = [
    {"Device": "Device1", "Tag": "MC1", "Name": "프레스01호기", "OperatorCode": "P01"},
    {"Device": "Device1", "Tag": "MC2", "Name": "프레스02호기", "OperatorCode": "P02"},
    {"Device": "Device1", "Tag": "MC3", "Name": "프레스03호기", "OperatorCode": "P03"},
    {"Device": "Device1", "Tag": "MC4", "Name": "프레스04호기", "OperatorCode": "P04"},
    {"Device": "Device1", "Tag": "MC5", "Name": "프레스05호기", "OperatorCode": "P05"},
    {"Device": "Device1", "Tag": "MC6", "Name": "프레스09호기", "OperatorCode": "P09"},
    {"Device": "Device2", "Tag": "MC1", "Name": "프레스10호기", "OperatorCode": "P10"},
]

# press_list 안의 각 항목(x)을
# x["Tag"] 값을 key 로 하고
# 전체 항목 x 를 value 로 하는 딕셔너리로 변환한다.
# 예) PRESS_BY_MC["MC1"] → press_list 안에서 Tag == "MC1" 인 전체 데이터

PRESS_BY_MC = {x["Tag"]: x for x in press_list}

# ──────────────────────────────────────────────────────────────
# 조업일자(기준 08:00)
seoul_tz = pytz.timezone("Asia/Seoul")

def convert_day(day=None) -> date:
    if day is None or (isinstance(day, str) and day.strip() == ""):
        dt = datetime.now(seoul_tz)
    elif isinstance(day, datetime):
        dt = day if day.tzinfo else seoul_tz.localize(day)
        dt = dt.astimezone(seoul_tz)
    elif isinstance(day, date):
        dt = seoul_tz.localize(datetime.combine(day, datetime.min.time()))
    elif isinstance(day, str):
        s = day.strip().replace("오전", "AM").replace("오후", "PM")
        fmts = [
            "%Y-%m-%d %p %I:%M:%S",
            "%Y-%m-%d %p %I:%M",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
        ]
        dt = None
        for fmt in fmts:
            try:
                dt_naive = datetime.strptime(s, fmt)
                dt = seoul_tz.localize(dt_naive)
                break
            except ValueError:
                continue
        if dt is None:
            dt = datetime.now(seoul_tz)
    else:
        dt = datetime.now(seoul_tz)

    base = dt.replace(hour=8, minute=0, second=0, microsecond=0)
    return (dt - timedelta(days=1)).date() if dt < base else dt.date()
 
# ──────────────────────────────────────────────────────────────
# Oracle 연결 팩토리 (워커 스레드에서 사용)
def get_oracle_conn():
    # Thick 모드 초기화 (환경에 맞게 경로 조정)
    oracledb.init_oracle_client(lib_dir=r"C:\oracle\instantclient_23_9")

    host = "1.223.199.138"
    port = 1521
    user = "bom"
    pwd  = "hybom"
    svc = "orcl"

    try:
        dsn = oracledb.makedsn(host, port, service_name=svc)
        conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
        conn.autocommit = True
        logging.info(f"[DB] Oracle 연결 성공 (Thick, service_name={svc})")
    except Exception as e1:
        logging.warning(f"[DB] service_name={svc} 실패: {e1}")
        # SID로 재시도
        sid = "orcl"
        dsn = oracledb.makedsn(host, port, sid=sid)
        conn = oracledb.connect(user=user, password=pwd, dsn=dsn)
        conn.autocommit = True
        logging.info(f"[DB] Oracle 연결 성공 (Thick, sid={sid})")

    # (선택) 실제 service_name 로깅
    try:
        with conn.cursor() as c:
            c.execute("SELECT sys_context('userenv','service_name') FROM dual")
            logging.info(f"[DB] 실제 service_name = {c.fetchone()[0]}")
    except Exception:
        pass

    logging.info(f"[DB] is_thin_mode={oracledb.is_thin_mode()}")
    return conn

# ──────────────────────────────────────────────────────────────
# 헬퍼들
def _is_nullish(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False
 
def _unwrap_stringnode(s: str) -> str:
    if s.startswith("StringNodeId(") and s.endswith(")"):
        return s[len("StringNodeId("):-1]
    return s

def _sanitize_device(device: str) -> str:
    if not device:
        return device
    device = device.strip()
    device = re.sub(r"^(?:ns=\d+;)?s=", "", device)
    device = re.sub(r"^\d+;s=", "", device)
    return device

def parse_device_tag(node):
    nid_obj = getattr(node, "nodeid", node)

    ident = getattr(nid_obj, "Identifier", None)
    if ident:
        core = str(ident)  # e.g., 'Device1.MC3_1'
        if "." in core:
            device, tag = core.split(".", 1)
            return _sanitize_device(device), tag

    s = nid_obj.to_string() if hasattr(nid_obj, "to_string") else str(nid_obj)
    s = _unwrap_stringnode(s)

    m = re.search(r"s=([^. \t\r\n]+)\.([^) \t\r\n]+)", s)
    if m:
        device = _sanitize_device(m.group(1))
        tag = m.group(2)
        return device, tag

    try:
        after_s = s.split("s=", 1)[1].rstrip(")")
        if "." in after_s:
            device, tag = after_s.split(".", 1)
            return _sanitize_device(device), tag
    except Exception:
        pass

    return "", str(getattr(node, "nodeid", node))

def get_mc_info(tag: str, device: str):
    """
    tag에서 'MC\\d+' 추출 후, 동일한 Tag와 Device가 일치하는 press_list 항목 반환
    """
    if not tag:
        return None

    # 태그에서 MC번호 추출 (예: 'MC1', 'MC2_1' → 'MC1', 'MC2')
    m = re.match(r"^(MC\d+)", tag)
    if not m:
        return None

    mc = m.group(1)

    # press_list 내에서 Device와 Tag 모두 일치하는 항목 찾기
    for info in press_list:
        if info["Tag"] == mc and info["Device"] == device:
            return info

    return None  # 일치하는 항목이 없으면 None


# ──────────────────────────────────────────────────────────────
# DB 쓰기 전용 워커 (큐 → 순차/배치 처리)
class DBWriter:
    def __init__(self, conn_factory, batch_size=100, commit_interval=0.5, max_queue=10000):
        self.conn_factory = conn_factory
        self.batch_size = batch_size
        self.commit_interval = commit_interval
        self.q = queue.Queue(maxsize=max_queue)
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self._run, name="DBWriter", daemon=True)

        # 프레스 타발 수 수집 ( 수집 설비 일 경우 만 실적 등록)
        # self.sql_prod = ("""
        #     DECLARE
        #         v_yn       VARCHAR2(1);
        #         v_assayno  VARCHAR2(20);
        #     BEGIN
        #         -- 실적 수집 가능 설비인지 확인
        #         BEGIN
        #             SELECT p_yn INTO v_yn
        #             FROM prod_pop_start
        #             WHERE p_code = :p_code;
        #         EXCEPTION
        #             WHEN NO_DATA_FOUND THEN
        #                 v_yn := NULL;
        #         END;

        #         -- 실적 수집 가능 설비면, 해당 일자의 ASSYNO 조회
        #         IF v_yn = 'Y' THEN
        #             BEGIN 
        #                 v_assayno := NULL;
                         
        #                 SELECT assyno INTO v_assayno
        #                   FROM prod_poppart
        #                  WHERE p_code = :p_code
        #                    AND p_date = :eff_date;
                         
        #                   -- 실적 기록
        #                 INSERT INTO prod_sj_real (p_code, eff_date, p_date, p_okng, assyno, rawcount)
        #                 VALUES (:p_code, :eff_date, :pdate, '1', v_assayno, :rawcount); 
                         
        #             EXCEPTION
        #                 WHEN NO_DATA_FOUND THEN
        #                     v_assayno := NULL;
        #             END; 
        #         END IF; 
                         
               
        #     END;
        # """)

        # 생산 실적 수집 가능 설비 만 등록 하도록 설정 / 아닐 경우 에도 메세지 처리를 위해 데이터 등록 여부 반환   :o_ins 를 사용 
        self.sql_prod = """
        DECLARE
            v_yn      VARCHAR2(2);
            v_assayno VARCHAR2(20);
        BEGIN
            :o_ins := 0;  -- 기본: 미삽입

            -- 실적 수집 가능 설비인지 확인
            BEGIN
                SELECT p_yn INTO v_yn
                FROM prod_pop_start
                WHERE p_code = :p_code;
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_yn := NULL;
            END;

            -- 실적 수집 가능 설비면, 해당 일자의 ASSYNO 조회 후 INSERT
            IF v_yn = 'Y' THEN
                BEGIN 

                    SELECT MAX(assyno) INTO v_assayno
                    FROM prod_poppart
                    WHERE p_code = :p_code
                    AND p_date = :eff_date;


                    INSERT INTO prod_sj_real (p_code, eff_date, p_date, p_okng, assyno, rawcount)
                    VALUES (:p_code, :eff_date, :pdate, '1', v_assayno, :rawcount);
                    
                    :o_ins := 1; -- 삽입 성공

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_assayno := NULL; -- assyno 없어서 미삽입 유지
                END;
            END IF;
        END;
        """

        # 설비 가동 비가동 데이터 수집
        self.sql_state = ("""
        DECLARE
            v_last_yn VARCHAR2(10); 
        BEGIN
            -- 1) 같은 p_code에서 p_date가 최댓값인 행의 p_yn 조회
            BEGIN
                SELECT p_yn
                INTO v_last_yn
                FROM prod_pop_onoff
                WHERE p_code = :p_code
                AND p_date = (
                        SELECT MAX(p_date)
                        FROM prod_pop_onoff
                        WHERE p_code = :p_code
                );
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    v_last_yn := 'N';  
            END;

            -- 2) 이전 값과 동일하면 아무 것도 안 함, 다르면 INSERT
            IF v_last_yn <> :runstopflag THEN
                INSERT INTO prod_pop_onoff (p_code, p_date, p_yn)
                VALUES (
                    :p_code,
                    :pdate,
                    :runstopflag
                );
            END IF;
        END;
        """)



        self.thread.start()

    def enqueue(self, item: dict):
        self.q.put_nowait(item)

    def stop_and_join(self, drain=True, timeout=5):
        if drain:
            self.q.join()
        self.stop_flag.set()
        self.thread.join(timeout=timeout)

    def _run(self):
        conn = None
        cursor = None
        last_commit = time.time()
        processed = 0

        def ensure_conn():
            nonlocal conn, cursor
            if conn is None:
                conn = self.conn_factory()
                cursor = conn.cursor()

        try:
            ensure_conn()
            while not self.stop_flag.is_set() or not self.q.empty():
                try:
                    item = self.q.get(timeout=0.2)
                except queue.Empty:
                    # 주기적 커밋
                    if cursor and processed > 0 and (time.time() - last_commit) >= self.commit_interval:
                        try:
                            conn.commit()
                        except Exception:
                            pass
                        last_commit = time.time()
                        processed = 0
                    continue

                try:
                    kind = item.get("kind")
                    if kind == "PROD":
                        # cursor.execute(self.sql_prod,
                        #                p_code=item["p_code"],
                        #                eff_date=item["eff_date"],
                        #                pdate=item["pdate"],
                        #                rawcount=item["rawcount"])
                        # logging.info(f"[PROD] {item.get('debug','')}")  
                        # print(f"생산 실적 등록 : {item["p_code"]} , 조업일자 : {item["eff_date"]} , 일시 : { item["pdate"]} , 누적 카운트  : {item["rawcount"]}") 
                        # cursor 는 Oracle cursor

                        
                        o_ins = cursor.var(int)  # oracledb/cx_Oracle 모두 가능

                        cursor.execute(
                            self.sql_prod,
                            p_code=item["p_code"],
                            eff_date=item["eff_date"],
                            pdate=item["pdate"],
                            rawcount=item["rawcount"],
                            o_ins=o_ins,  # OUT 변수
                        )

                        if int(o_ins.getvalue() or 0) == 1:
                            logging.info(f"[PROD] {item.get('debug','')}")
                            print(f"생산 실적 등록 : {item['p_code']} , 조업일자 : {item['eff_date']} , 일시 : {item['pdate']} , 누적 카운트 : {item['rawcount']}")
                        else:
                            print(f"생산 실적 수집 미 허용 상태 생산 카운트 :  {item['p_code']} , 조업일자 : {item['eff_date']} , 일시 : {item['pdate']} , 누적 카운트 : {item['rawcount']}")


                    elif kind == "STATE": 
                        cursor.execute(self.sql_state,
                                       p_code=item["p_code"],
                                       pdate=item["pdate"],
                                       runstopflag=item["runstopflag"])
                        logging.info(f"[STATE] {item.get('debug','')}")  
                        print(f"가동 비가동 실적 등록  : {item["p_code"]} , 조업일자 : {item["eff_date"]} , 상태 : {item["runstopflag"]} , 일시 : { item["pdate"]} ") 
 
                    processed += 1
                    self.q.task_done()

                    # 배치 커밋
                    if processed >= self.batch_size or (time.time() - last_commit) >= self.commit_interval:
                        conn.commit()
                        last_commit = time.time()
                        processed = 0

                except Exception as ex:
                    logging.error(f"[DB] 처리 실패: {ex} / item={item}")
                    print(f"[DB] 처리 실패: {ex} / item={item}")
                    # 연결 오류 가능 → 롤백·재연결
                    try:
                        if conn:
                            conn.rollback()
                            conn.close()
                    except Exception:
                        pass
                    conn, cursor = None, None
                    time.sleep(0.3)
                    ensure_conn()

        finally:
            try:
                if conn:
                    conn.commit()
                    conn.close()
            except Exception:
                pass

# ──────────────────────────────────────────────────────────────
# 구독 핸들러(콜백): 가볍게 → 큐 적재만
class SubHandler:
    def __init__(self, writer: DBWriter):
        self.writer = writer
        self._seen = set()

    def datachange_notification(self, node, val, data):
        # 1) 첫 통지(현재값) 스킵 
        dt_NowWorkDate = convert_day().strftime("%Y-%m-%d")
        try:
            # 전역 조업일자 문자열 (YYYY-MM-DD)
            handle = getattr(data.monitored_item, "ClientHandle", None)
        except Exception:
            handle = None
        key = handle if handle is not None else str(getattr(node, "nodeid", node))
        if key not in self._seen:
            self._seen.add(key)
            return

        # 2) 상태코드 Good 이외는 무시
        try:
            dv = data.monitored_item.Value
            sc = getattr(dv, "StatusCode", None)
            if isinstance(sc, ua.StatusCode) and not sc.is_good():
                return
        except Exception:
            pass

        # 3) 값 체크
        if _is_nullish(val):
            return

        # 4) device/tag 파싱
        device, tag = parse_device_tag(node)

        # 5) 서버 타임스탬프 → 서울시간
        try:
            server_ts = data.monitored_item.Value.ServerTimestamp
            if server_ts:
                if server_ts.tzinfo is None:
                    server_ts = pytz.utc.localize(server_ts)
                ts_seoul = server_ts.astimezone(seoul_tz)
            else:
                ts_seoul = datetime.now(seoul_tz)
        except Exception:
            ts_seoul = datetime.now(seoul_tz)

        trans_datetime = ts_seoul.strftime("%Y%m%d%H%M%S")  # 20250101080101 

        # 6) 태그 규칙 분기 → 큐 적재
        try:
            # 설비 가동 상태 (예: MCx_1)
            if re.match(r"^MC\d+_1$", tag or "") and val is not None:
                
                mcinfo = get_mc_info(tag,device)
                # 상태 태그도 설비별로 p_code(작업코드)가 필요하다면 규칙에 맞게 매핑
                op_code = (mcinfo["OperatorCode"] if mcinfo else "UNKNOWN")
                runstopflag = 'Y' if int(val) == 1 else 'N' 
                 
                self.writer.enqueue({
                    "kind": "STATE",
                    "p_code": op_code,
                    "eff_date": dt_NowWorkDate,   # 조업일자  YYYY-MM-DD (10자리)
                    "pdate": trans_datetime,
                    "runstopflag": runstopflag, 
                    "debug": f"{device}.{tag} → STATE={runstopflag}"
                })    
                
            # 생산 실적 (예: MC1, MC2, …)
            elif re.match(r"^MC\d", tag or "") and val is not None:          
               
                mcinfo = get_mc_info(tag,device)
                p_code = (mcinfo["OperatorCode"] if mcinfo else "UNKNOWN")
                self.writer.enqueue({
                    "kind": "PROD",
                    "p_code": p_code,
                    "eff_date": dt_NowWorkDate,   # 조업일자  YYYY-MM-DD (10자리)
                    "pdate": trans_datetime,      # 14자리
                    "rawcount": val,             # 누적 카운트
                    "debug": f"{device}.{tag} → VAL={val}"
                })  
                

        except queue.Full:
            logging.warning("DB queue full; dropping an item")
        except Exception as ex:
            logging.error(f"Enqueue 실패: {ex} / {device}.{tag} : {val}")

# ──────────────────────────────────────────────────────────────
def main():
    # url = "opc.tcp://192.168.100.241:52250"  #2026/01/07 IP 대역대 변경
    url = "opc.tcp://192.168.0.156:52250" 
    client = None
    subscription_fast = None
    subscription_slow = None
    writer = None

    try:
        # DBWriter 시작(별도 스레드에서 Oracle 연결)
        writer = DBWriter(get_oracle_conn, batch_size=100, commit_interval=0.5, max_queue=20000)

        client = Client(url)
        client.connect()
        logging.info("OPC UA Connected")

        handler_fast = SubHandler(writer)
        handler_slow = SubHandler(writer)

        subscription_fast = client.create_subscription(2000, handler_fast)  # 2초
        subscription_slow = client.create_subscription(500, handler_slow)  # 1초

        # 모니터링할 노드들
        fast_nodeids = [
            # 빠른 샘플링이 필요한 태그가 있다면 등록
        ]
        slow_nodeids = [
            "ns=2;s=Device1.MC1",
            "ns=2;s=Device1.MC1_1",
            "ns=2;s=Device1.MC2",
            "ns=2;s=Device1.MC2_1",
            "ns=2;s=Device1.MC3",
            "ns=2;s=Device1.MC3_1",
            "ns=2;s=Device1.MC4",     
            "ns=2;s=Device1.MC4_1",
            "ns=2;s=Device1.MC5",
            "ns=2;s=Device1.MC5_1",
            "ns=2;s=Device1.MC6",
            "ns=2;s=Device1.MC6_1",
            "ns=2;s=Device2.MC1",
            "ns=2;s=Device2.MC1_1",
        ]

        # 빠른 구독 100ms
        for idx, nid in enumerate(fast_nodeids, start=1):
            node = client.get_node(nid)
            params = ua.MonitoredItemCreateRequest()
            params.ItemToMonitor.NodeId = node.nodeid
            params.ItemToMonitor.AttributeId = ua.AttributeIds.Value
            params.MonitoringMode = ua.MonitoringMode.Reporting
            params.RequestedParameters.ClientHandle = idx
            params.RequestedParameters.SamplingInterval = 100  # ms
            params.RequestedParameters.QueueSize = 1
            params.RequestedParameters.DiscardOldest = True
            subscription_fast.create_monitored_items([params])

        # 느린 구독 500ms
        for idx, nid in enumerate(slow_nodeids, start=1):
            node = client.get_node(nid)
            params = ua.MonitoredItemCreateRequest()
            params.ItemToMonitor.NodeId = node.nodeid
            params.ItemToMonitor.AttributeId = ua.AttributeIds.Value
            params.MonitoringMode = ua.MonitoringMode.Reporting
            params.RequestedParameters.ClientHandle = idx
            params.RequestedParameters.SamplingInterval = 500  # ms
            params.RequestedParameters.QueueSize = 1
            params.RequestedParameters.DiscardOldest = True
            subscription_slow.create_monitored_items([params])

        print("Monitoring started. Press Ctrl+C to stop...")
        while True:
            sleep(1)

    except KeyboardInterrupt:
        print("\nStopping by user request...")
    except Exception as e:
        logging.error("OPC UA 연결/구독 실패: %s", e)
        print("cannot connect OPC UA! " + str(e))
    finally:
        try:
            if subscription_fast is not None:
                subscription_fast.delete()
            if subscription_slow is not None:
                subscription_slow.delete()
        except Exception:
            pass
        try:
            if client is not None:
                client.disconnect()
        except Exception:
            pass
        logging.info("OPC UA Disconnected")

        # 워커 종료
        try:
            if writer is not None:
                writer.stop_and_join(drain=True, timeout=10)
        except Exception:
            pass

        print("Disconnected.")

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":  
        main()    