< Project Hierarchy > - 생성 후 폴더 및 파일 이름 반영해주세요<br/>
```
bosungAgent(root)
    ├── .venv : 가상환경 관련 디렉토리
    │
    ├── SeokYoung : SeokYoung OPC
    │   ├── log_files : 에러 로그 디렉토리
    │   ├── SeokYoung_OPC.py : 계측 데이터 수집 Agent
    │   └── initList.py : OPC Tag, MSSQL 테이블 컬럼, DevideId, EquipmentId 등 정보
    │
    ├── ...
    └── requirements.txt : 가상환경에 설치된 라이브러리 목록
```
<br/>
< Git Commit 시 주의할 점 ><br/>
라이브러리를 새로 설치하거나 버전을 변경하였을 경우, requirements.txt에 반영 필수!<br/>
- Pycharm 하단 터미널 or 사용하는 IDE의 터미널에서 pip freeze > requirements.txt 입력<br/>
<br/>
< Git Pull 시 주의할 점 ><br/>
requirements.txt 가 변경된 Pull을 받으면 라이브러리 설치<br/>
- Pycharm 하단 터미널 or 사용하는 IDE의 터미널에서 pip install -r requirements.txt 입력<br/>
<br/>
< pyinstaller로 생성한 실행파일이 바이러스로 인식될 때><br/>
'--clean --noupx' 추가하기<br/>
예시) pyinstaller --onefile --clean --noupx myAgent.py
