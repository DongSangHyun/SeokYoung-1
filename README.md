
 <C:\oracle\instantclient_23_9  : Python Oracle 접속 라이브 러리 ><br/>
  > Oracle 공식 사이트에서 Instant Client 다운로드
     https://www.oracle.com/database/technologies/instant-client/downloads.html
     “Windows x64” → “Basic Package (ZIP)” 버전 다운로드   
     다운로드한 ZIP 파일을 풀어 우측 폴더 등록  > C:\oracle\instantclient_21_13\  이폴더 가 있어야 함. .


     
파워쉘 실행파일 만들기 
& "C:\Users\LIHAON\AppData\Local\Programs\Python\Python314\python.exe" -m pip install pyinstaller
Set-Location "C:\Users\LIHAON\.vscode\workspace\Lihaon_OPC\LIHAON"
& "C:\Users\LIHAON\AppData\Local\Programs\Python\Python314\python.exe" -m PyInstaller --onefile --hidden-import=getpass OPC_dataAcqusitionV1.3.py


# 아래 폴더 에 복사 하여 테스트
C:\Users\LIHAON\.vscode\workspace