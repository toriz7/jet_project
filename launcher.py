import sys
import os

# PyInstaller로 패키징된 경우 경로 설정
if getattr(sys, 'frozen', False):
    # exe로 실행된 경우
    base_path = sys._MEIPASS
    app_path = os.path.join(base_path, 'streamlit_app.py')
else:
    # 일반 Python으로 실행된 경우
    base_path = os.path.dirname(os.path.abspath(__file__))
    app_path = os.path.join(base_path, 'streamlit_app.py')

# 작업 디렉토리 설정
os.chdir(base_path)

# streamlit 실행
from streamlit.web import cli as stcli

sys.argv = [
    'streamlit', 'run', app_path,
    '--server.headless', 'true',
    '--global.developmentMode', 'false',
    '--browser.gatherUsageStats', 'false'
]
sys.exit(stcli.main())
