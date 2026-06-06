@echo off
echo ============================================================
echo   FRAUD OBSERVATORY - Lancement complet
echo ============================================================

echo [1/3] Demarrage Docker...
docker-compose -f docker/docker-compose.yml up -d
timeout /t 20 /nobreak

echo [2/3] Lancement du detector en arriere-plan...
start "Fraud Detector" C:\Users\dell\AppData\Local\Programs\Python\Python311\python.exe src\detection\fraud_detector.py

echo [3/3] Lancement du simulateur en arriere-plan...
start "Simulator" C:\Users\dell\AppData\Local\Programs\Python\Python311\python.exe src\simulator\simulator_continuous.py

timeout /t 5 /nobreak

echo [4/4] Lancement du dashboard...
C:\Users\dell\AppData\Local\Programs\Python\Python311\python.exe -m streamlit run src\dashboard.py
