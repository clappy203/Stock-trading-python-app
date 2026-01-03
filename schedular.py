import schedular
import time
from script import run_stock_job
from datetime import datetime

def basic_job():
    print("Job started at", datetime.now())

#run job every minute 
schedular.every().minute.do(basic_job)
schedular.every().minute.do(run_stock_job)

while True:
    schedular.run_pending()
    time.sleep(1)
