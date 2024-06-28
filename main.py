from selenium import webdriver
from datetime import datetime, timedelta
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.alert import Alert
from selenium.webdriver.chrome import service as fs
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import requests
import time
import pickle
import pandas as pd
import re
import boto3
import io
from tempfile import mkdtemp


def load_pickle_from_s3(bucket_name, file_key):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        body = response['Body'].read()
        return pickle.loads(body)
    except Exception as e:
        print(f"Error reading {file_key} from bucket {bucket_name}: {e}")
        return None


# 処理を行う関数
def lambda_handler(event, context):
    options = webdriver.ChromeOptions()
    service = webdriver.ChromeService("/opt/chromedriver")

    options.binary_location = '/opt/chrome/chrome'
    options.add_argument("--headless=new")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280x1696")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--no-zygote")
    options.add_argument(f"--user-data-dir={mkdtemp()}")
    options.add_argument(f"--data-path={mkdtemp()}")
    options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    options.add_argument("--remote-debugging-port=9222")
    driver = webdriver.Chrome(options=options, service=service)

    bucket_name="layerk"
    file_key="ol.pkl"
    
    ol=[]
    id_list=load_pickle_from_s3(bucket_name, file_key)
    base_url = 'https://race.netkeiba.com/odds/index.html'
    for id in id_list:
        param_race_id = str(id)[:4]+str(id)[-8:]
        url = f'{base_url}?type=b8&race_id={param_race_id}&housiki=C2'
        try:
            driver.get(url)
            # 全選択ボタンをクリック（要素が見つかるまで最大10秒間待つ）
            wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "label[for='btn_sel_1'].All_Action_Button"))).click()
            # 確定ボタンをクリック
            wait.until(EC.element_to_be_clickable((By.XPATH, "//*[@id='odds_view_form']/div[2]/div[2]/div[2]/div[1]/button"))).click()
            time.sleep(10)
            # テーブルの内容を取得
            innerHTML = wait.until(EC.presence_of_element_located((By.ID, "sort_table"))).get_attribute('outerHTML')
            df_list = pd.read_html(innerHTML)
            ol.append(df_list)
        except TimeoutException as e:
            print(f"タイムアウトエラー: {id} - {str(e)}")
        except Exception as e:
            print(f"その他のエラーが発生しました。race_id: {id} - {str(e)}")
        finally:
            driver.quit()
            driver = webdriver.Chrome(options=options, service=service)
    do=pd.concat(ol)
    # DataFrameをpickle形式でバイナリデータに変換
    pickle_buffer = io.BytesIO()
    do.to_pickle(pickle_buffer)
    pickle_buffer.seek(0)
    # S3バケットにpickleファイルをアップロード
    s3 = boto3.client('s3')
    bucket_name = 'layerk'
    object_name = 'file.pkl'
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=pickle_buffer)
    return {
        'statusCode': 200,
        'body': json.dumps('DataFrame has been successfully saved to S3 as a pickle file.')
    }
