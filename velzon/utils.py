import math
import pandas as pd
import boto3
import io
import os
from datetime import datetime
import awswrangler as wr

from dateutil.relativedelta import *

BUCKET = 'derma-material-screening'
IAM_ID = "AKIAYRBXPIB6Y4K42NZR"
IAM_PW = "bOTOJRFDUpgmnGb8JD9PhfycjSZFdlh2A5mmMd8d"
REGION_NAME = "ap-northeast-2"

s3_client = boto3.client('s3',
                         aws_access_key_id=IAM_ID,
                         aws_secret_access_key=IAM_PW,
                         region_name=REGION_NAME
                         )


def call_csv(bucket_nm, key_nm, **kwargs):
    obj = s3_client.get_object(Bucket=bucket_nm, Key=key_nm)
    data = obj['Body'].read()

    data = pd.read_csv(io.BytesIO(data), **kwargs)

    return data


session = boto3.Session(aws_access_key_id=IAM_ID,
                        aws_secret_access_key=IAM_PW,
                        region_name=REGION_NAME)


def getSqlData(query):
    """
    /**
    * 기능 : 쿼리 조회 후 dist 반환
    * @param {query} 조회 할 쿼리문
    * @param {dflist} 결과 레코드 dict

    */
    """
    try:
        qr = wr.athena.read_sql_query(query, database="dash", boto3_session=session)
        df = pd.DataFrame(qr)
        dflist = df.to_dict(orient='records')
        return dflist
    except:
        pass


def delete_old_files(path_target, days_elapsed):
    """
    /**
    * 기능 : DataFrame 경과일 지난 파일 삭제
    * @param {path_target} 삭제할 파일이 있는 디렉토리
    * @param {days_elapsed} 경과일수

    */
    """
    for f in os.listdir(path_target):  # 디렉토리를 조회한다
        f = os.path.join(path_target, f)
        if os.path.isfile(f):  # 파일이면
            timestamp_now = datetime.now().timestamp()  # 타임스탬프(단위:초)
            # st_mtime(마지막으로 수정된 시간)기준 X일 경과 여부
            is_old = os.stat(f).st_mtime < timestamp_now - \
                (days_elapsed * 24 * 60 * 60)
            if is_old:  # X일 경과했다면
                try:
                    os.remove(f)  # 파일을 지운다
                    print(f, 'is deleted')  # 삭제완료 로깅
                except OSError:  # Device or resource busy (다른 프로세스가 사용 중)등의 이유
                    print(f, 'can not delete')  # 삭제불가 로깅

def isNullChk(val):
    if (type(val) == str):
        return True if not val else False
    elif (type(val) == int):
        return True if not val else False
    elif (type(val) == None):
        return True
    elif (type(val) == float):
        return True if math.isnan(val) else False