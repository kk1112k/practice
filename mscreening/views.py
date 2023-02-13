import shutil
import os
import json
import pandas as pd
import numpy as np

from django.views.decorators.http import require_GET, require_POST
from django.shortcuts import render
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin
from django.http import JsonResponse, HttpResponse, Http404
from mscreening.services import Full_code_fin
from datetime import datetime
from velzon.settings import DJANGO_DRF_FILEPOND_FILE_STORE_PATH, DJANGO_DRF_FILEPOND_UPLOAD_TMP
from django_drf_filepond.api import store_upload
from django_drf_filepond.api import get_stored_upload
from django_drf_filepond.api import get_stored_upload_file_data

from velzon.utils import getSqlData

# Create your views here.


class MscreeningView(LoginRequiredMixin, TemplateView):
    def get(self, request):
        result = {"dbUpdateList": [
            [
                "한국화장품협회 성분사전", "2023-02-03", "1800건", "수시"
            ],
            [
                "2021년 중국화장품기사용 목록", "2015-01-01", "1200건", "X"
            ],
            [
                "2021년 중국화장품안전기술규범", "2015-01-01", "2342건", "X"
            ],
            [
                "원료사용목적", "2022-10-01", "1078건", "X"
            ],
            [
                "Global CITES", "2023-02-03", "2344건", "수시"
            ],
            [
                "중국위기동식물", "2023-02-03", "7335건", "수시"
            ],
            [
                "EWG 사이트", "2023-02-03", "2342건", "수시"
            ],
            [
                "화해", "2023-01-31", "987건", "분기별"
            ],
            [
                "화메이리슈싱", "2023-02-03", "766건", "수시"
            ],
            [
                "EU CI Number DB", "2023-02-03", "86556건", "수시"
            ],
            [
                "CIR 논문자료", "2023-02-03", "4022건", "수시"
            ]
        ]}
        return render(request, 'mscreening/index.html', result)


class DBSearchView(LoginRequiredMixin, TemplateView):
    def get(self, request):
        result = {"dbUpdateList": [
            [
                "한국화장품협회 성분사전", "2023-02-03", "1800건", "수시"
            ],
            [
                "2021년 중국화장품기사용 목록", "2015-01-01", "1200건", "X"
            ],
            [
                "2021년 중국화장품안전기술규범", "2015-01-01", "2342건", "X"
            ],
            [
                "원료사용목적", "2022-10-01", "1078건", "X"
            ],
            [
                "Global CITES", "2023-02-03", "2344건", "수시"
            ],
            [
                "중국위기동식물", "2023-02-03", "7335건", "수시"
            ],
            [
                "EWG 사이트", "2023-02-03", "2342건", "수시"
            ],
            [
                "화해", "2023-01-31", "987건", "분기별"
            ],
            [
                "화메이리슈싱", "2023-02-03", "766건", "수시"
            ],
            [
                "EU CI Number DB", "2023-02-03", "86556건", "수시"
            ],
            [
                "CIR 논문자료", "2023-02-03", "4022건", "수시"
            ]
        ]}
        return render(request, 'mscreening/dbsearch.html', result)


mscreening_view = MscreeningView.as_view()
dbsearch_view = DBSearchView.as_view(template_name="mscreening/dbsearch.html")


@require_POST  # 해당 뷰는 POST method 만 받는다.
def getExcelParse(request):
    # POST 요청일 때
    if request.method == 'POST':
        data = json.loads(request.body)
        result = {}
        files = os.listdir(os.path.join(
            DJANGO_DRF_FILEPOND_UPLOAD_TMP, data["key"]))
        for file in files:
            filename = os.path.join(os.path.join(
                DJANGO_DRF_FILEPOND_UPLOAD_TMP, data["key"]), file)
            dirPath = os.path.join(DJANGO_DRF_FILEPOND_UPLOAD_TMP, data["key"])
           
            os.rename(filename, filename + '.xlsx')
            result = Full_code_fin(filename + '.xlsx')
            shutil.rmtree(dirPath)

            # try:
            #     os.rename(filename, filename + '.xlsx')
            #     result = Full_code_fin(filename + '.xlsx')
            #     shutil.rmtree(dirPath)
            # except:
            #     print('error')
            #     pass

            result["file_id"] = os.path.basename(filename)
        return JsonResponse(result)

# @require_POST # 해당 뷰는 POST method 만 받는다.


def excelDownload(request):
    file = request.GET.get('filename') + ".xlsx"
    filename = os.path.join(DJANGO_DRF_FILEPOND_FILE_STORE_PATH, file)
    if os.path.exists(filename):
        f = open(filename, 'rb')
        response = HttpResponse(
            f.read(), content_type="application/vnd.ms-excel")
        downName = "screening_file - " + datetime.today().strftime("%Y%m%d%H%M%S") + ".xlsx"
        response['Content-Disposition'] = 'inline; filename=' + downName
        f.close()
        # os.remove(filename)
        return response
    raise Http404


def getDbSearch(request):
    search = request.GET.get('search')
    query = """
    SELECT KCIACODE                          AS KCIA_CD   /* 성분코드                                   */
      ,TRIM(CAST(KORNAME    AS VARCHAR)) AS KR_NM     /* 국문 성분명                                */
      ,TRIM(CAST(ENGNAME    AS VARCHAR)) AS EN_NM     /* 영문 성분명                                */
      ,TRIM(CAST(CHINNAME   AS VARCHAR)) AS CN_NM     /* 중문 성분명                                */
      ,TRIM(CAST(CASNO      AS VARCHAR)) AS CAS_NO    /* CAS No.                                    */
      ,TRIM(CAST(OLDNAME    AS VARCHAR)) AS OLD_NM    /* 구 성분명                                  */
      ,TRIM(CAST(USECHINA   AS VARCHAR)) AS USE_CN    /* 중국사용가능물질                           */
      ,TRIM(CAST(WASHOFF    AS VARCHAR)) AS WASH_OFF  /* 씻어내는 제품 중 최고 역사 사용량（%）     */
      ,TRIM(CAST(LEAVEON    AS VARCHAR)) AS LEAV_ON   /* 씻어내지 않는 제품 중 최고 역사 사용량（%）*/
      ,TRIM(CAST(LIMITMAT   AS VARCHAR)) AS LIMT_MAT  /* 사용제한물질                               */
      ,TRIM(CAST(NAME_1     AS VARCHAR)) AS NM_1      /* 성분코드                                   */
      ,TRIM(CAST(NAME_2     AS VARCHAR)) AS NM_2      /* 성분코드                                   */
      ,TRIM(CAST(EWGDATA    AS VARCHAR)) AS EWG_DATE  /* EWG                                        */
      ,TRIM(CAST(HWAHAEDATA AS VARCHAR)) AS HWA_DATA  /* 화해                                       */
      ,TRIM(CAST(MEIDATA    AS VARCHAR)) AS MEI_DATA  /* 메이리슈싱                                 */
  FROM MAT_SCREENING.SEARCH_DB
 WHERE UPPER(KORNAME)  LIKE UPPER('%{0}%')
    OR UPPER(ENGNAME)  LIKE UPPER('%{0}%')
    OR UPPER(CHINNAME) LIKE UPPER('%{0}%')
LIMIT 100
    """
    at_data = getSqlData(query.format(search))
    results = {}
    results["results"] = at_data
    results["count"] = len(at_data)
    return JsonResponse(results)
