from django.urls import path
from mscreening.views import (
    getExcelParse,
    excelDownload,
    getDbSearch,
    mscreening_view,
    dbsearch_view,
)

app_name = 'mscreening'

urlpatterns = [
    path('',view =mscreening_view,name="mscreening"),
    path('dbsearch',view =dbsearch_view,name="dbsearch"),
    path('getDbSearch',view =getDbSearch,name="getDbSearch"),
    path('getExcelParse',view =getExcelParse,name="getExcelParse"),
    path('excelDownload',view =excelDownload,name="excelDownload"),
]


