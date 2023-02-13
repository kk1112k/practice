from django.urls import path
from dashboards.views import (
    dashboard_view,
    summary_view,
    tmall_china_view,
    tmall_global_view,
    douyin_china_view,
    douyin_global_view
)

app_name = 'dashboards'

urlpatterns = [
    path('',view =dashboard_view,name="dashboard"),
    path('summary',view =summary_view,name="summary"),
    path('tmallchina',view =tmall_china_view,name="tmallchina"),
    path('tmallglobal',view =tmall_global_view,name="tmallglobal"),
    path('douyinchina',view =douyin_china_view,name="douyinchina"),
    path('douyinglobal',view =douyin_global_view,name="douyinglobal"),
   
]


