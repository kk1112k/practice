from django.shortcuts import render
from django.views.generic import TemplateView
from django.contrib.auth.mixins import LoginRequiredMixin

# Create your views here.
class DashboardView(LoginRequiredMixin,TemplateView):
    pass
    
dashboard_view = DashboardView.as_view(template_name="dashboards/index.html")
summary_view = DashboardView.as_view(template_name="dashboards/summary.html")
tmall_china_view = DashboardView.as_view(template_name="dashboards/tmall_china.html")
tmall_global_view = DashboardView.as_view(template_name="dashboards/tmall_global.html")
douyin_china_view = DashboardView.as_view(template_name="dashboards/douyin_china.html")
douyin_global_view = DashboardView.as_view(template_name="dashboards/douyin_global.html")