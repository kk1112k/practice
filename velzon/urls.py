"""velzon URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path,include,re_path
from django.contrib.auth.decorators import login_required
from .views import MyPasswordChangeView, MyPasswordSetView
urlpatterns = [
    path('admin/', admin.site.urls),
    # Dashboard
    path('',include('dashboards.urls')),
    # Mscreening
    path('mscreening/',include('mscreening.urls')), 
    # keywordprod
    path('keywordprod/',include('keywordprod.urls')), 
    # Layouts
    path('layouts/',include('layouts.urls')),    
    # Pages
    path('pages/',include('pages.urls')),
    #account
    path(
        "account/password/change/",
        login_required(MyPasswordChangeView.as_view()),
        name="account_change_password",
    ),
    path(
        "account/password/set/",
        login_required(MyPasswordSetView.as_view()),
        name="account_set_password",
    ),
    # All Auth 
    path('account/', include('allauth.urls')),
    path('social-auth/',include('social_django.urls', namespace='social')),

    # file upload
    re_path(r'^fp/', include('django_drf_filepond.urls')),
]
