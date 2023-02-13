from django.urls import path
from keywordprod.views import (
    keywordprod_view,
)

app_name = 'keywordprod'

urlpatterns = [
    path('',view =keywordprod_view,name="keywordprod"),
]


