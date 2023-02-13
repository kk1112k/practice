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
from datetime import datetime
from velzon.settings import DJANGO_DRF_FILEPOND_FILE_STORE_PATH, DJANGO_DRF_FILEPOND_UPLOAD_TMP
from django_drf_filepond.api import store_upload
from django_drf_filepond.api import get_stored_upload
from django_drf_filepond.api import get_stored_upload_file_data

from velzon.utils import getSqlData

class KeywordprodView(LoginRequiredMixin, TemplateView):
    def get(self, request):
         return render(request, 'keywordprod/index.html')

# Create your views here.
keywordprod_view = KeywordprodView.as_view()