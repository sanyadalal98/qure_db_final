"""dashboard URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
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
from django.conf.urls import url
from stats.views import *

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^details', get_details),
    url(r'^chart_points', get_chart_points),
    url(r'^current_deployments_table', get_table_data),
    url(r'^onpremise_table', get_onpremise ),
    url(r'^map_locations', get_map_locations)
]
