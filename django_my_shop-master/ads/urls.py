from django.urls import path

from ads import views
from ads.models import Category, Ad


urlpatterns = [
    path('', views.index_ads),

    # списковые
    path('cat/', views.Cats_List_View.as_view()),
    path('ad/', views.Ads_List_View.as_view()),
    # детальные
    path('cat/<int:pk>/', views.Cats_Detail_View.as_view()),
    path('ad/<int:pk>/', views.Ads_Detail_View.as_view()),
    # создание
    path('cat/create/', views.Ads_Create_View.as_view(model=Category)),
    path('ad/create/', views.Ads_Ad_Create_View.as_view()),
    # изменение
    path('cat/<int:pk>/update/', views.Ads_Update_View.as_view(model=Category)),
    path('ad/<int:pk>/update/', views.Ads_Update_View.as_view(model=Ad)),
    # удаление
    path('cat/<int:pk>/delete/', views.Ads_Delete_View.as_view(model=Category)),
    path('ad/<int:pk>/delete/', views.Ads_Delete_View.as_view(model=Ad)),
    # работа с картинками
    path('ad/<int:pk>/image/', views.Ads_Image_View.as_view(model=Ad)),

]
