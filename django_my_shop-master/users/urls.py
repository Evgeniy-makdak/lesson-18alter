from django.urls import path

from users import views

urlpatterns = [  # у всех путей "user/+"
    # спикок
    path('', views.Users_List_View.as_view()),
    # детальные
    path('<int:pk>/', views.Users_Detail_View.as_view()),
    # создание
    path('create/', views.Users_Create_View.as_view()),
    # изменение
    path('<int:pk>/update/', views.Users_Update_View.as_view()),
    #  удаление
    path('<int:pk>/delete/', views.Users_Delete_View.as_view()),

]
