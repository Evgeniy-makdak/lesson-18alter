from rest_framework import viewsets
from rest_framework.generics import CreateAPIView, ListAPIView, RetrieveAPIView, UpdateAPIView, DestroyAPIView

from users.models import User, Location
from users.serializers import UserCreateModelSerializer, UserModelSerializer, UserUpdateModelSerializer, \
    UserDeleteModelSerializer, LocationModelSerializer


class Users_List_View(ListAPIView):
    queryset = User.objects.all()
    serializer_class = UserModelSerializer


class Users_Detail_View(RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserModelSerializer


class Users_Create_View(CreateAPIView):
    queryset = User.objects.all()
    serializer_class = UserCreateModelSerializer

class Users_Update_View(UpdateAPIView):
    queryset = User.objects.all()
    serializer_class = UserUpdateModelSerializer

class Users_Delete_View(DestroyAPIView):
    queryset = User.objects.all()
    serializer_class = UserDeleteModelSerializer

class LocationsViewSet(viewsets.ModelViewSet):
    queryset = Location.objects.all()
    serializer_class = LocationModelSerializer


