from rest_framework import serializers

from ads.models import Ad, Category
from users.models import Location, User


class AdsListModelSerializer(serializers.ModelSerializer):
    category = serializers.SlugRelatedField(
        required=False,
        queryset=Category.objects.all(),
        slug_field="name",
    )
    author = serializers.SlugRelatedField(
        required=False,
        queryset=User.objects.all(),
        slug_field="username",
    )

    class Meta:
        model = Ad
        fields = "__all__"


class CategoryModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Category
        fields = "__all__"


class LocationModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Location
        fields = "__all__"


class UserModelSerializer(serializers.ModelSerializer):
    locations = serializers.SlugRelatedField(many=True, read_only=True, slug_field="name")

    class Meta:
        model = User
        fields = "__all__"


class UserCreateModelSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(required=False)
    locations = serializers.SlugRelatedField(
        many=True,
        required=False,
        queryset=Location.objects.all(),
        slug_field="name",
    )

    class Meta:
        model = User
        fields = "__all__"

    def is_valid(self, *, raise_exception=False):
        self._locs = self.initial_data.pop("locations")
        return super().is_valid(raise_exception=raise_exception)

    def create(self, validated_data):
        new_obj = User.objects.create(**validated_data)
        for loc in self._locs:
            location, _ = Location.objects.get_or_create(name=loc)
            new_obj.locations.add(location)
        new_obj.save()
        return new_obj


class UserUpdateModelSerializer(serializers.ModelSerializer):
    locations = serializers.SlugRelatedField(
        queryset=Location.objects.all(),
        many=True,
        slug_field="name"
    )

    def is_valid(self, raise_exception=False):
        self._locs = self.initial_data.pop("locations")
        return super().is_valid(raise_exception=raise_exception)

    def save(self):
        upd_user = super().save()
        upd_user.locations.clear()
        for loc in self._locs:
            obj, _ = Location.objects.get_or_create(name=loc)
            upd_user.locations.add(obj)
        return upd_user

    class Meta:
        model = User
        fields = '__all__'


class UserDeleteModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ["id"]
