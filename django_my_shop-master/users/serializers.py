from rest_framework import serializers
from users.models import User, Location


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
        qd = self.initial_data.copy()
        self._locs = qd.pop("locations")
        self.initial_data =qd
        result_valid = super().is_valid(raise_exception=raise_exception)
        qd.update({"locations":self._locs})
        self.initial_data = qd
        return result_valid

    def create(self, validated_data):
        validated_data.pop("locations")
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

class LocationModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Location
        fields = "__all__"