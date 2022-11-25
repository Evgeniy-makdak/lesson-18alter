from rest_framework import serializers

from ads.models import Ad, Category
from users.models import User


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






