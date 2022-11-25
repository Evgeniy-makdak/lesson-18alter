import json

from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import CreateView, UpdateView, DeleteView

from rest_framework.generics import ListAPIView, RetrieveAPIView

from ads.models import Category, Ad
from django_Avito.utils import load_data_locations, load_data_users, load_data_cats, load_data_ads
from users.models import User
from ads.serializers import AdsListModelSerializer, CategoryModelSerializer


def index_ads(request):
    return JsonResponse({"status": "ok"}, status=200)


def csv_in_bd(request):
    """загружаем данные в БД"""
    return JsonResponse({
        "locations": str(load_data_locations()),
        "users": str(load_data_users()),
        "cats": str(load_data_cats()),
        "ads": str(load_data_ads()),
    })


class Ads_List_View(ListAPIView):
    queryset = Ad.objects.all()
    serializer_class = AdsListModelSerializer

    def get(self, request, *args, **kwargs):
        cat_num = request.GET.get('cat', None)
        text = request.GET.get('text', None)
        loc_name = request.GET.get('location', None)
        price_from, price_to = request.GET.get('price_from', None), request.GET.get('price_to', None)
        if cat_num:
            self.queryset = self.queryset.filter(category__id__exact=int(cat_num))
        if text:
            self.queryset = self.queryset.filter(name__icontains=text)
        if loc_name:
            self.queryset = self.queryset.filter(author__locations__name__icontains=loc_name)
        if price_from:
            self.queryset = self.queryset.filter(price__gte=int(price_from))
        if price_to:
            self.queryset = self.queryset.filter(price__lte=int(price_to))

        self.queryset = self.queryset.select_related('author').order_by("-price")

        return super().get(request, *args, **kwargs)


class Cats_List_View(ListAPIView):
    queryset = Category.objects.all()
    serializer_class = CategoryModelSerializer


class Ads_Detail_View(RetrieveAPIView):
    queryset = Ad.objects.all()
    serializer_class = AdsListModelSerializer


class Cats_Detail_View(RetrieveAPIView):
    queryset = Category.objects.all()
    serializer_class = CategoryModelSerializer


@method_decorator(csrf_exempt, name='dispatch')
class Ads_Create_View(CreateView):  # общий класс (кроме Ad) для создания записей (model задается в urls)
    def post(self, request, *args, **kwargs):
        add_data = json.loads(request.body)
        new_obj = self.model.objects.create(**add_data)
        dict_obj = vars(new_obj)
        dict_obj.pop('_state')
        return JsonResponse(dict_obj, status=200)


@method_decorator(csrf_exempt, name='dispatch')
class Ads_Ad_Create_View(CreateView):  # класс для создания Ad
    model = Ad
    fields = ["name", "author", "price", "description", "is_published", "category"]

    def post(self, request, *args, **kwargs):
        add_data = json.loads(request.body)
        author = get_object_or_404(User, add_data["author_id"])
        category = get_object_or_404(Category, add_data["category_id"])

        new_obj = Ad.objects.create(
            name=add_data["name"],
            author=author,
            price=add_data["price"],
            description=add_data["description"],
            is_published=add_data["is_published"],
            category=category,
        )
        dict_obj = vars(new_obj)
        try:
            dict_obj["image"] = dict_obj["image"].url
        except:
            dict_obj["image"] = None
        dict_obj.pop('_state')
        return JsonResponse(dict_obj, status=200)


@method_decorator(csrf_exempt, name='dispatch')
class Ads_Update_View(UpdateView):  # общий класс для изменения (model задается в urls)
    def patch(self, request, *args, **kwargs):
        apd_data = json.loads(request.body)
        self.model.objects.filter(id=kwargs["pk"]).update(**apd_data)

        new_obj = self.get_object()
        dict_obj = vars(new_obj)
        try:
            dict_obj["image"] = dict_obj["image"].url
        except:
            dict_obj["image"] = None
        dict_obj.pop('_state')
        return JsonResponse(dict_obj, status=200)


@method_decorator(csrf_exempt, name='dispatch')
class Ads_Delete_View(DeleteView):  # общий класс для удаления (model задается в urls)
    success_url = "/"

    def delete(self, request, *args, **kwargs):
        super().delete(request, *args, **kwargs)
        return JsonResponse({"status": "ok"}, status=200)


@method_decorator(csrf_exempt, name='dispatch')
class Ads_Image_View(UpdateView):  # работа с картинками
    def post(self, request, *args, **kwargs):
        self.object = self.get_object()

        self.object.image = request.FILES.get("image", None)
        image_url = None
        if self.object.image:
            image_url = self.object.image.url
        self.object.save()

        dict_obj = vars(self.object)
        dict_obj.update({"image": image_url})
        dict_obj.pop('_state')
        return JsonResponse(dict_obj, status=200)

