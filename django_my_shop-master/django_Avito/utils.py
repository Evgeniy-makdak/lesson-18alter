import csv

from ads.models import Ad, Category
from users.models import User, Location

# загрузка данных а БД

def load_data_cats():
    with open(f'datasets/category.csv', encoding="utf-8") as csvfile:
        rows_data = list(csv.DictReader(csvfile))
    for row_data in rows_data:
        cat = Category(
            name=row_data["name"],
        )
        try:
            cat.save()
        except:
            return False
    return True


def load_data_ads():
    with open(f'datasets/ad.csv', encoding="utf-8") as csvfile:
        rows_data = list(csv.DictReader(csvfile))
    for row_data in rows_data:
        ad = Ad(
            name=row_data["name"],
            price=int(row_data["price"]),
            description=row_data["description"],
            is_published=bool(row_data["is_published"]),
            image=row_data["image"],
            author=User.objects.get(id=int(row_data["author_id"])),
            category=Category.objects.get(id=int(row_data["category_id"])),
        )
        try:
            ad.save()
        except:
            return False
    return True

def load_data_locations():
    with open(f'datasets/location.csv', encoding="utf-8") as csvfile:
        rows_data = list(csv.DictReader(csvfile))
    for row_data in rows_data:
        loc = Location(
            name=row_data["name"],
            lat=float(row_data["lat"]),
            lng=float(row_data["lng"]),
        )
        try:
            loc.save()
        except:
            return False
    return True


def load_data_users():
    with open(f'datasets/user.csv', encoding="utf-8") as csvfile:
        rows_data = list(csv.DictReader(csvfile))
    for row_data in rows_data:
        location = Location.objects.filter(id=int(row_data["location_id"]))
        user = User(
            first_name=row_data["first_name"],
            last_name=row_data["last_name"],
            username=row_data["username"],
            password=row_data["password"],
            age=int(row_data["age"]),
        )
        try:
            user.save()
            user.locations.add(*location)
        except:
            return False

    return True
