from django.contrib import admin

from users.models import Location, User

admin.site.register(User)
admin.site.register(Location)
