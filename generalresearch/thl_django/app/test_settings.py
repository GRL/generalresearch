DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": 'unittest-2026-09-03-728bcf',
        "USER": 'jenkins',
        "PASSWORD": '123456789',
        "HOST": 'unittest-postgresql.fmt2.grl.internal',
        "PORT": 5432,
    }
}
INSTALLED_APPS = ['django.contrib.postgres', 'django.contrib.contenttypes', 'generalresearch.thl_django']
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_L10N = True
USE_TZ = True
