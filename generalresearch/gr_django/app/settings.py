import os

INSTALLED_APPS = [
    "django.contrib.postgres",
    "django.contrib.contenttypes",
    "generalresearch.gr_django.apps.GRSchemaConfig",
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": os.environ.get("GR_DB_NAME", "gr"),
        "USER": os.environ.get("GR_DB_USER", "postgres"),
        "PASSWORD": os.environ.get("GR_DB_PASSWORD", "password"),
        "HOST": os.environ.get("GR_DB_HOST", "127.0.0.1"),
        "PORT": os.environ.get("GR_DB_PORT", "5432"),
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True