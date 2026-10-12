from django.apps import AppConfig


class GRSchemaConfig(AppConfig):
    name = "generalresearch.gr_django"
    label = "common"  # preserves the existing gr-carer migration identity
    default_auto_field = "django.db.models.BigAutoField"