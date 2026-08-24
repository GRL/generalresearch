from django.apps import AppConfig


class THLSchemaConfig(AppConfig):
    name = "generalresearch.thl_django"
    label = "thl_django"

    def ready(self):
        from .accounting import models  # pycharm: keep
        from .common import models  # pycharm: keep
        from .contest import models  # pycharm: keep
        from .event import models  # pycharm: keep
        from .marketplace import models  # pycharm: keep
        from .network import models  # pycharm: keep
        from .userhealth import models  # pycharm: keep
        from .userprofile import models  # noqa: F401  # pycharm: keep
