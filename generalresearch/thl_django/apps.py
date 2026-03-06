from django.apps import AppConfig


class THLSchemaConfig(AppConfig):
    name = "generalresearchutils.thl_django"
    label = "thl_django"

    def ready(self):
        from .accounting import models  # noqa: F401  # pycharm: keep
        from .common import models  # noqa: F401  # pycharm: keep
        from .contest import models  # noqa: F401  # pycharm: keep
        from .event import models  # noqa: F401  # pycharm: keep
        from .marketplace import models  # noqa: F401  # pycharm: keep
        from .userhealth import models  # noqa: F401  # pycharm: keep
        from .userprofile import models  # noqa: F401  # pycharm: keep
