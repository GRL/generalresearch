from django.apps import AppConfig


class THLSchemaConfig(AppConfig):
    name = "generalresearch.thl_django"
    label = "thl_django"

    def ready(self):
        from .accounting import models  # noqa: F401
        from .common import models  # noqa: F401
        from .contest import models  # noqa: F401
        from .event import models  # noqa: F401
        from .geo import models  # noqa: F401
        from .marketplace import models  # noqa: F401
        from .network import models  # noqa: F401
        from .userhealth import models  # noqa: F401
        from .userprofile import models  # noqa: F401
