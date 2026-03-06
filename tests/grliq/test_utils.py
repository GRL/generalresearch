from pathlib import Path
from uuid import uuid4


class TestUtils:

    def test_get_screenshot_fp(self, mnt_grliq_archive_dir, utc_hour_ago):
        from generalresearch.grliq.utils import get_screenshot_fp

        fp1 = get_screenshot_fp(
            created_at=utc_hour_ago,
            forensic_uuid=uuid4(),
            grliq_archive_dir=mnt_grliq_archive_dir,
            create_dir_if_not_exists=True,
        )

        assert isinstance(fp1, Path)
