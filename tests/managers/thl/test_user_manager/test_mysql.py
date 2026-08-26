from __future__ import annotations

from generalresearch.managers.thl.user_manager.mysql_user_manager import (
    MysqlUserManager,
)
from generalresearch.models.thl.user import User


class TestUserManagerMysqlNew:

    def test_get_notset(self, mysql_user_manager: MysqlUserManager):
        assert mysql_user_manager.get_user_from_mysql(user_id=-3105) is None

    def test_get_user_id(self, user: User, mysql_user_manager: MysqlUserManager):
        assert mysql_user_manager.get_user_from_mysql(user_id=user.user_id) == user

    def test_get_uuid(self, user: User, mysql_user_manager: MysqlUserManager):
        u = mysql_user_manager.get_user_from_mysql(user_uuid=user.uuid)
        assert u == user

    def test_get_ubp(self, user: User, mysql_user_manager: MysqlUserManager):
        u = mysql_user_manager.get_user_from_mysql(
            product_id=user.product_id, product_user_id=user.product_user_id
        )
        assert u == user
