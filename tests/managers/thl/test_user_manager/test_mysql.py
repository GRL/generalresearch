from test_utils.models.conftest import user, user_manager


class TestUserManagerMysqlNew:

    def test_get_notset(self, user_manager):
        assert (
            user_manager.mysql_user_manager.get_user_from_mysql(user_id=-3105) is None
        )

    def test_get_user_id(self, user, user_manager):
        assert (
            user_manager.mysql_user_manager.get_user_from_mysql(user_id=user.user_id)
            == user
        )

    def test_get_uuid(self, user, user_manager):
        u = user_manager.mysql_user_manager.get_user_from_mysql(user_uuid=user.uuid)
        assert u == user

    def test_get_ubp(self, user, user_manager):
        u = user_manager.mysql_user_manager.get_user_from_mysql(
            product_id=user.product_id, product_user_id=user.product_user_id
        )
        assert u == user
