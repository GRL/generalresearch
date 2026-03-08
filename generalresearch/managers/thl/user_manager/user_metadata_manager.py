from typing import Collection, List, Optional

from generalresearch.managers.base import PostgresManager
from generalresearch.models.thl.user_profile import UserMetadata


class UserMetadataManager(PostgresManager):
    def filter(
        self,
        user_ids: Optional[Collection[int]] = None,
        email_addresses: Optional[Collection[str]] = None,
        email_sha256s: Optional[Collection[str]] = None,
        email_sha1s: Optional[Collection[str]] = None,
        email_md5s: Optional[Collection[str]] = None,
    ) -> List[UserMetadata]:
        for arg in [
            user_ids,
            email_addresses,
            email_sha256s,
            email_sha1s,
            email_md5s,
        ]:
            assert arg is None or isinstance(
                arg, (set, list)
            ), "must pass a collection of objects"

        filters = []
        params = {}

        if user_ids:
            params["user_id"] = list(set(user_ids))
            filters.append("user_id = ANY(%(user_id)s)")
        if email_addresses:
            params["email_address"] = list(set(email_addresses))
            filters.append("email_address = ANY(%(email_address)s)")
        if email_sha256s:
            params["email_sha256"] = list(set(email_sha256s))
            filters.append("email_sha256 = ANY(%(email_sha256)s)")
        if email_sha1s:
            params["email_sha1"] = list(set(email_sha1s))
            filters.append("email_sha1 = ANY(%(email_sha1)s)")
        if email_md5s:
            params["email_md5"] = list(set(email_md5s))
            filters.append("email_md5 = ANY(%(email_md5)s)")

        filter_str = "WHERE " + " AND ".join(filters) if filters else ""
        res = self.pg_config.execute_sql_query(
            f"""
        SELECT user_id, email_address, email_sha256, email_sha1, email_md5
        FROM thl_usermetadata
        {filter_str}
        """,
            params,
        )

        return [UserMetadata.from_db(**x) for x in res]

    def get_if_exists(
        self,
        user_id: Optional[int] = None,
        email_address: Optional[str] = None,
        email_sha256: Optional[str] = None,
        email_sha1: Optional[str] = None,
        email_md5: Optional[str] = None,
    ) -> Optional[UserMetadata]:
        filters = {
            "user_ids": user_id,
            "email_addresses": email_address,
            "email_sha256s": email_sha256,
            "email_sha1s": email_sha1,
            "email_md5s": email_md5,
        }
        filters = {k: [v] for k, v in filters.items() if v is not None}
        assert len(filters) == 1, "Exactly ONE filter argument must be provided."
        res = self.filter(**filters)
        if len(res) == 0:
            return None
        if len(res) > 1:
            raise ValueError("More than 1 result returned!")
        return UserMetadata.model_validate(res[0])

    def get(
        self,
        user_id: Optional[int] = None,
        email_address: Optional[str] = None,
        email_sha256: Optional[str] = None,
        email_sha1: Optional[str] = None,
        email_md5: Optional[str] = None,
    ) -> UserMetadata:
        res = self.get_if_exists(
            user_id=user_id,
            email_address=email_address,
            email_sha256=email_sha256,
            email_sha1=email_sha1,
            email_md5=email_md5,
        )
        if res is None:
            if user_id is not None:
                # We don't raise a "not found" here, b/c it just means that
                # nothing has been set for this user.
                return UserMetadata(user_id=user_id)
            else:
                # We are filtering, not looking up a user's info, so in this
                # case, nothing is found
                raise ValueError("not found")
        return res

    def update(self, user_metadata: UserMetadata) -> int:
        """
        The row in the thl_usermetadata might not exist. We'll
        implicitly create it if it doesn't yet exist. The caller
        does not need to know this detail.
        """
        res = self.get_if_exists(user_id=user_metadata.user_id)

        # We're assuming the user itself exists. There's a foreign key so the
        # db call will fail if it doesn't, so we don't need to check
        # it beforehand.
        if not res:
            return self._create(user_metadata=user_metadata)

        with self.pg_config.make_connection() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                UPDATE thl_usermetadata
                SET email_address = %(email_address)s, email_sha256 = %(email_sha256)s,
                email_sha1 = %(email_sha1)s, email_md5 = %(email_md5)s
                WHERE user_id = %(user_id)s;
                """,
                    params=user_metadata.to_db(),
                )
                rowcount = c.rowcount
            conn.commit()
        return rowcount

    def _create(self, user_metadata: UserMetadata) -> int:
        return self.pg_config.execute_write(
            query="""
            INSERT INTO thl_usermetadata
            (user_id, email_address, email_sha256, email_sha1, email_md5)
            VALUES (%(user_id)s, %(email_address)s, %(email_sha256)s, 
                    %(email_sha1)s, %(email_md5)s);
        """,
            params=user_metadata.to_db(),
        )
