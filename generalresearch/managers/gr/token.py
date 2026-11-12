import hashlib
import logging
import secrets

from generalresearch.managers.base import PostgresManager
from generalresearch.models.gr import GRUserRef
from generalresearch.models.gr.token import TokenCreated, TokenSummary

logger = logging.getLogger(__name__)


class TokenManager(PostgresManager):
    @staticmethod
    def generate_public_id() -> str:
        return f"tok_{secrets.token_urlsafe(16)}"

    @staticmethod
    def generate_token() -> tuple[str, str]:
        plain_token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(plain_token.encode()).hexdigest()
        return plain_token, token_hash

    def create_token(self, user_id: int, name: str = "Default") -> TokenCreated:
        name_dupe_query = """
        SELECT 1
        FROM gr_api_token
        WHERE user_id = %(user_id)s
            AND name = %(name)s
        """
        insert_query = """
        INSERT INTO gr_api_token (
            public_id, user_id, name, token_hash
        ) VALUES (
            %(public_id)s, %(user_id)s, %(name)s, %(token_hash)s
        ) RETURNING public_id AS id, name, created_at
        """
        with self.connection() as conn:
            with conn.cursor() as c:
                # We don't *need* to do this separately, as there is a unique
                #   constraint, but do it anyway so we can throw a
                #   meaningful error message.
                c.execute(
                    name_dupe_query,
                    {"user_id": user_id, "name": name},
                )
                if c.fetchone() is not None:
                    raise ValueError(f"Token name already exists: {name}")

                plain_token, token_hash = self.generate_token()
                params = {
                    "public_id": self.generate_public_id(),
                    "user_id": user_id,
                    "name": name,
                    "token_hash": token_hash,
                }
                c.execute(insert_query, params)
                row = c.fetchone()
            conn.commit()

        return TokenCreated(
            id=row["id"],
            name=row["name"],
            token=plain_token,
            created_at=row["created_at"],
        )

    def list_tokens(self, user_id: int) -> list[TokenSummary]:
        query = """
            SELECT
                t.public_id AS id,
                t.name,
                t.is_active,
                t.revoked_at,
                t.last_used_at,
                t.created_at
            FROM gr_api_token t
            WHERE t.user_id = %(user_id)s
            ORDER BY t.created_at DESC
        """
        rows = self.pg_config.execute_sql_query(query, {"user_id": user_id})
        return [TokenSummary.model_validate(row) for row in rows]

    def get_token(self, user_id: int, token_id: str) -> TokenSummary | None:
        query = """
            SELECT
                t.public_id AS id,
                t.name,
                t.is_active,
                t.revoked_at,
                t.last_used_at,
                t.created_at
            FROM gr_api_token t
            WHERE t.user_id = %(user_id)s
                AND t.public_id = %(token_id)s
        """
        rows = self.pg_config.execute_sql_query(
            query, {"user_id": user_id, "token_id": token_id}
        )
        return TokenSummary.model_validate(rows[0]) if rows else None

    def revoke_token(self, user_id: int, token_id: str) -> TokenSummary:
        query = """
            UPDATE gr_api_token t
            SET
                is_active = FALSE,
                revoked_at = COALESCE(revoked_at, NOW())
            WHERE t.user_id = %(user_id)s
                AND t.public_id = %(token_id)s
            RETURNING
                t.public_id AS id,
                t.name,
                t.is_active,
                t.revoked_at,
                t.last_used_at,
                t.created_at
        """
        with self.connection() as conn, conn.cursor() as c:
            c.execute(query, {"user_id": user_id, "token_id": token_id})
            row = c.fetchone()
            if row is None:
                raise ValueError(f"Token not found: {token_id}")
            conn.commit()
            return TokenSummary.model_validate(row)

    def validate_token_hash(self, token_hash: str) -> tuple[TokenSummary, GRUserRef]:
        query = """
        SELECT
            t.id AS token_pk,
            t.public_id AS token_id,
            t.token_hash,
            t.is_active,
            t.created_at as token_created_at,
            t.name,
            u.id AS user_id,
            u.name AS user_name,
            u.email AS user_email,
            u.is_superuser,
            u.date_joined,
            u.avatar_url
        FROM gr_api_token t
             JOIN gr_user u ON u.id = t.user_id
        WHERE t.token_hash = %(token_hash)s
          AND t.is_active = TRUE
          AND t.revoked_at IS NULL
          -- AND u.is_enabled = TRUE
        LIMIT 1
        """

        with self.connection() as conn, conn.cursor() as c:
            c.execute(query, {"token_hash": token_hash})
            row = c.fetchone()

            if row is None or not secrets.compare_digest(token_hash, row["token_hash"]):
                logger.warning(f"Invalid token: {token_hash}")
                raise ValueError("Invalid token")

            c.execute(
                """
                    UPDATE gr_api_token
                    SET last_used_at = NOW()
                    WHERE id = %(token_pk)s
                    """,
                {"token_pk": row["token_pk"]},
            )
        token = TokenSummary(
            id=row["token_id"],
            name=row["name"],
            is_active=row["is_active"],
            created_at=row["token_created_at"],
        )
        user = GRUserRef(
            email=row["user_email"],
            name=row["user_name"],
            id=row["user_id"],
            avatar_url=row["avatar_url"],
            is_superuser=row["is_superuser"],
            date_joined=row["date_joined"],
        )
        return token, user
