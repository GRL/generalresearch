# from datetime import datetime, timezone
# from typing import Optional
#
# from generalresearch.managers.gr.authentication import GRUserManager
# from generalresearch.managers.thl.user_manager.user_manager import UserManager
# from generalresearch.models.custom_types import AwareDatetimeISO, UUIDStr
# from pydantic import BaseModel, Field, PositiveInt, model_validator
# from pydantic.json_schema import SkipJsonSchema
#
# from api.decorators import THL_WEB_RR, GR_DB
#
# GR_UM = GRUserManager(sql_helper=GR_DB)
# UM = UserManager(sql_helper_rr=THL_WEB_RR)
#

# @pytest.mark.skip(reason="moving to pyutils 2.5.1")
# class TestUserDeleteRequestManager:
#
#     def test_delete_request(self, gr_user, user, product, user_manager, gr_um):
#         from api.models.product_user import DeleteRequest
#         from api.managers.product_user import UserDeletionRequestManager
#
#         # A valid Respondent and GR Admin account need to exist in the test
#         #   database for any of this to work
#         user = user_manager.create_dummy(
#             product_id=product.id,
#             product_user_id=f"test-{uuid4().hex[:6]}",
#         )
#
#         instance = DeleteRequest(
#             product_id=user.product_id,
#             product_user_id=user.product_user_id,
#             created_by_user_id=gr_user.id,
#         )
#
#         start: int = UserDeletionRequestManager().get_count_by_product_id(
#             product_id=user.product_id
#         )
#
#         UserDeletionRequestManager.save(deletion_request=instance)
#
#         finish: int = UserDeletionRequestManager().get_count_by_product_id(
#             product_id=user.product_id
#         )
#
#         assert finish == start + 1


# @pytest.mark.skip(reason="Moving to py-utils in 2.5.1")
# class TestProductUserDeleteRequest:
#
#     def test_no_user_provided(self, product, business, team, gr_user):
#         from api.models.product_user import DeleteRequest
#
#         # product_id and product_user_id is required
#         with pytest.raises(expected_exception=ValueError) as cm:
#             DeleteRequest(created_by_user_id=gr_user.id)
#
#         assert "2 validation errors" in str(cm.value)
#
#     def test_no_user_exists(self, gr_user, product):
#         from api.models.product_user import DeleteRequest
#
#         with pytest.raises(expected_exception=ValueError) as cm:
#             DeleteRequest(
#                 product_id=product.id,
#                 product_user_id=f"test-user-{uuid4().hex[:12]}",
#                 created_by_user_id=gr_user.id,
#             )
#
#         assert "Could not find Worker" in str(cm.value)
#
#     def test_no_create_by_user(self, user, product):
#         from api.models.product_user import DeleteRequest
#
#         with pytest.raises(expected_exception=ValueError) as cm:
#             DeleteRequest(
#                 product_id=user.product_id,
#                 product_user_id=user.product_user_id,
#                 created_by_user_id=randint(a=999_999, b=999_999_999),
#             )
#         assert "GRUser not found" in str(cm.value)


#
# class DeleteRequest(BaseModel):
#     id: SkipJsonSchema[Optional[PositiveInt]] = Field(default=None, exclude=True)
#     uuid: UUIDStr = Field(examples=[uuid4().hex], default_factory=lambda: uuid4().hex)
#
#     product_id: UUIDStr = Field(examples=["00e96773d4ae47f8812488a976a080c8"])
#     product_user_id: str = Field(
#         min_length=3, max_length=128, examples=["bpuid-68d989"]
#     )
#
#     created: AwareDatetimeISO = Field(
#         default=datetime.now(tz=timezone.utc),
#         description="When the DeleteRequest was created, this is the UTC time "
#                     "that a Worker / Respondent's Profiling Questions were "
#                     "deleted.",
#     )
#     created_by_user_id: SkipJsonSchema[PositiveInt] = Field(exclude=True)
#
#     @model_validator(mode="after")
#     def check_valid_worker(self) -> "DeleteRequest":
#         """ Raise an error if the User that the GRUser is attempting to delete
#             does not actually exist in the system. We can check the production
#             thl-web user table here for real time users
#         """
#         user = UM.get_user_if_exists(
#             product_id=self.product_id, product_user_id=self.product_user_id
#         )
#
#         if not user:
#             raise ValueError("Could not find Worker")
#
#         return self
#
#     @model_validator(mode="after")
#     def check_valid_owner(self) -> "DeleteRequest":
#         """ Ensure we can track which GRUser made a deletion request so we can
#             track the chain of command for who took what action.
#
#         """
#         gr_user = GR_UM.get_by_id(gr_user_id=self.created_by_user_id)
#
#         if not gr_user:
#             raise ValueError("Could not find General Research account")
#
#         return self


# @staticmethod
# def save(deletion_request: DeleteRequest) -> bool:
#     with GR_DB.make_connection() as conn:
#         with conn.cursor(row_factory=dict_row) as c:
#             c: Cursor
#
#             c.execute(
#                 query=f"""
#                     INSERT INTO product_user_deleterequest
#                         (uuid, product_id, product_user_id, created,
#                          created_by_user_id)
#                     VALUES (%s, %s, %s, %s, %s)
#                 """,
#                 params=[
#                     deletion_request.uuid,
#                     deletion_request.product_id,
#                     deletion_request.product_user_id,
#                     deletion_request.created,
#                     deletion_request.created_by_user_id,
#                 ],
#             )
#
#             conn.commit()
#
#     return True
#
#
# @staticmethod
# def get_count_by_product_id(product_id: UUIDStr) -> NonNegativeInt:
#     with GR_DB.make_connection() as conn:
#         with conn.cursor(row_factory=dict_row) as c:
#             c: Cursor
#
#             c.execute(
#                 query=f"""
#                     SELECT COUNT(1) as cnt
#                     FROM product_user_deleterequest AS dr
#                     WHERE dr.product_id = %s
#                 """,
#                 params=[
#                     product_id,
#                 ],
#             )
#             res = c.fetchall()
#
#     assert len(res) == 1, "invalid query"
#     return int(res[0]["cnt"])
