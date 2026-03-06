# def test_loaded(self, client_no_amm, collection, new_user_fixture, pop_ledger_merge):
#     collection._client = client_no_amm
#
#     teardown_events(collection)
#     THL_LM.create_main_accounts()
#
#     for item in collection.items:
#         populate_events(item, user=new_user_fixture)
#         item.initial_load()
#
#     ddf = collection.ddf(
#         force_rr_latest=False,
#         include_partial=True,
#         filters=[
#             ("created", ">=", collection.start),
#             ("created", "<", collection.finished),
#         ],
#     )
#
#     assert isinstance(ddf, dd.DataFrame)
#     df = client_no_amm.compute(collections=ddf, sync=True)
#     assert isinstance(df, pd.DataFrame)
#
#     # Simple validation check(s)
#     assert not df.tx_id.is_unique
#     df["net"] = df.direction * df.amount
#     assert df.groupby("tx_id").net.sum().sum() == 0
#
#     teardown_events(collection)
#
#
#
