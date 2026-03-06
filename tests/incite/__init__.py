# class TestParquetBehaviors(CleanTempDirectoryTestCls):
#     wall_coll = WallDFCollection(
#         start=GLOBAL_VARS["wall"].start,
#         offset="49h",
#         archive_path=f"{settings.incite_mount_dir}/raw/df-collections/{DFCollectionType.WALL.value}",
#     )
#
#     def test_filters(self):
#         # Using REAL data here
#         start = datetime(year=2024, month=1, day=15, hour=12, tzinfo=timezone.utc)
#         end = datetime(year=2024, month=1, day=15, hour=20, tzinfo=timezone.utc)
#         end_max = datetime(
#             year=2024, month=1, day=15, hour=20, tzinfo=timezone.utc
#         ) + timedelta(hours=2)
#
#         ir = pd.Interval(left=pd.Timestamp(start), right=pd.Timestamp(end))
#         wall_items = [w for w in self.wall_coll.items if w.interval.overlaps(ir)]
#         ddf = self.wall_coll.ddf(
#             items=wall_items,
#             include_partial=True,
#             force_rr_latest=False,
#             columns=["started", "finished"],
#             filters=[
#                 ("started", ">=", start),
#                 ("started", "<", end),
#             ],
#         )
#
#         df = ddf.compute()
#         self.assertIsInstance(df, pd.DataFrame)
#
#         # No started=None, and they're all between the started and the end
#         self.assertFalse(df.started.isna().any())
#         self.assertFalse((df.started < start).any())
#         self.assertFalse((df.started > end).any())
#
#         # Has finished=None and finished=time, so
#         # the finished is all between the started and
#         # the end_max
#         self.assertTrue(df.finished.isna().any())
#         self.assertTrue((df.finished.dt.year == 2024).any())
#
#         self.assertFalse((df.finished > end_max).any())
#         self.assertFalse((df.finished < start).any())
#
#     # def test_user_id_list(self):
#     #     # Calling compute turns it into a np.ndarray
#     #     user_ids = self.instance.ddf(
#     #         columns=["user_id"]
#     #     ).user_id.unique().values.compute()
#     #     self.assertIsInstance(user_ids, np.ndarray)
#     #
#     #     # If ddf filters work with ndarray
#     #     user_product_merge = <todo: assign>
#     #
#     #     with self.assertRaises(TypeError) as cm:
#     #         user_product_merge.ddf(
#     #             filters=[("id", "in", user_ids)])
#     #     self.assertIn("Value of 'in' filter must be a list, set or tuple.", str(cm.exception))
#     #
#     #     # No compute == dask array
#     #     user_ids = self.instance.ddf(
#     #         columns=["user_id"]
#     #     ).user_id.unique().values
#     #     self.assertIsInstance(user_ids, da.Array)
#     #
#     #     with self.assertRaises(TypeError) as cm:
#     #         user_product_merge.ddf(
#     #             filters=[("id", "in", user_ids)])
#     #     self.assertIn("Value of 'in' filter must be a list, set or tuple.", str(cm.exception))
#     #
#     #     # pick a product_id (most active one)
#     #     self.product_id = instance.df.product_id.value_counts().index[0]
#     #     self.expected_columns: int = len(instance._schema.columns)
#     #     self.instance = instance
#
#     # def test_basic(self):
#     #     # now try to load up the data!
#     #     self.instance.grouped_key = self.product_id
#     #
#     #     # Confirm any of the items are archived
#     #     self.assertTrue(self.instance.progress.has_archive.eq(True).any())
#     #
#     #     # Confirm it returns a df
#     #     df = self.instance.dd().compute()
#     #
#     #     self.assertFalse(df.empty)
#     #     self.assertEqual(df.shape[1], self.expected_columns)
#     #     self.assertGreater(df.shape[0], 1)
#     #
#     #     # Confirm that DF only contains this product_id
#     #     self.assertEqual(df[df.product_id == self.product_id].shape, df.shape)
#
#     # def test_god_vs_product_id(self):
#     #     self.instance.grouped_key = self.product_id
#     #     df_product_origin = self.instance.dd(columns=None, filters=None).compute()
#     #
#     #     self.instance.grouped_key = None
#     #     df_god_origin = self.instance.dd(columns=None,
#     #                                      filters=[("product_id", "==", self.product_id)]).compute()
#     #
#     #     self.assertTrue(df_god_origin.equals(df_product_origin))
#
#     #
#     #     instance = POPSessionMerge(
#     #         start=START,
#     #         archive_path=self.PATH,
#     #         group_by="product_id"
#     #     )
#     #     instance.build(U=GLOBAL_VARS["user"], S=GLOBAL_VARS["session"], W=GLOBAL_VARS["wall"])
#     #     instance.save(god_only=False)
#     #
#     #     # pick a product_id (most active one)
#     #     self.product_id = instance.df.product_id.value_counts().index[0]
#     #     self.expected_columns: int = len(instance._schema.columns)
#     #     self.instance = instance
#
#
# class TestValidItem(CleanTempDirectoryTestCls):
#
#     def test_interval(self):
#         for k in GLOBAL_VARS.keys():
#             coll = GLOBAL_VARS[k]
#             item = coll.items[0]
#             ir = item.interval
#
#             self.assertIsInstance(ir, pd.Interval)
#             self.assertLess(a=ir.left, b=ir.right)
#
#     def test_str(self):
#         for k in GLOBAL_VARS.keys():
#             coll = GLOBAL_VARS[k]
#             item = coll.items[0]
#
#             offset = coll.offset or "–"
#
#             self.assertIn(offset, str(item))
