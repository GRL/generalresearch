# import json
# import logging
# import os
# from typing import Optional
#
# import grpc
# import pandas as pd
# from generalresearch.locales import Localelator
# from generalresearch.sql_helper import SqlHelper
# from pandas import DataFrame
#
# from incite.data.build import BuildObject
# from incite.protos import generalresearch_pb2, generalresearch_pb2_grpc
#
# web_sql_helper = SqlHelper(**WEB_CONFIG_DICT)
# locale_helper = Localelator()
#
# logging.basicConfig()
# logger = logging.getLogger()
# logger.setLevel(LOG_LEVEL)
#
#
# class BuildAccountBlocksActive(BuildObject):
#
#     def build(self) -> None:
#         df = self.get_account_blocks_active_df()
#         self.result = df.reset_index()
#
#     def export(self, dry_run=True, god_only: Optional[bool] = False, rebuild: Optional[bool] = False) -> None:
#         file_path = os.path.join(EXPORT_DIR, "_account_blocks_active.feather")
#
#         if dry_run:
#             logger.info(f"[dryrun] saving: {file_path}: {' x '.join([str(i) for i in self.result.shape])}")
#         else:
#             logger.info(f"saving: {file_path}: {' x '.join([str(i) for i in self.result.shape])}")
#             self.result.to_feather(file_path)
#
#     def get_account_blocks_active_df(self) -> DataFrame:
#         logger.info(f"BuildAccountBlocksGeo.get_account_blocks_active_df")
#
#         res = web_sql_helper.execute_sql_query(f"""SELECT bp.id FROM userprofile_brokerageproduct bp""")
#         product_ids = [x["id"] for x in res]
#
#         # 1. Global config
#         with grpc.insecure_channel(GRPC_SERVER) as channel:
#             stub = generalresearch_pb2_grpc.GeneralResearchStub(channel)
#             msg = generalresearch_pb2.GetBPConfigRequest(product_id=GLOBAL_CONFIG)
#             res = list(stub.GetBPConfig(msg))
#
#         # Global config always needs routers
#         routers = {x.key: json.loads(x.value) for x in res}["routers"]
#         sources = set([r["name"] for r in routers])
#         from incite.data.utils import MARKETPLACE_KEYS
#         opts = set(MARKETPLACE_KEYS.keys())
#         assert opts.issuperset(sources), "Default router definitions not available option"
#
#         df = pd.DataFrame(
#             index=pd.MultiIndex.from_product([product_ids, ["global_config", "product_config"]],
#                                              names=["product_id", "reason"]),
#             columns=sources).fillna(0)
#
#         for router in routers:
#             if not router["active"]:
#                 df.loc[(slice(None), "global_config"), [router["name"]]] = 1
#
#         # 2. bpid specific router definitions
#         with grpc.insecure_channel(GRPC_SERVER) as channel:
#             stub = generalresearch_pb2_grpc.GeneralResearchStub(channel)
#
#             for product_id in product_ids:
#                 msg = generalresearch_pb2.GetBPConfigRequest(product_id=product_id)
#                 res = list(stub.GetBPConfig(msg))
#
#                 routers = {x.key: json.loads(x.value) for x in res}["routers"]
#
#                 for router in routers:
#                     if not router["active"]:
#                         df.loc[(product_id, "product_config"), [router["name"]]] = 1
#
#         return df


#
# import json
# import logging
# import os
# from typing import Optional
#
# import grpc
# import pandas as pd
# from generalresearch.locales import Localelator
# from generalresearch.sql_helper import SqlHelper
# from google.protobuf.json_format import MessageToDict
# from pandas import DataFrame
#
# from incite.data.build import BuildObject
# from incite.protos import thl_pb2, thl_pb2_grpc, generalresearch_pb2, generalresearch_pb2_grpc
#
# web_sql_helper = SqlHelper(**WEB_CONFIG_DICT)
# locale_helper = Localelator()
#
# logging.basicConfig()
# logger = logging.getLogger()
# logger.setLevel(LOG_LEVEL)
#
#
# class BuildAccountBlocksGeo(BuildObject):
#
#     def build(self) -> None:
#         df = self.get_account_blocks_geo_df()
#         self.result = df.reset_index()
#
#     def export(self, dry_run=True, god_only: Optional[bool] = False, rebuild: Optional[bool] = False) -> None:
#         file_path = os.path.join(DS["exports"], "_account_blocks_geo.feather")
#
#         if dry_run:
#             logger.info(f"[dryrun] saving: {file_path}: {' x '.join([str(i) for i in self.result.shape])}")
#         else:
#             logger.info(f"saving: {file_path}: {' x '.join([str(i) for i in self.result.shape])}")
#             self.result.to_feather(file_path)
#
#     def get_account_blocks_geo_df(self) -> DataFrame:
#         logger.info(f"BuildAccountBlocksGeo.get_account_blocks_geo_df")
#
#         res = web_sql_helper.execute_sql_query(f"""SELECT bp.id FROM userprofile_brokerageproduct bp""")
#         product_ids = [x["id"] for x in res]
#         # (TODO) Stupid GB vs UK here... can cleanup on admin portal... not sure what/where to do it
#         geos = list(locale_helper.get_all_countries()) + ["uk"]
#
#         # 1. Global config
#         with grpc.insecure_channel(GRPC_SERVER) as channel:
#             stub = generalresearch_pb2_grpc.GeneralResearchStub(channel)
#             msg = generalresearch_pb2.GetBPConfigRequest(product_id=GLOBAL_CONFIG)
#             res = list(stub.GetBPConfig(msg))
#
#         # Global config always needs routers
#         routers = {x.key: json.loads(x.value) for x in res}["routers"]
#         sources = set([r["name"] for r in routers])
#         from incite.data.utils import MARKETPLACE_KEYS
#         opts = set(MARKETPLACE_KEYS.keys())
#         assert opts.issuperset(sources), "Default router definitions not available option"
#
#         df = pd.DataFrame(
#             index=pd.MultiIndex.from_product([product_ids, sources, ["global_config", "product_config",
#             "eligibility"]],
#                                              names=["product_id", "source", "reason"]),
#             columns=geos).fillna(0)
#
#         for router in routers:
#             df.loc[(slice(None), router["name"], "global_config"), router.get("banned_countries", [])] = 1
#
#         # 2. bpid specific router definitions
#         with grpc.insecure_channel(GRPC_SERVER) as channel:
#             stub = generalresearch_pb2_grpc.GeneralResearchStub(channel)
#
#             for product_id in product_ids:
#                 msg = generalresearch_pb2.GetBPConfigRequest(product_id=product_id)
#                 res = list(stub.GetBPConfig(msg))
#
#                 routers = {x.key: json.loads(x.value) for x in res}["routers"]
#
#                 for router in routers:
#                     df.loc[(product_id, router["name"], "product_config"), router.get("banned_countries", [])] = 1
#
#         # 3. bpid specific eligibility values
#         with grpc.insecure_channel(GRPC_SERVER) as channel:
#             thl_stub = thl_pb2_grpc.THLStub(channel)
#
#             for product_id in product_ids:
#                 req = thl_pb2.GetPlatformStatsRequest(bpid=product_id)
#                 res = thl_stub.GetPlatformStats(req)
#                 res = MessageToDict(res, including_default_value_fields=True,
#                                     preserving_proto_field_name=True,
#                                     use_integers_for_enums=True)
#                 stats = res.get("stats", None)
#
#                 if not stats:
#                     continue
#
#                 res = [dict(name=x[0], value=x[1]) for x in zip(stats.keys(), stats.values())]
#
#                 for stat in res:
#                     statn = stat["name"].split("MARKETPLACE_ELIGIBILITY.")
#                     if len(statn) == 2:
#                         source, geo = statn[1].split(".")
#                         if stat.get("value") == 0:
#                             df.loc[(product_id, source, "eligibility"), [geo]] = 1
#
#         return df
