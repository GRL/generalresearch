import re

from generalresearch.managers.base import PostgresManager
from generalresearch.managers.thl.tango_api import TangoClient
from generalresearch.models.thl.wallet.definitions import (
    CURRENCY_MAX_VALUE,
)


class TangoAdmin(PostgresManager):
    def __init__(self, *, tango_client: TangoClient, tango_account_id: str, **kwargs):
        super().__init__(**kwargs)
        self.tango_client = tango_client
        self.tango_account_id = tango_account_id

    def get_account_balance(self) -> float:
        account = self.tango_client.get_account(
            account_identifier=self.tango_account_id
        )
        return account["currentBalance"]

    @staticmethod
    def is_available_tango_card(item: dict):
        """
        We're supporting cards that meet the following conditions or checks:
         - see below
         - non USD card support variable value only (todo: handling conversion of fixed amounts)
        """
        from generalresearch.models.thl.wallet.definitions import SUPPORTED_CURRENCIES

        reward_types = ["cash equivalent", "gift card"]  # these are the only options
        value_types = ["VARIABLE_VALUE", "FIXED_VALUE"]  # these are the only options
        ignore_cards = {
            "U557938",  # Southwest® Gift Card $25, doesn't work...?
        }
        return bool(
            item["rewardType"] in reward_types
            and item["status"] == "active"
            and item["valueType"] in value_types
            and item["utid"] not in ignore_cards
            and item["currencyCode"] in SUPPORTED_CURRENCIES
            and (
                item["currencyCode"] == "USD"
                or item["currencyCode"] != "USD"
                and item["valueType"] == "VARIABLE_VALUE"
            )
        )

    @staticmethod
    def format_tango_card(item: dict, brand: dict):
        """
        For non-USD, we support variable value only. We aren't doing any currency conversion here, for
            them. To handle the $250USD max value, I'm setting a hard limit in each currency. These are
            for our supported, stable currencies only, so this shouldn't be a problem....
        """
        tango_card_info = {
            # 'currency': 'USD',
            "provider": "TANGO",
            "cashout_method_type": "GIFT_CARD",
        }
        brand_dict = {
            "image_url": brand["imageUrls"]["1200w-326ppi"],
            "description": brand["description"],
            "disclaimer": brand["disclaimer"],
            "terms": brand["terms"],
        }
        value_type_map = {"VARIABLE_VALUE": "variable", "FIXED_VALUE": "fixed"}
        item["rewardName"] = re.sub(r"\s+", " ", item["rewardName"])
        # if this is a fixed value, non USD card, we need to save the original value before
        #   we convert it to USD, b/c we need that value to make the request
        if item["valueType"] == "FIXED_VALUE" and item["currencyCode"] != "USD":
            raise NotImplementedError("FIXED VALUE non-USD not supported")
        if item["valueType"] == "VARIABLE_VALUE" and item["currencyCode"] != "USD":
            pass
        if item["valueType"] == "FIXED_VALUE":
            item["minValue"] = item["faceValue"]
            item["maxValue"] = item["faceValue"]

        card = {
            "name": item["rewardName"],
            "utid": item["utid"],
            "type": item["rewardType"],
            # 'min_value_usd': round(item.min_value, 2),
            # 'max_value_usd': round(min(item.max_value, MAX_VALUE[item.currency_code]), 2),
            "min_value": round(100 * float(item["minValue"])),
            "max_value": min(
                round(100 * float(item["maxValue"])),
                CURRENCY_MAX_VALUE[item["currencyCode"]] * 100,
            ),
            "countries": item["countries"],
            "value_type": value_type_map[item["valueType"]],
            # This is the currency the reward is based in. We'll need this to convert the request
            #   to USD when the user requests it.
            "currency": item["currencyCode"],
        }
        card.update(brand_dict)
        card.update(tango_card_info)
        return card

    def get_tango_cards(self) -> list:
        """
        This is called to populate the accounting_cashoutmethod table ONLY. It is not LIVE.
        For non-USD cards, the $ fields are in foreign currency because this is stored in
            our DB for lookup purposes and has to be converted when requested.
        """
        # usd_exchange_rate = get_tango_exchange_rates()
        cards: list[dict] = []
        catalog = self.tango_client.get_catalog()
        brands = [b for b in catalog["brands"] if b["status"] == "active"]
        for brand in brands:
            for item in brand["items"]:
                if self.is_available_tango_card(item):
                    print(
                        item["currencyCode"],
                        item["rewardName"],
                        item["valueType"],
                        (
                            f"{item['minValue']}-{item['maxValue']}"
                            if "minValue" in item
                            else item["faceValue"]
                        ),
                    )
                    cards.append(self.format_tango_card(item, brand))
        return cards

#
# def update_tango_cashout_method_db():
#     # Synchronizes Tango cashout methods/gift cards with the accounting_cashoutmethod table.
#     # Should only need to be run sporadically.
#     # Note: Nothing calls this at the moment. It was run once manually.
#     now = datetime.utcnow()
#     tango_cards = get_tango_cards()
#     provider = "TANGO"
#
#     db_res = THL_WEB_RW.execute_sql_query(
#         f"""
#     SELECT `id`, `provider`, `ext_id` FROM `{THL_WEB_RW.db}`.`accounting_cashoutmethod`"""
#     )
#     existing_ids = {(provider, x["ext_id"]): x["id"] for x in db_res}
#
#     fields = ["id", "last_updated", "is_live", "provider", "ext_id", "name", "data"]
#     values = [
#         [
#             existing_ids.get((provider, x["utid"]), uuid.uuid4().hex),
#             now,
#             True,
#             provider,
#             x["utid"],
#             x["name"],
#             json.dumps(x),
#         ]
#         for x in tango_cards
#     ]
#
#     THL_WEB_RW.bulk_update("accounting_cashoutmethod", fields, values)
#
#
# def check_and_refill_balance() -> None:
#     # dont do this more often than once a week!!!
#     REFILL_AMOUNT = 5000
#     LOW_BALANCE = 1000
#     balance = get_account_balance()
#
#     if balance < LOW_BALANCE:
#         SC.chat_postMessage(
#             channel=settings.SLACK_CHANNEL,
#             text=f"Refilling Tango account. current balance: {balance}",
#         )
#         depost_request = {
#             "accountIdentifier": settings.TANGO_ACCOUNT_ID,
#             "amount": REFILL_AMOUNT,
#             "creditCardToken": settings.TANGO_CREDITCARD_TOKEN,
#             "customerIdentifier": settings.TANGO_CUSTOMER_ID,
#         }
#         response = None
#         try:
#             response = api_client.fund.add_funds(depost_request)
#             assert response.status == "SUCCESS"
#             response = json.loads(APIHelper.json_serialize(response))
#         except Exception as e:
#             SC.chat_postMessage(
#                 channel=settings.SLACK_CHANNEL,
#                 text=f"Tango refill error: {e}. Response: {response}",
#             )
#         SC.chat_postMessage(
#             channel=settings.SLACK_CHANNEL,
#             text=f"Tango refill success!: Response: {response}",
#         )
#     elif balance < (LOW_BALANCE * 1.5):
#         SC.chat_postMessage(
#             channel=settings.SLACK_CHANNEL,
#             text=f"Tango balance low. current balance: {balance}",
#         )
