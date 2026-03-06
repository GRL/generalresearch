from enum import Enum


class SupplierTag(str, Enum):
    """Available tags which can be used to annotate supplier traffic

    Note: should not include commas!
    """

    MOBILE = "mobile"
    JS_OFFERWALL = "js-offerwall"
    DOI = "double-opt-in"
    SSO = "single-sign-on"
    PHONE_VERIFIED = "phone-number-verified"
    TEST_A = "test-a"
    TEST_B = "test-b"
