from enum import StrEnum


class SupplierTag(StrEnum):
    """Available tags which can be used to annotate supplier traffic
    """
    MOBILE = "mobile"
    JS_OFFERWALL = "js-offerwall"
    DOI = "double-opt-in"
    SSO = "single-sign-on"
    PHONE_VERIFIED = "phone-number-verified"
    TEST_A = "test-a"
    TEST_B = "test-b"
