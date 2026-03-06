import mariadb.constants
from mariadb.constants import EXT_FIELD_TYPE

# This should be an enum, or a dictionary ... of course it's not
# field_flags = {k: getattr(mariadb.constants.FIELD_FLAG, k) for k in dir(mariadb.constants.FIELD_FLAG)
#                if not k.startswith('__')}
ext_field_flags = {
    k: getattr(EXT_FIELD_TYPE, k) for k in dir(EXT_FIELD_TYPE) if not k.startswith("__")
}
ext_field_flags_rev = {v: k for k, v in ext_field_flags.items()}


# def decode_field_flags(field_flag: int):
#     # https://mariadb-corporation.github.io/mariadb-connector-python/cursor.html
#     # This was written by chatgpt basically... idk how binary works.
#     decoded_flags = []
#     for flag, value in field_flags.items():
#         if field_flag & value:
#             decoded_flags.append(flag)
#
#     return decoded_flags


def example():
    # actually we don't need the field flags. I didn't see, but there is an
    # extended field type returned also. Which explicitly tags uuid fields.
    conn = mariadb.connect(
        host="127.0.0.1", user="root", password="", database="300large-morning"
    )
    c = conn.cursor()
    c.execute("SELECT user_id, pid as greg FROM morning_userpid limit 1")
    for m in zip(c.metadata["field"], c.metadata["ext_type_or_format"]):
        # here we can just check if the field's ext_field_flag == 'UUID' (2)
        print(m[0], ext_field_flags_rev[m[1]])


def get_column_types():
    # How does django do this?
    res = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = 'morning_userpid' AND table_schema = DATABASE()"""
