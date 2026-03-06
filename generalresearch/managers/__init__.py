def parse_order_by(order_by_str: str):
    """
    Converts django-rest-framework ordering str to mysql clause
    :param order_by_str: e.g. 'created,-name'
    :return: mysql clause e.g. ORDER BY created ASC, name DESC
    """
    fields = order_by_str.split(",")

    order_clause = []
    for field in fields:
        if field.startswith("-"):
            order_clause.append(f"{field[1:]} DESC")
        else:
            order_clause.append(f"{field} ASC")

    return "ORDER BY " + ", ".join(order_clause)
