import json


def country_default_lang():
    """
    Some marketplaces have no language specified. Surveys are in the "default
    language for that country", whatever that means. This helper is meant to
    provide a reasonable guess as to what language it is.

    Derived from: http://download.geonames.org/export/dump/countryInfo.txt
    """
    raise ValueError("no need to run this, I already ran it.")
    import pandas as pd
    from generalresearch.locales import Localelator

    l = Localelator()

    df = pd.read_csv(
        "http://download.geonames.org/export/dump/countryInfo.txt",
        sep="\t",
        skiprows=49,
    )
    df["default_lang"] = df.Languages.str.split(",").str[0].str.split("-").str[0]
    df.default_lang = df.default_lang.fillna("en")
    df.default_lang = df.default_lang.map(
        lambda x: l.get_language_iso(x) if x in l.languages else "eng"
    )
    df["#ISO"] = df["#ISO"].str.lower()
    df["country_iso"] = df["#ISO"].map(
        lambda x: l.get_country_iso(x) if x in l.countries else None
    )
    df = df[df.country_iso.notnull()]
    d = df.set_index("country_iso").default_lang.to_dict()
    with open("country_default_lang.json", "w") as f:
        json.dump(d, f, indent=2)
    return d


def setup_json():
    # pycountry is 30mb, which makes using this package on AWS lambda problematic.
    # These JSONs are stolen from pycountry and adapted.

    raise ValueError("no need to run this, I already ran it.")

    # languages
    d = json.load(open("iso639-3.json"))
    d["639-3"] = [x for x in d["639-3"] if "alpha_2" in x]
    for x in d["639-3"]:
        x["alpha_3b"] = x.pop("bibliographic", None) or x["alpha_3"]
        del x["scope"]
        del x["type"]
    with open("iso639-3.json", "w") as f:
        json.dump(d["639-3"], f, indent=2)

    # countries
    d = json.load(open("iso3166-1.json"))["3166-1"]
    for x in d:
        x["alpha_2"] = x["alpha_2"].lower()
        x["alpha_3"] = x["alpha_3"].lower()
    with open("iso3166-1.json", "w") as f:
        json.dump(d, f, indent=2)
