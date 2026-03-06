from generalresearch.models.thl.profiling.upk_property import PropertyType


class TestUpkSchemaManager:

    def test_get_props_info(self, upk_schema_manager, upk_data):
        props = upk_schema_manager.get_props_info()
        assert (
            len(props) == 16955
        )  # ~ 70 properties x each country they are available in

        gender = [
            x
            for x in props
            if x.country_iso == "us"
            and x.property_id == "73175402104741549f21de2071556cd7"
        ]
        assert len(gender) == 1
        gender = gender[0]
        assert len(gender.allowed_items) == 3
        assert gender.allowed_items[0].label == "female"
        assert gender.allowed_items[1].label == "male"
        assert gender.prop_type == PropertyType.UPK_ITEM
        assert gender.categories[0].label == "Demographic"

        age = [
            x
            for x in props
            if x.country_iso == "us"
            and x.property_id == "94f7379437874076b345d76642d4ce6d"
        ]
        assert len(age) == 1
        age = age[0]
        assert age.allowed_items is None
        assert age.prop_type == PropertyType.UPK_NUMERICAL
        assert age.gold_standard

        cars = [
            x
            for x in props
            if x.country_iso == "us" and x.property_label == "household_auto_type"
        ][0]
        assert not cars.gold_standard
        assert cars.categories[0].label == "Autos & Vehicles"
