from datetime import datetime, timezone
from itertools import product

import pytest
from pandera import Column, Index, DataFrameSchema

from generalresearch.incite.collections import DFCollection
from generalresearch.incite.collections import DFCollectionType
from generalresearch.incite.collections.thl_marketplaces import (
    InnovateSurveyHistoryCollection,
    MorningSurveyTimeseriesCollection,
    SagoSurveyHistoryCollection,
    SpectrumSurveyTimeseriesCollection,
)
from test_utils.incite.conftest import mnt_filepath


def combo_object():
    for x in product(
        [
            InnovateSurveyHistoryCollection,
            MorningSurveyTimeseriesCollection,
            SagoSurveyHistoryCollection,
            SpectrumSurveyTimeseriesCollection,
        ],
        ["5min", "6H", "30D"],
    ):
        yield x


@pytest.mark.parametrize("df_coll, offset", combo_object())
class TestDFCollection_thl_marketplaces:

    def test_init(self, mnt_filepath, df_coll, offset, spectrum_rw):
        assert issubclass(df_coll, DFCollection)

        # This is stupid, but we need to pull the default from the
        #   Pydantic field
        data_type = df_coll.model_fields["data_type"].default
        assert isinstance(data_type, DFCollectionType)

        # (1) Can't be totally empty, needs a path...
        with pytest.raises(expected_exception=Exception) as cm:
            instance = df_coll()

        # (2) Confirm it only needs the archive_path
        instance = df_coll(
            archive_path=mnt_filepath.archive_path(enum_type=data_type),
        )
        assert isinstance(instance, DFCollection)

        # (3) Confirm it loads with all
        instance = df_coll(
            archive_path=mnt_filepath.archive_path(enum_type=data_type),
            sql_helper=spectrum_rw,
            offset=offset,
            start=datetime(year=2023, month=6, day=1, minute=0, tzinfo=timezone.utc),
            finished=datetime(year=2023, month=6, day=1, minute=5, tzinfo=timezone.utc),
        )
        assert isinstance(instance, DFCollection)

        # (4) Now that we initialize the Class, we can access the _schema
        assert isinstance(instance._schema, DataFrameSchema)
        assert isinstance(instance._schema.index, Index)

        for c in instance._schema.columns.keys():
            assert isinstance(c, str)
            col = instance._schema.columns[c]
            assert isinstance(col, Column)

        assert instance._schema.coerce, "coerce on all Schemas"
        assert isinstance(instance._schema.checks, list)
        assert len(instance._schema.checks) == 0
        assert isinstance(instance._schema.metadata, dict)
        assert len(instance._schema.metadata.keys()) == 2
