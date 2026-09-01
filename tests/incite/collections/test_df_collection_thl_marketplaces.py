from datetime import UTC, datetime
from itertools import product
from typing import TYPE_CHECKING

import pytest
from pandera.pandas import Column, DataFrameSchema, Index

from generalresearch.incite.collections.base import DFCollection, DFCollectionType
from generalresearch.incite.collections.thl_marketplaces import (
    InnovateSurveyHistoryCollection,
    MorningSurveyTimeseriesCollection,
    SagoSurveyHistoryCollection,
    SpectrumSurveyTimeseriesCollection,
)

if TYPE_CHECKING:
    from generalresearch.incite.base import GRLDatasets
    from generalresearch.pg_helper import PostgresConfig


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
        yield from x


@pytest.mark.parametrize("df_coll, offset", combo_object())
class TestDFCollection_thl_marketplaces:

    def test_init(
        self,
        mnt_filepath: GRLDatasets,
        df_coll: DFCollection,
        offset: str,
        spectrum_rw: PostgresConfig,
    ):
        assert issubclass(df_coll, DFCollection)

        # This is stupid, but we need to pull the default from the
        #   Pydantic field
        data_type = df_coll.model_fields["data_type"].default
        assert isinstance(data_type, DFCollectionType)

        # (1) Can't be totally empty, needs a path...
        with pytest.raises(expected_exception=ValueError):
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
            start=datetime(year=2023, month=6, day=1, minute=0, tzinfo=UTC),
            finished=datetime(year=2023, month=6, day=1, minute=5, tzinfo=UTC),
        )
        assert isinstance(instance, DFCollection)

        # (4) Now that we initialize the Class, we can access the _schema
        assert isinstance(instance._schema, DataFrameSchema)
        assert isinstance(instance._schema.index, Index)

        for c in instance._schema.columns:
            assert isinstance(c, str)
            col = instance._schema.columns[c]
            assert isinstance(col, Column)

        assert instance._schema.coerce, "coerce on all Schemas"
        assert isinstance(instance._schema.checks, list)
        assert len(instance._schema.checks) == 0
        assert isinstance(instance._schema.metadata, dict)
        assert len(instance._schema.metadata.keys()) == 2
