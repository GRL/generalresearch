import pandas as pd
import pytest
from pandera.errors import SchemaError


class TestWallSchema:

    def test_empty(self):
        from generalresearch.incite.schemas.thl_web import THLWallSchema

        with pytest.raises(SchemaError):
            THLWallSchema.validate(pd.DataFrame())

    def test_index_missing(self):
        from generalresearch.incite.schemas.thl_web import THLWallSchema

        df = pd.DataFrame(columns=THLWallSchema.columns.keys())

        with pytest.raises(SchemaError) as cm:
            THLWallSchema.validate(df)

    def test_no_rows(self):
        from generalresearch.incite.schemas.thl_web import THLWallSchema

        df = pd.DataFrame(index=["uuid"], columns=THLWallSchema.columns.keys())

        with pytest.raises(SchemaError) as cm:
            THLWallSchema.validate(df)

    def test_new_empty_df(self):
        from generalresearch.incite.schemas import empty_dataframe_from_schema
        from generalresearch.incite.schemas.thl_web import THLWallSchema

        df = empty_dataframe_from_schema(THLWallSchema)
        assert isinstance(df, pd.DataFrame)
        assert df.columns.size == 20


class TestSessionSchema:

    def test_empty(self):
        from generalresearch.incite.schemas.thl_web import THLSessionSchema

        with pytest.raises(SchemaError):
            THLSessionSchema.validate(pd.DataFrame())

    def test_index_missing(self):
        from generalresearch.incite.schemas.thl_web import THLSessionSchema

        df = pd.DataFrame(columns=THLSessionSchema.columns.keys())
        df.set_index("uuid", inplace=True)

        with pytest.raises(SchemaError) as cm:
            THLSessionSchema.validate(df)

    def test_no_rows(self):
        from generalresearch.incite.schemas.thl_web import THLSessionSchema

        df = pd.DataFrame(index=["id"], columns=THLSessionSchema.columns.keys())

        with pytest.raises(SchemaError) as cm:
            THLSessionSchema.validate(df)

    def test_new_empty_df(self):
        from generalresearch.incite.schemas import empty_dataframe_from_schema
        from generalresearch.incite.schemas.thl_web import THLSessionSchema

        df = empty_dataframe_from_schema(THLSessionSchema)
        assert isinstance(df, pd.DataFrame)
        assert df.columns.size == 21
