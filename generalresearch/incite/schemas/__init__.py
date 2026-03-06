from typing import List

import pandas as pd
import pandera.pandas as pa

ORDER_KEY = "order_key"
# How long after an DFCollectionItem's .finish can we archive it ? Should be
#   90 min for Wall / Session, typically if rows are never modified, we'll
#   use 1 min.
ARCHIVE_AFTER = "archive_after"
PARTITION_ON = "partition_on"


def empty_dataframe_from_schema(schema: pa.DataFrameSchema) -> "pd.DataFrame":
    index_names: List[str] = schema.index.names
    columns = set(schema.dtypes.keys())

    if len(index_names) > 1:
        columns = columns | set(index_names)

    df = pd.DataFrame(columns=list(columns)).astype(
        {col: str(dtype) for col, dtype in schema.dtypes.items()}
    )

    if len(index_names) > 1:
        df.set_index(keys=index_names, inplace=True)

    df.index.set_names(names=index_names, inplace=True)
    return df
