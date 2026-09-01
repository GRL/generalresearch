from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from generalresearch.managers.lucid.profiling import get_profiling_library

if TYPE_CHECKING:
    from generalresearch.pg_helper import PostgresConfig

qids = ["42", "43", "45", "97", "120", "639", "15297"]


class TestLucidProfiling:

    @pytest.mark.skip
    def test_get_library(self, thl_web_rr: PostgresConfig):
        pks = [(qid, "us", "eng") for qid in qids]
        qs = get_profiling_library(thl_web_rr, pks=pks)
        assert len(qids) == len(qs)

        # just making sure this doesn't raise errors
        for q in qs:
            q.to_upk_question()

        # a lot will fail parsing because they have no options or the options are blank
        #   just asserting that we get some back
        qs = get_profiling_library(thl_web_rr, country_iso="mx", language_iso="spa")
        assert len(qs) > 100
