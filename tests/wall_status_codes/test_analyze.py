from generalresearch.models.thl.definitions import StatusCode1, Status
from generalresearch.wall_status_codes import innovate


class TestInnovate:
    def test_complete(self):
        status, status_code_1, status_code_2 = innovate.annotate_status_code("1", None)
        assert Status.COMPLETE == status
        assert StatusCode1.COMPLETE == status_code_1
        assert status_code_2 is None
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "1", "whatever"
        )
        assert Status.COMPLETE == status
        assert StatusCode1.COMPLETE == status_code_1
        assert status_code_2 is None

    def test_unknown(self):
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "69420", None
        )
        assert Status.FAIL == status
        assert StatusCode1.UNKNOWN == status_code_1
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "69420", "Speeder"
        )
        assert Status.FAIL == status
        assert StatusCode1.UNKNOWN == status_code_1

    def test_ps(self):
        status, status_code_1, status_code_2 = innovate.annotate_status_code("5", None)
        assert Status.FAIL == status
        assert StatusCode1.PS_FAIL == status_code_1
        # The ext_status_code_2 should reclassify this as PS_FAIL
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "8", "DeviceType"
        )
        assert Status.FAIL == status
        assert StatusCode1.PS_FAIL == status_code_1
        # this should be reclassified from PS_FAIL to PS_OQ
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "5", "Group NA"
        )
        assert Status.FAIL == status
        assert StatusCode1.PS_OVERQUOTA == status_code_1

    def test_dupe(self):
        # innovate calls it a quality, should be dupe
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "8", "Duplicated to token Tq2SwRVX7PUWnFunGPAYWHk"
        )
        assert Status.FAIL == status
        assert StatusCode1.PS_DUPLICATE == status_code_1
        # stay as quality
        status, status_code_1, status_code_2 = innovate.annotate_status_code(
            "8", "Selected threat potential score at joblevel not allow the survey"
        )
        assert Status.FAIL == status
        assert StatusCode1.PS_QUALITY == status_code_1


# todo: fix me: This got broke because I opened the csv in libreoffice and it broke it
#   status codes "1.0" -> 1 (facepalm)

# class TestAllCsv::
#     def get_wall(self) -> pd.DataFrame:
#         df = pd.read_csv(os.path.join(os.path.dirname(__file__), "wall_excerpt.csv.gz"))
#         df['started'] = pd.to_datetime(df['started'])
#         df['finished'] = pd.to_datetime(df['finished'])
#         return df
#
#     def test_dynata(self):
#         df = self.get_wall()
#         df = df[df.source == Source.DYNATA]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(dynata.annotate_status_code(row.status_code)), axis=1)
#         self.assertEqual(1419, len(df[df.t_status == Status.COMPLETE]))
#         assert len(df[df.t_status == Status.FAIL]) == 2109
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) == 0
#         assert 1000 < len(df[df.t_status_code_1 == StatusCode1.BUYER_FAIL]) < 1100
#         assert 30 < len(df[df.t_status_code_1 == StatusCode1.PS_BLOCKED]) < 40
#
#     def test_fullcircle(self):
#         df = self.get_wall()
#         df = df[df.source == Source.FULL_CIRCLE]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(fullcircle.annotate_status_code(row.status_code)), axis=1)
#         # assert len(df[df.t_status == Status.COMPLETE]) == 1419
#         # assert len(df[df.t_status == Status.FAIL]) == 2109
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) == 0
#
#     def test_innovate(self):
#         df = self.get_wall()
#         df = df[df.source == Source.INNOVATE]
#         df = df[~df.status.isin({'r', 'e'})]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(innovate.annotate_status_code(
#                 row.status_code, row.status_code_2 if pd.notnull(row.status_code_2) else None)),
#             axis=1)
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) / len(df) < 0.05
#         # assert len(df[df.t_status == Status.COMPLETE]) == 1419
#         # assert len(df[df.t_status == Status.FAIL]) == 2109
#         # assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) == 0
#
#     def test_morning(self):
#         df = self.get_wall()
#         df = df[df.source == Source.MORNING_CONSULT]
#         df = df[df.status_code.notnull()]
#         # we have to do this for old values...
#         df['status_code'] = df['status_code'].apply(short_code_to_status_codes_morning.get)
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(morning.annotate_status_code(row.status, row.status_code)), axis=1)
#         dff = df[df.t_status != Status.COMPLETE]
#         assert len(dff[dff.t_status_code_1 == StatusCode1.UNKNOWN]) / len(dff) < 0.05
#
#     def test_pollfish(self):
#         df = self.get_wall()
#         df = df[df.source == Source.POLLFISH]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(pollfish.annotate_status_code(row.status_code)), axis=1)
#
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) / len(df) < 0.05
#
#     def test_pollfish(self):
#         df = self.get_wall()
#         df = df[df.source == Source.PRECISION]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(precision.annotate_status_code(row.status_code)), axis=1)
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) / len(df) < 0.05
#
#     def test_sago(self):
#         df = self.get_wall()
#         df = df[df.source == Source.SAGO]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(sago.annotate_status_code(row.status_code)), axis=1)
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) / len(df) < 0.05
#
#     def test_spectrum(self):
#         df = self.get_wall()
#         df = df[df.source == Source.SPECTRUM]
#         df = df[df.status_code.notnull()]
#         df[['t_status', 't_status_code_1', 't_status_code_2']] = df.apply(
#             lambda row: pd.Series(spectrum.annotate_status_code(row.status_code)), axis=1)
#         assert len(df[df.t_status_code_1 == StatusCode1.UNKNOWN]) / len(df) < 0.05
