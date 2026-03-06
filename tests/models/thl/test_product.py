import os
import shutil
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional
from uuid import uuid4

import pytest
from pydantic import ValidationError

from generalresearch.currency import USDCent
from generalresearch.models import Source
from generalresearch.models.thl.product import (
    Product,
    PayoutConfig,
    PayoutTransformation,
    ProfilingConfig,
    SourcesConfig,
    IntegrationMode,
    SupplyConfig,
    SourceConfig,
    SupplyPolicy,
)


class TestProduct:

    def test_init(self):
        # By default, just a Pydantic instance doesn't have an id_int
        instance = Product.model_validate(
            dict(
                id="968a9acc79b74b6fb49542d82516d284",
                name="test-968a9acc",
                redirect_url="https://www.google.com/hey",
            )
        )
        assert instance.id_int is None

        res = instance.model_dump_json()
        # We're not excluding anything here, only in the "*Out" variants
        assert "id_int" in res

    def test_init_db(self, product_manager):
        # By default, just a Pydantic instance doesn't have an id_int
        instance = product_manager.create_dummy()
        assert isinstance(instance.id_int, int)

        res = instance.model_dump_json()

        # we json skip & exclude
        res = instance.model_dump()

    def test_redirect_url(self):
        p = Product.model_validate(
            dict(
                id="968a9acc79b74b6fb49542d82516d284",
                created="2023-09-21T22:13:09.274672Z",
                commission_pct=Decimal("0.05"),
                enabled=True,
                sources=[{"name": "d", "active": True}],
                name="test-968a9acc",
                max_session_len=600,
                team_id="8b5e94afd8a246bf8556ad9986486baa",
                redirect_url="https://www.google.com/hey",
            )
        )

        with pytest.raises(expected_exception=ValidationError):
            p.redirect_url = ""

        with pytest.raises(expected_exception=ValidationError):
            p.redirect_url = None

        with pytest.raises(expected_exception=ValidationError):
            p.redirect_url = "http://www.example.com/test/?a=1&b=2"

        with pytest.raises(expected_exception=ValidationError):
            p.redirect_url = "http://www.example.com/test/?a=1&b=2&tsid="

        p.redirect_url = "https://www.example.com/test/?a=1&b=2"
        c = p.generate_bp_redirect(tsid="c6ab6ba1e75b44e2bf5aab00fc68e3b7")
        assert (
            c
            == "https://www.example.com/test/?a=1&b=2&tsid=c6ab6ba1e75b44e2bf5aab00fc68e3b7"
        )

    def test_harmonizer_domain(self):
        p = Product(
            id="968a9acc79b74b6fb49542d82516d284",
            created="2023-09-21T22:13:09.274672Z",
            commission_pct=Decimal("0.05"),
            enabled=True,
            name="test-968a9acc",
            team_id="8b5e94afd8a246bf8556ad9986486baa",
            harmonizer_domain="profile.generalresearch.com",
            redirect_url="https://www.google.com/hey",
        )
        assert p.harmonizer_domain == "https://profile.generalresearch.com/"
        p.harmonizer_domain = "https://profile.generalresearch.com/"
        p.harmonizer_domain = "https://profile.generalresearch.com"
        assert p.harmonizer_domain == "https://profile.generalresearch.com/"
        with pytest.raises(expected_exception=Exception):
            p.harmonizer_domain = ""
        with pytest.raises(expected_exception=Exception):
            p.harmonizer_domain = None
        with pytest.raises(expected_exception=Exception):
            # no https
            p.harmonizer_domain = "http://profile.generalresearch.com"
        with pytest.raises(expected_exception=Exception):
            # "/a" at the end
            p.harmonizer_domain = "https://profile.generalresearch.com/a"

    def test_payout_xform(self):
        p = Product(
            id="968a9acc79b74b6fb49542d82516d284",
            created="2023-09-21T22:13:09.274672Z",
            commission_pct=Decimal("0.05"),
            enabled=True,
            name="test-968a9acc",
            team_id="8b5e94afd8a246bf8556ad9986486baa",
            harmonizer_domain="profile.generalresearch.com",
            redirect_url="https://www.google.com/hey",
        )

        p.payout_config.payout_transformation = PayoutTransformation.model_validate(
            {
                "f": "payout_transformation_percent",
                "kwargs": {"pct": "0.5", "min_payout": "0.10"},
            }
        )

        assert (
            "payout_transformation_percent" == p.payout_config.payout_transformation.f
        )
        assert 0.5 == p.payout_config.payout_transformation.kwargs.pct
        assert (
            Decimal("0.10") == p.payout_config.payout_transformation.kwargs.min_payout
        )
        assert p.payout_config.payout_transformation.kwargs.max_payout is None

        # This calls get_payout_transformation_func
        # 50% of $1.00
        assert Decimal("0.50") == p.calculate_user_payment(Decimal(1))
        # with a min
        assert Decimal("0.10") == p.calculate_user_payment(Decimal("0.15"))

        with pytest.raises(expected_exception=ValidationError) as cm:
            p.payout_config.payout_transformation = PayoutTransformation.model_validate(
                {"f": "payout_transformation_percent", "kwargs": {}}
            )
        assert "1 validation error for PayoutTransformation\nkwargs.pct" in str(
            cm.value
        )

        with pytest.raises(expected_exception=ValidationError) as cm:
            p.payout_config.payout_transformation = PayoutTransformation.model_validate(
                {"f": "payout_transformation_percent"}
            )

        assert "1 validation error for PayoutTransformation\nkwargs" in str(cm.value)

        with pytest.warns(expected_warning=Warning) as w:
            p.payout_config.payout_transformation = PayoutTransformation.model_validate(
                {
                    "f": "payout_transformation_percent",
                    "kwargs": {"pct": 1, "min_payout": "0.5"},
                }
            )
        assert "Are you sure you want to pay respondents >95% of CPI?" in "".join(
            [str(i.message) for i in w]
        )

        p.payout_config = PayoutConfig()
        assert p.calculate_user_payment(Decimal("0.15")) is None

    def test_payout_xform_amt(self):
        p = Product(
            id="968a9acc79b74b6fb49542d82516d284",
            created="2023-09-21T22:13:09.274672Z",
            commission_pct=Decimal("0.05"),
            enabled=True,
            name="test-968a9acc",
            team_id="8b5e94afd8a246bf8556ad9986486baa",
            harmonizer_domain="profile.generalresearch.com",
            redirect_url="https://www.google.com/hey",
        )

        p.payout_config.payout_transformation = PayoutTransformation.model_validate(
            {
                "f": "payout_transformation_amt",
            }
        )

        assert "payout_transformation_amt" == p.payout_config.payout_transformation.f

        # This calls get_payout_transformation_func
        # 95% of $1.00
        assert p.calculate_user_payment(Decimal(1)) == Decimal("0.95")
        assert p.calculate_user_payment(Decimal("1.05")) == Decimal("1.00")

        assert p.calculate_user_payment(
            Decimal("0.10"), user_wallet_balance=Decimal(0)
        ) == Decimal("0.07")
        assert p.calculate_user_payment(
            Decimal("1.05"), user_wallet_balance=Decimal(0)
        ) == Decimal("0.97")
        assert p.calculate_user_payment(
            Decimal(".05"), user_wallet_balance=Decimal(1)
        ) == Decimal("0.02")
        # final balance will be <0, so pay the full amount
        assert p.calculate_user_payment(
            Decimal(".50"), user_wallet_balance=Decimal(-1)
        ) == p.calculate_user_payment(Decimal("0.50"))
        # final balance will be >0, so do the 7c rounding
        assert p.calculate_user_payment(
            Decimal(".50"), user_wallet_balance=Decimal("-0.10")
        ) == (
            p.calculate_user_payment(Decimal(".40"), user_wallet_balance=Decimal(0))
            - Decimal("-0.10")
        )

    def test_payout_xform_none(self):
        p = Product(
            id="968a9acc79b74b6fb49542d82516d284",
            created="2023-09-21T22:13:09.274672Z",
            commission_pct=Decimal("0.05"),
            enabled=True,
            name="test-968a9acc",
            team_id="8b5e94afd8a246bf8556ad9986486baa",
            harmonizer_domain="profile.generalresearch.com",
            redirect_url="https://www.google.com/hey",
            payout_config=PayoutConfig(payout_format=None, payout_transformation=None),
        )
        assert p.format_payout_format(Decimal("1.00")) is None

        pt = PayoutTransformation.model_validate(
            {"kwargs": {"pct": 0.5}, "f": "payout_transformation_percent"}
        )
        p.payout_config = PayoutConfig(
            payout_format="{payout*10:,.0f} Points", payout_transformation=pt
        )
        assert p.format_payout_format(Decimal("1.00")) == "1,000 Points"

    def test_profiling(self):
        p = Product(
            id="968a9acc79b74b6fb49542d82516d284",
            created="2023-09-21T22:13:09.274672Z",
            commission_pct=Decimal("0.05"),
            enabled=True,
            name="test-968a9acc",
            team_id="8b5e94afd8a246bf8556ad9986486baa",
            harmonizer_domain="profile.generalresearch.com",
            redirect_url="https://www.google.com/hey",
        )
        assert p.profiling_config.enabled is True

        p.profiling_config = ProfilingConfig(max_questions=1)
        assert p.profiling_config.max_questions == 1

    def test_bp_account(self, product, thl_lm):
        assert product.bp_account is None

        product.prefetch_bp_account(thl_lm=thl_lm)

        from generalresearch.models.thl.ledger import LedgerAccount

        assert isinstance(product.bp_account, LedgerAccount)


class TestGlobalProduct:
    # We have one product ID that is special; we call it the Global
    # Product ID and in prod the. This product stores a bunch of extra
    # things in the SourcesConfig

    def test_init_and_props(self):
        instance = Product(
            name="Global Config",
            redirect_url="https://www.example.com",
            sources_config=SupplyConfig(
                policies=[
                    # This is the config for Dynata that any BP is allowed to use
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        integration_mode=IntegrationMode.PLATFORM,
                    ),
                    # Spectrum that is using OUR credentials, that anyone is allowed to use.
                    #   Same as the dynata config above, just that the dynata supplier_id is
                    #   inferred by the dynata-grpc; it's not required to be set.
                    SupplyPolicy(
                        address=["https://spectrum.internal:50051"],
                        active=True,
                        name=Source.SPECTRUM,
                        supplier_id="example-supplier-id",
                        # implicit Scope = GLOBAL
                        # default integration_mode=IntegrationMode.PLATFORM,
                    ),
                    # A spectrum config with a different supplier_id, but
                    #   it is OUR supplier, and we are paid for the completes. Only a certain BP
                    #   can use this config.
                    SupplyPolicy(
                        address=["https://spectrum.internal:50051"],
                        active=True,
                        name=Source.SPECTRUM,
                        supplier_id="example-supplier-id",
                        team_ids=["d42194c2dfe44d7c9bec98123bc4a6c0"],
                        # implicit Scope = TEAM
                        # default integration_mode=IntegrationMode.PLATFORM,
                    ),
                    # The supplier ID is associated with THEIR
                    #   credentials, and we do not get paid for this activity.
                    SupplyPolicy(
                        address=["https://cint.internal:50051"],
                        active=True,
                        name=Source.CINT,
                        supplier_id="example-supplier-id",
                        product_ids=["db8918b3e87d4444b60241d0d3a54caa"],
                        integration_mode=IntegrationMode.PASS_THROUGH,
                    ),
                    # We could have another global cint integration available
                    # to anyone also, or we could have another like above
                    SupplyPolicy(
                        address=["https://cint.internal:50051"],
                        active=True,
                        name=Source.CINT,
                        supplier_id="example-supplier-id",
                        team_ids=["b163972a59584de881e5eab01ad10309"],
                        integration_mode=IntegrationMode.PASS_THROUGH,
                    ),
                ]
            ),
        )

        assert Product.model_validate_json(instance.model_dump_json()) == instance

        s = instance.sources_config
        # Cint should NOT have a global config
        assert set(s.global_scoped_policies_dict.keys()) == {
            Source.DYNATA,
            Source.SPECTRUM,
        }

        # The spectrum global config is the one that isn't scoped to a
        # specific supplier
        assert (
            s.global_scoped_policies_dict[Source.SPECTRUM].supplier_id
            == "grl-supplier-id"
        )

        assert set(s.team_scoped_policies_dict.keys()) == {
            "b163972a59584de881e5eab01ad10309",
            "d42194c2dfe44d7c9bec98123bc4a6c0",
        }
        # This team has one team-scoped config, and it's for spectrum
        assert s.team_scoped_policies_dict[
            "d42194c2dfe44d7c9bec98123bc4a6c0"
        ].keys() == {Source.SPECTRUM}

        # For a random product/team, it'll just have the globally-scoped config
        random_product = uuid4().hex
        random_team = uuid4().hex
        res = instance.sources_config.get_policies_for(
            product_id=random_product, team_id=random_team
        )
        assert res == s.global_scoped_policies_dict

        # It'll have the global config plus cint, and it should use the PRODUCT
        #  scoped config, not the TEAM scoped!
        res = instance.sources_config.get_policies_for(
            product_id="db8918b3e87d4444b60241d0d3a54caa",
            team_id="b163972a59584de881e5eab01ad10309",
        )
        assert set(res.keys()) == {
            Source.DYNATA,
            Source.SPECTRUM,
            Source.CINT,
        }
        assert res[Source.CINT].supplier_id == "example-supplier-id"

    def test_source_vs_supply_validate(self):
        # sources_config can be a SupplyConfig or SourcesConfig.
        # make sure they get model_validated correctly
        gp = Product(
            name="Global Config",
            redirect_url="https://www.example.com",
            sources_config=SupplyConfig(
                policies=[
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        integration_mode=IntegrationMode.PLATFORM,
                    )
                ]
            ),
        )
        bp = Product(
            name="test product config",
            redirect_url="https://www.example.com",
            sources_config=SourcesConfig(
                user_defined=[
                    SourceConfig(
                        active=False,
                        name=Source.DYNATA,
                    )
                ]
            ),
        )
        assert Product.model_validate_json(gp.model_dump_json()) == gp
        assert Product.model_validate_json(bp.model_dump_json()) == bp

    def test_validations(self):
        with pytest.raises(
            ValidationError, match="Can only have one GLOBAL policy per Source"
        ):
            SupplyConfig(
                policies=[
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        integration_mode=IntegrationMode.PLATFORM,
                    ),
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        integration_mode=IntegrationMode.PASS_THROUGH,
                    ),
                ]
            )
        with pytest.raises(
            ValidationError,
            match="Can only have one PRODUCT policy per Source per BP",
        ):
            SupplyConfig(
                policies=[
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        product_ids=["7e417dec1c8a406e8554099b46e518ca"],
                        integration_mode=IntegrationMode.PLATFORM,
                    ),
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        product_ids=["7e417dec1c8a406e8554099b46e518ca"],
                        integration_mode=IntegrationMode.PASS_THROUGH,
                    ),
                ]
            )
        with pytest.raises(
            ValidationError,
            match="Can only have one TEAM policy per Source per Team",
        ):
            SupplyConfig(
                policies=[
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        team_ids=["7e417dec1c8a406e8554099b46e518ca"],
                        integration_mode=IntegrationMode.PLATFORM,
                    ),
                    SupplyPolicy(
                        address=["https://dynata.internal:50051"],
                        active=True,
                        name=Source.DYNATA,
                        team_ids=["7e417dec1c8a406e8554099b46e518ca"],
                        integration_mode=IntegrationMode.PASS_THROUGH,
                    ),
                ]
            )


class TestGlobalProductConfigFor:
    def test_no_user_defined(self):
        sc = SupplyConfig(
            policies=[
                SupplyPolicy(
                    address=["https://dynata.internal:50051"],
                    active=True,
                    name=Source.DYNATA,
                )
            ]
        )
        product = Product(
            name="Test Product Config",
            redirect_url="https://www.example.com",
            sources_config=SourcesConfig(),
        )
        res = sc.get_config_for_product(product=product)
        assert len(res.policies) == 1

    def test_user_defined_merge(self):
        sc = SupplyConfig(
            policies=[
                SupplyPolicy(
                    address=["https://dynata.internal:50051"],
                    banned_countries=["mx"],
                    active=True,
                    name=Source.DYNATA,
                ),
                SupplyPolicy(
                    address=["https://dynata.internal:50051"],
                    banned_countries=["ca"],
                    active=True,
                    name=Source.DYNATA,
                    team_ids=[uuid4().hex],
                ),
            ]
        )
        product = Product(
            name="Test Product Config",
            redirect_url="https://www.example.com",
            sources_config=SourcesConfig(
                user_defined=[
                    SourceConfig(
                        name=Source.DYNATA,
                        active=False,
                        banned_countries=["us"],
                    )
                ]
            ),
        )
        res = sc.get_config_for_product(product=product)
        assert len(res.policies) == 1
        assert not res.policies[0].active
        assert res.policies[0].banned_countries == ["mx", "us"]

    def test_no_eligible(self):
        sc = SupplyConfig(
            policies=[
                SupplyPolicy(
                    address=["https://dynata.internal:50051"],
                    active=True,
                    name=Source.DYNATA,
                    team_ids=["7e417dec1c8a406e8554099b46e518ca"],
                    integration_mode=IntegrationMode.PLATFORM,
                )
            ]
        )
        product = Product(
            name="Test Product Config",
            redirect_url="https://www.example.com",
            sources_config=SourcesConfig(),
        )
        res = sc.get_config_for_product(product=product)
        assert len(res.policies) == 0


class TestProductFinancials:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "30d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return None

    def test_balance(
        self,
        business,
        product_factory,
        user_factory,
        mnt_filepath,
        bp_payout_factory,
        thl_lm,
        lm,
        duration,
        offset,
        thl_redis_config,
        start,
        thl_web_rr,
        brokerage_product_payout_event_manager,
        session_with_tx_factory,
        delete_ledger_db,
        create_main_accounts,
        client_no_amm,
        ledger_collection,
        pop_ledger_merge,
        delete_df_collection,
    ):
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User
        from generalresearch.models.thl.finance import ProductBalances
        from generalresearch.currency import USDCent

        p1: Product = product_factory(business=business)
        u1: User = user_factory(product=p1)
        bp_wallet = thl_lm.get_account_or_create_bp_wallet(product=p1)
        thl_lm.get_account_or_create_user_wallet(user=u1)
        brokerage_product_payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)

        assert len(thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet.uuid)) == 0

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".50"),
            started=start + timedelta(days=1),
        )
        assert thl_lm.get_account_balance(account=bp_wallet) == 48
        assert len(thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet.uuid)) == 1

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal("1.00"),
            started=start + timedelta(days=2),
        )
        assert thl_lm.get_account_balance(account=bp_wallet) == 143
        assert len(thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet.uuid)) == 2

        with pytest.raises(expected_exception=AssertionError) as cm:
            p1.prebuild_balance(
                thl_lm=thl_lm,
                ds=mnt_filepath,
                client=client_no_amm,
            )
        assert "Cannot build Product Balance" in str(cm.value)

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        p1.prebuild_balance(
            thl_lm=thl_lm,
            ds=mnt_filepath,
            client=client_no_amm,
        )
        assert isinstance(p1.balance, ProductBalances)
        assert p1.balance.payout == 143
        assert p1.balance.adjustment == 0
        assert p1.balance.expense == 0
        assert p1.balance.net == 143
        assert p1.balance.balance == 143
        assert p1.balance.retainer == 35
        assert p1.balance.available_balance == 108

        p1.prebuild_payouts(
            thl_lm=thl_lm,
            bp_pem=brokerage_product_payout_event_manager,
        )
        assert p1.payouts is not None
        assert len(p1.payouts) == 0
        assert p1.payouts_total == 0
        assert p1.payouts_total_str == "$0.00"

        # -- Now pay them out...

        bp_payout_factory(
            product=p1,
            amount=USDCent(50),
            created=start + timedelta(days=3),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        assert len(thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet.uuid)) == 3

        # RM the entire directories
        shutil.rmtree(ledger_collection.archive_path)
        os.makedirs(ledger_collection.archive_path, exist_ok=True)
        shutil.rmtree(pop_ledger_merge.archive_path)
        os.makedirs(pop_ledger_merge.archive_path, exist_ok=True)

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        p1.prebuild_balance(
            thl_lm=thl_lm,
            ds=mnt_filepath,
            client=client_no_amm,
        )
        assert isinstance(p1.balance, ProductBalances)
        assert p1.balance.payout == 143
        assert p1.balance.adjustment == 0
        assert p1.balance.expense == 0
        assert p1.balance.net == 143
        assert p1.balance.balance == 93
        assert p1.balance.retainer == 23
        assert p1.balance.available_balance == 70

        p1.prebuild_payouts(
            thl_lm=thl_lm,
            bp_pem=brokerage_product_payout_event_manager,
        )
        assert p1.payouts is not None
        assert len(p1.payouts) == 1
        assert p1.payouts_total == 50
        assert p1.payouts_total_str == "$0.50"

        # -- Now pay ou another!.

        bp_payout_factory(
            product=p1,
            amount=USDCent(5),
            created=start + timedelta(days=4),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        assert len(thl_lm.get_tx_filtered_by_account(account_uuid=bp_wallet.uuid)) == 4

        # RM the entire directories
        shutil.rmtree(ledger_collection.archive_path)
        os.makedirs(ledger_collection.archive_path, exist_ok=True)
        shutil.rmtree(pop_ledger_merge.archive_path)
        os.makedirs(pop_ledger_merge.archive_path, exist_ok=True)

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        p1.prebuild_balance(
            thl_lm=thl_lm,
            ds=mnt_filepath,
            client=client_no_amm,
        )
        assert isinstance(p1.balance, ProductBalances)
        assert p1.balance.payout == 143
        assert p1.balance.adjustment == 0
        assert p1.balance.expense == 0
        assert p1.balance.net == 143
        assert p1.balance.balance == 88
        assert p1.balance.retainer == 22
        assert p1.balance.available_balance == 66

        p1.prebuild_payouts(
            thl_lm=thl_lm,
            bp_pem=brokerage_product_payout_event_manager,
        )
        assert p1.payouts is not None
        assert len(p1.payouts) == 2
        assert p1.payouts_total == 55
        assert p1.payouts_total_str == "$0.55"


class TestProductBalance:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "30d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return None

    def test_inconsistent(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        brokerage_product_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        payout_event_manager,
    ):
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)

        # 1. Complete and Build Parquets 1st time
        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        # 2. Payout and build Parquets 2nd time
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        bp_payout_factory(
            product=product,
            amount=USDCent(71),
            ext_ref_id=uuid4().hex,
            created=start + timedelta(days=1, minutes=1),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        with pytest.raises(expected_exception=AssertionError) as cm:
            product.prebuild_balance(
                thl_lm=thl_lm, ds=mnt_filepath, client=client_no_amm
            )
        assert "Sql and Parquet Balance inconsistent" in str(cm)

    def test_not_inconsistent(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        brokerage_product_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        payout_event_manager,
    ):
        # This is very similar to the test_complete_payout_pq_inconsistent
        #   test, however this time we're only going to assign the payout
        #   in real time, and not in the past. This means that even if we
        #   build the parquet files multiple times, they will include the
        #   payout.

        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)

        # 1. Complete and Build Parquets 1st time
        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        # 2. Payout and build Parquets 2nd time but this payout is "now"
        #    so it hasn't already been archived
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        bp_payout_factory(
            product=product,
            amount=USDCent(71),
            ext_ref_id=uuid4().hex,
            created=datetime.now(tz=timezone.utc),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        # We just want to call this to confirm it doesn't raise.
        product.prebuild_balance(thl_lm=thl_lm, ds=mnt_filepath, client=client_no_amm)


class TestProductPOPFinancial:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "30d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return None

    def test_base(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        brokerage_product_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        payout_event_manager,
    ):
        # This is very similar to the test_complete_payout_pq_inconsistent
        #   test, however this time we're only going to assign the payout
        #   in real time, and not in the past. This means that even if we
        #   build the parquet files multiple times, they will include the
        #   payout.

        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)

        # 1. Complete and Build Parquets 1st time
        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        # --- test ---
        assert product.pop_financial is None
        product.prebuild_pop_financial(
            thl_lm=thl_lm,
            ds=mnt_filepath,
            client=client_no_amm,
            pop_ledger=pop_ledger_merge,
        )

        from generalresearch.models.thl.finance import POPFinancial

        assert isinstance(product.pop_financial, list)
        assert isinstance(product.pop_financial[0], POPFinancial)
        pf1: POPFinancial = product.pop_financial[0]
        assert isinstance(pf1.time, datetime)
        assert pf1.payout == 71
        assert pf1.net == 71
        assert pf1.adjustment == 0
        for adj in pf1.adjustment_types:
            assert adj.amount == 0


class TestProductCache:

    @pytest.fixture
    def start(self) -> "datetime":
        return datetime(year=2018, month=3, day=14, hour=0, tzinfo=timezone.utc)

    @pytest.fixture
    def offset(self) -> str:
        return "30d"

    @pytest.fixture
    def duration(self) -> Optional["timedelta"]:
        return None

    def test_basic(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        brokerage_product_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
    ):
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        # Confirm the default / null behavior
        rc = thl_redis_config.create_redis_client()
        res: Optional[str] = rc.get(product.cache_key)
        assert res is None
        with pytest.raises(expected_exception=AssertionError):
            product.set_cache(
                thl_lm=thl_lm,
                ds=mnt_filepath,
                client=client_no_amm,
                bp_pem=brokerage_product_payout_event_manager,
                redis_config=thl_redis_config,
            )

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)

        session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )

        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        # Now try again with everything in place
        product.set_cache(
            thl_lm=thl_lm,
            ds=mnt_filepath,
            client=client_no_amm,
            bp_pem=brokerage_product_payout_event_manager,
            redis_config=thl_redis_config,
        )

        # Fetch from cache and assert the instance loaded from redis
        res: Optional[str] = rc.get(product.cache_key)
        assert isinstance(res, str)
        from generalresearch.models.thl.ledger import LedgerAccount

        assert isinstance(product.bp_account, LedgerAccount)

        p1: Product = Product.model_validate_json(res)
        assert p1.balance.product_id == product.uuid
        assert p1.balance.payout_usd_str == "$0.71"
        assert p1.balance.retainer_usd_str == "$0.17"
        assert p1.balance.available_balance_usd_str == "$0.54"

    def test_neg_balance_cache(
        self,
        product,
        mnt_filepath,
        thl_lm,
        client_no_amm,
        thl_redis_config,
        brokerage_product_payout_event_manager,
        delete_ledger_db,
        create_main_accounts,
        delete_df_collection,
        ledger_collection,
        business,
        user_factory,
        product_factory,
        session_with_tx_factory,
        pop_ledger_merge,
        start,
        bp_payout_factory,
        payout_event_manager,
        adj_to_fail_with_tx_factory,
    ):
        # Now let's load it up and actually test some things
        delete_ledger_db()
        create_main_accounts()
        delete_df_collection(coll=ledger_collection)

        from generalresearch.models.thl.product import Product
        from generalresearch.models.thl.user import User

        u1: User = user_factory(product=product)

        # 1. Complete
        s1 = session_with_tx_factory(
            user=u1,
            wall_req_cpi=Decimal(".75"),
            started=start + timedelta(days=1),
        )

        # 2. Payout
        payout_event_manager.set_account_lookup_table(thl_lm=thl_lm)
        bp_payout_factory(
            product=product,
            amount=USDCent(71),
            ext_ref_id=uuid4().hex,
            created=start + timedelta(days=1, minutes=1),
            skip_wallet_balance_check=True,
            skip_one_per_day_check=True,
        )

        # 3. Recon
        adj_to_fail_with_tx_factory(
            session=s1,
            created=start + timedelta(days=1, minutes=1),
        )

        # Finally, process everything:
        ledger_collection.initial_load(client=None, sync=True)
        pop_ledger_merge.build(client=client_no_amm, ledger_coll=ledger_collection)

        product.set_cache(
            thl_lm=thl_lm,
            ds=mnt_filepath,
            client=client_no_amm,
            bp_pem=brokerage_product_payout_event_manager,
            redis_config=thl_redis_config,
        )

        # Fetch from cache and assert the instance loaded from redis
        rc = thl_redis_config.create_redis_client()
        res: Optional[str] = rc.get(product.cache_key)
        assert isinstance(res, str)

        p1: Product = Product.model_validate_json(res)
        assert p1.balance.product_id == product.uuid
        assert p1.balance.payout_usd_str == "$0.71"
        assert p1.balance.adjustment == -71
        assert p1.balance.expense == 0
        assert p1.balance.net == 0
        assert p1.balance.balance == -71
        assert p1.balance.retainer_usd_str == "$0.00"
        assert p1.balance.available_balance_usd_str == "$0.00"
