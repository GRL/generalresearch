import os
from datetime import datetime, timezone
from uuid import uuid4

import faker
import pytest

from generalresearch.models.network.definitions import IPProtocol

from generalresearch.models.network.tool_run import (
    ToolName,
    ToolClass,
    Status,
)

fake = faker.Faker()


def test_create_tool_run_from_nmap_run(nmap_run, toolrun_manager):

    toolrun_manager.create_nmap_run(nmap_run)

    run_out = toolrun_manager.get_nmap_run(nmap_run.id)

    assert nmap_run == run_out


def test_create_tool_run_from_rdns_run(rdns_run, toolrun_manager):

    toolrun_manager.create_rdns_run(rdns_run)

    run_out = toolrun_manager.get_rdns_run(rdns_run.id)

    assert rdns_run == run_out


def test_create_tool_run_from_mtr_run(mtr_run, toolrun_manager):

    toolrun_manager.create_mtr_run(mtr_run)

    run_out = toolrun_manager.get_mtr_run(mtr_run.id)

    assert mtr_run == run_out
