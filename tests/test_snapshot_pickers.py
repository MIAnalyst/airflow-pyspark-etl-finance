import os, pathlib, pytest
from typing import List

def mk_part(root: pathlib.Path, table: str, dates: List[str]):
    base = root / table
    base.mkdir(parents=True, exist_ok=True)
    for d in dates:
        (base / f"ingestion_date={d}").mkdir()

def test_per_table_latest(tmp_path):
    # arrange
    mk_part(tmp_path, "A", ["2025-08-20","2025-08-22"])
    mk_part(tmp_path, "B", ["2025-08-21"])
    # act
    from jobs.build_dim_customer import resolve_dates_per_table  # or move to libs/snapshot.py
    out = resolve_dates_per_table(str(tmp_path), ["A","B"], requested="latest")
    # assert
    assert out == {"A":"2025-08-22","B":"2025-08-21"}

def test_common_latest(tmp_path):
    mk_part(tmp_path, "A", ["2025-08-20","2025-08-22"])
    mk_part(tmp_path, "B", ["2025-08-22","2025-08-23"])
    from jobs.build_dim_customer import resolve_date_common
    picked = resolve_date_common(str(tmp_path), ["A","B"], requested="latest")
    assert picked == "2025-08-22"

def test_common_no_intersection(tmp_path):
    mk_part(tmp_path, "A", ["2025-08-20"])
    mk_part(tmp_path, "B", ["2025-08-21"])
    from jobs.build_dim_customer import resolve_date_common
    with pytest.raises(RuntimeError) as e:
        resolve_date_common(str(tmp_path), ["A","B"], requested="latest")
    assert "No common ingestion_date" in str(e.value)

def test_per_table_explicit_date(tmp_path):
    mk_part(tmp_path, "A", ["2025-08-20"])
    mk_part(tmp_path, "B", ["2025-08-21"])
    from jobs.build_dim_customer import resolve_dates_per_table
    out = resolve_dates_per_table(str(tmp_path), ["A","B"], requested="2025-08-19")
    assert out == {"A":"2025-08-19","B":"2025-08-19"}
