"""HDFS is the only lake storage configured by the shared path helper."""

from bnpl_common import get_settings, lake_path


def test_lake_path_uses_hdfs(monkeypatch):
    monkeypatch.setenv("HDFS_URI", "hdfs://namenode:8020/")
    monkeypatch.setenv("HDFS_BASE_PATH", "/bnpl-data/")

    assert lake_path("silver/transactions") == (
        "hdfs://namenode:8020/bnpl-data/silver/transactions"
    )


def test_hdfs_replication_defaults_to_three(monkeypatch):
    monkeypatch.delenv("HDFS_REPLICATION", raising=False)

    assert get_settings().hdfs_replication == 3
