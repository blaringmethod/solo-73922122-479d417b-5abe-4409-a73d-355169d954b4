"""单号冲突清理测试：被拒请求不得在共享卷/目录/检查点留下任何痕迹。

覆盖修复要求：
* 同单异内容 409：原件库无被拒字节、暂存区无残留、目录与检查点不新增；
* 同单同字节幂等重试：复用且不产生第二个原件文件；
* 限额拒绝同样零残留；
* 提升阶段写盘失败返回 507、事务回滚、可原样重试；
* 暂存阶段写盘失败返回 507、可原样重试；
* 暂存不占名额：限额附近一个实例暂存时，另一实例仍可发布不同内容。
"""
from __future__ import annotations

import hashlib
import io

import pytest

from app import main as main_mod
from app.storage import BlobError, StagedBlob


def _publish(client, station, day, order, data):
    return client.post(
        "/releases",
        data={"station_id": station, "sample_date": day, "order_no": order},
        files={"file": ("day.csv", io.BytesIO(data), "text/csv")},
    )


def _blob_files(settings) -> set[str]:
    """内容寻址原件库中的全部文件名（相对 blob_dir）。"""
    return {
        str(p.relative_to(settings.blob_dir))
        for p in settings.blob_dir.rglob("*")
        if p.is_file()
    }


def _staged_files(settings) -> set[str]:
    return {p.name for p in settings.temp_dir.iterdir() if p.is_file()}


def test_conflict_leaves_no_blob_no_tmp_no_records(app_client):
    client, _app, settings = app_client
    original = b"station,ph\nC1,7.0\n"
    rejected = b"station,ph\nC1,9.9-DIFFERENT\n"
    rejected_digest = hashlib.sha256(rejected).hexdigest()

    r1 = _publish(client, "CF1", "2026-11-16", "ORD-1", original)
    assert r1.status_code == 201
    blobs_after_first = _blob_files(settings)
    cps_after_first = client.get("/checkpoints").json()
    releases_after_first = client.get("/releases").json()

    r2 = _publish(client, "CF1", "2026-11-16", "ORD-1", rejected)
    assert r2.status_code == 409

    # 原件库：被拒字节不存在，文件集合与首次发布后完全一致。
    assert not main_mod.blob_store.exists(rejected_digest)
    assert _blob_files(settings) == blobs_after_first
    # 暂存区：无任何残留。
    assert _staged_files(settings) == set()
    # 目录：记录数与内容不变，原记录仍指向首次字节。
    releases = client.get("/releases").json()
    assert releases == releases_after_first
    rec = client.get("/releases/CF1/2026-11-16/ORD-1").json()
    assert rec["blob_sha256"] == hashlib.sha256(original).hexdigest()
    # 检查点：不为本次拒绝新增。
    assert client.get("/checkpoints").json() == cps_after_first


def test_concurrent_conflict_both_rejections_clean(app_client):
    """两实例语义（同进程内两个并发请求）同单异内容，败者也必须零残留。"""
    client, _app, settings = app_client
    import concurrent.futures

    payloads = [b"station,ph\nCC,1.0\n", b"station,ph\nCC,2.0\n"]
    digests = {hashlib.sha256(p).hexdigest() for p in payloads}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        statuses = list(
            pool.map(
                lambda data: _publish(
                    client, "CC", "2026-11-16", "ORD-CC", data
                ).status_code,
                payloads,
            )
        )
    assert sorted(statuses) == [201, 409]

    present = {d for d in digests if main_mod.blob_store.exists(d)}
    # 恰有一个摘要落盘（胜出者），败者字节不在原件库。
    assert len(present) == 1
    assert _staged_files(settings) == set()
    before = len([r for r in client.get("/releases").json() if r["order_no"] == "ORD-CC"])
    assert before == 1
    # 最新检查点条数与总记录数一致，拒绝未追加检查点。
    releases_total = len(client.get("/releases").json())
    assert client.get("/checkpoints/latest").json()["seq"] == releases_total


def test_same_bytes_retry_reuses_single_blob(app_client):
    client, _app, settings = app_client
    data = b"station,ph\nSAME,7.3\n"
    digest = hashlib.sha256(data).hexdigest()

    r1 = _publish(client, "CF2", "2026-11-16", "ORD-CF2", data)
    r2 = _publish(client, "CF2", "2026-11-16", "ORD-CF2", data)
    assert r1.status_code == 201 and r1.json()["reused"] is False
    assert r2.status_code == 201 and r2.json()["reused"] is True
    assert r1.json()["release"] == r2.json()["release"]

    # 同摘要只对应一个原件文件（不随重试产生第二份），重试不留暂存件、不新增记录。
    matches = [
        p
        for p in settings.blob_dir.rglob("*")
        if p.is_file() and p.name == digest
    ]
    assert len(matches) == 1
    assert _staged_files(settings) == set()
    orders = [r["order_no"] for r in client.get("/releases").json()]
    assert orders.count("ORD-CF2") == 1
    assert client.get("/checkpoints/latest").json()["seq"] == len(orders)


def test_same_bytes_different_order_both_downloadable(app_client):
    """不同单号相同字节：两条记录都可下载、可取证，且只存一份原件。"""
    client, _app, settings = app_client
    data = b"station,ph\nSHARED,8.0\n"
    digest = hashlib.sha256(data).hexdigest()

    r1 = _publish(client, "CF3", "2026-11-16", "ORD-3A", data)
    r2 = _publish(client, "CF3", "2026-11-16", "ORD-3B", data)
    assert r1.status_code == 201 and r2.status_code == 201
    assert r1.json()["reused"] is False and r2.json()["reused"] is False

    # 两条记录共享同一份内容寻址原件，不复制字节。
    matches = [
        p
        for p in settings.blob_dir.rglob("*")
        if p.is_file() and p.name == digest
    ]
    assert len(matches) == 1
    for order in ("ORD-3A", "ORD-3B"):
        dl = client.get(f"/releases/CF3/2026-11-16/{order}/download")
        assert dl.status_code == 200 and dl.content == data
        proof = client.get(
            f"/releases/CF3/2026-11-16/{order}/inclusion-proof"
        )
        assert proof.status_code == 200
    assert client.get("/checkpoints/latest").json()["seq"] == len(
        client.get("/releases").json()
    )


def test_capacity_rejection_is_clean(app_client):
    client, _app, settings = app_client
    existing = len(client.get("/releases").json())
    original_limit = settings.max_releases
    object.__setattr__(settings, "max_releases", existing)
    try:
        data = b"station,ph\nCAP,5.5\n"
        digest = hashlib.sha256(data).hexdigest()
        blobs_before = _blob_files(settings)
        cps_before = client.get("/checkpoints").json()

        r = _publish(client, "CAPC", "2026-11-16", "ORD-CAP", data)
        assert r.status_code == 409 and "上限" in r.json()["detail"]
        assert not main_mod.blob_store.exists(digest)
        assert _blob_files(settings) == blobs_before
        assert _staged_files(settings) == set()
        assert client.get("/checkpoints").json() == cps_before
        assert len(client.get("/releases").json()) == existing
    finally:
        object.__setattr__(settings, "max_releases", original_limit)


def test_promote_failure_returns_507_rolls_back_and_retries(app_client, monkeypatch):
    client, _app, settings = app_client
    data = b"station,ph\nPROMO,6.6\n"
    digest = hashlib.sha256(data).hexdigest()
    before = len(client.get("/releases").json())
    blobs_before = _blob_files(settings)
    cps_before = client.get("/checkpoints").json()

    real_promote = main_mod.BlobStore.promote

    def boom_promote(self, staged):
        raise BlobError("ENOSPC: 模拟提升阶段卷写满")

    monkeypatch.setattr(main_mod.BlobStore, "promote", boom_promote)
    r = _publish(client, "PRM", "2026-11-16", "ORD-P", data)
    assert r.status_code == 507
    monkeypatch.undo()

    # 事务回滚：目录/检查点/原件库均无新增，暂存件已清理。
    assert len(client.get("/releases").json()) == before
    assert client.get("/checkpoints").json() == cps_before
    assert _blob_files(settings) == blobs_before
    assert not main_mod.blob_store.exists(digest)
    assert _staged_files(settings) == set()

    # 恢复后原样重试成功，且可下载。
    r2 = _publish(client, "PRM", "2026-11-16", "ORD-P", data)
    assert r2.status_code == 201 and r2.json()["reused"] is False
    dl = client.get("/releases/PRM/2026-11-16/ORD-P/download")
    assert dl.status_code == 200 and dl.content == data
    # 真正的提升路径仍然可用（monkeypatch 已撤销）。
    assert real_promote is main_mod.BlobStore.promote


def test_stage_failure_returns_507_and_retries(app_client, monkeypatch):
    client, _app, settings = app_client
    data = b"station,ph\nSTG,4.4\n"
    before = len(client.get("/releases").json())

    def boom_stage(self, payload):
        raise BlobError("ENOSPC: 模拟暂存阶段卷写满")

    monkeypatch.setattr(main_mod.BlobStore, "stage", boom_stage)
    r = _publish(client, "STG", "2026-11-16", "ORD-S", data)
    assert r.status_code == 507
    monkeypatch.undo()

    assert len(client.get("/releases").json()) == before
    assert _staged_files(settings) == set()
    r2 = _publish(client, "STG", "2026-11-16", "ORD-S", data)
    assert r2.status_code == 201


def test_staging_does_not_consume_capacity_slot(app_client, monkeypatch):
    """一个请求停在暂存阶段（尚未提交）时，另一个请求仍能占用名额发布。"""
    client, _app, settings = app_client
    existing = len(client.get("/releases").json())
    # 把限额设为“当前条数 + 1”，只剩一个名额。
    original_limit = settings.max_releases
    object.__setattr__(settings, "max_releases", existing + 1)
    try:
        entered = __import__("threading").Event()
        release = __import__("threading").Event()

        real_promote = main_mod.BlobStore.promote

        def blocking_promote(self, staged: StagedBlob):
            entered.set()
            assert release.wait(timeout=5), "阻塞提升超时"
            return real_promote(self, staged)

        monkeypatch.setattr(main_mod.BlobStore, "promote", blocking_promote)
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(
                _publish, client, "HOLD", "2026-11-16", "ORD-HOLD", b"hold-bytes\n"
            )
            assert entered.wait(timeout=5), "首个请求未进入提升阶段"

            # 首请求已暂存并持有写锁但尚未提交；第二个请求先尝试会被锁阻塞。
            second_future = pool.submit(
                _publish, client, "OTHER", "2026-11-16", "ORD-OTHER", b"other\n"
            )
            # 放行首请求：它提交并占用唯一名额。
            release.set()
            r1 = first.result(timeout=10)
            r2 = second_future.result(timeout=15)

        assert r1.status_code == 201
        # 名额已被首请求用完，第二个请求得到的是限额 409 而非 500/死锁；
        # 关键性质：暂存本身不占名额，且被拒请求仍然零残留。
        assert r2.status_code == 409 and "上限" in r2.json()["detail"]
        assert _staged_files(settings) == set()
        assert len(client.get("/releases").json()) == existing + 1
    finally:
        object.__setattr__(settings, "max_releases", original_limit)
        monkeypatch.undo()
