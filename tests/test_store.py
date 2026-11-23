"""交付记录落盘与内存摘要的引擎层测试。"""

from __future__ import annotations

import pytest

from app.engine import SwitchEngine
from app.store import DeliveryStore, StoreError


class FlakyStore(DeliveryStore):
    """可通过开关模拟写盘失败的存储。"""

    def __init__(self, path):
        super().__init__(path)
        self.fail_writes = False

    def append_batch(self, rows):
        if self.fail_writes:
            raise StoreError("模拟写盘失败：磁盘不可用")
        return super().append_batch(rows)


def test_store_roundtrip_and_per_flow_filter(tmp_path):
    store = DeliveryStore(str(tmp_path / "d.db"))
    store.append_batch([(1, "a", 1, 0), (2, "b", 1, 1), (3, "a", 2, 0)])
    assert store.last_order() == 3
    assert store.was_delivered("a", 1) and not store.was_delivered("a", 3)
    assert store.flow_seqs("a") == [1, 2]
    assert [r["order"] for r in store.list()] == [1, 2, 3]
    assert [r["seq"] for r in store.list("a")] == [1, 2]
    store.reset()
    assert store.last_order() == 0 and store.list() == []
    store.close()


def test_reopen_clears_history(tmp_path):
    path = str(tmp_path / "d.db")
    store = DeliveryStore(path)
    store.append_batch([(1, "a", 1, 0)])
    store.close()
    # 进程重启后清空历史，交付序号从 1 开始
    reopened = DeliveryStore(path)
    assert reopened.list() == [] and reopened.last_order() == 0
    reopened.close()


def test_in_memory_summary_is_bounded():
    e = SwitchEngine(queues=1, max_pending=4, summary_size=3)
    for seq in range(1, 11):
        e.enqueue("f", seq, 0)
        e.deliver(0)
    summary = e.flows["f"].summary
    assert summary.delivered_count == 10
    assert summary.last_seq == 10
    assert list(summary.recent) == [8, 9, 10]  # 内存里只留固定大小的窗口
    # 全量历史在磁盘上，查询范围照旧
    assert e.stream_state("f")["delivered"] == list(range(1, 11))
    assert [o for o, _ in e.egress] == list(range(1, 11))


def test_duplicate_and_stale_fall_back_to_disk():
    e = SwitchEngine(queues=1, max_pending=8, summary_size=2)
    for seq in (1, 2, 3, 4, 6):  # 5 号包从未出现
        assert e.enqueue("f", seq, 0).accepted
    e.deliver(0, count=10)
    assert list(e.flows["f"].summary.recent) == [4, 6]  # 1/2/3 已挤出摘要窗口
    # 交付过的老包仍报重复（靠磁盘记录判定）
    assert e.enqueue("f", 1, 0).reason == "duplicate"
    assert e.enqueue("f", 3, 0).reason == "duplicate"
    # 包号低于最新交付但从未交付的仍报过期
    assert e.enqueue("f", 5, 0).reason == "stale"


def test_failed_write_keeps_batch_queued_and_order_continues(tmp_path):
    store = FlakyStore(str(tmp_path / "d.db"))
    e = SwitchEngine(queues=2, store=store)
    e.enqueue("f", 1, 0)
    e.enqueue("f", 2, 1)
    e.enqueue("g", 1, 1)
    out = e.deliver(1, count=10)  # g 的 1 号包先成功交付
    assert [(p.flow_id, p.seq) for _, p in out.delivered] == [("g", 1)]

    store.fail_writes = True
    with pytest.raises(StoreError):
        e.deliver(0, count=10)
    # 整批包留在各自的接收队列，其他队列的同流后续包继续等候
    assert e.stream_state("f")["pending_count"] == 2
    assert e.deliver(1, count=10).delivered == []
    # 写盘尚未提交时，egress 仍只有此前成功的交付记录
    assert [(p.flow_id, p.seq) for _, p in e.egress] == [("g", 1)]
    # 为恢复暂存的状态只保留本批
    assert e._staged is not None
    assert [p.seq for p in e._staged.packets] == [1]

    store.fail_writes = False
    out = e.deliver(0, count=10)
    assert [(o, p.seq) for o, p in out.delivered] == [(2, 1)]  # 接着上次成功记录
    assert e._staged is None  # 写盘成功后暂存即清空
    out = e.deliver(1, count=10)
    assert [(o, p.seq) for o, p in out.delivered] == [(3, 2)]
    assert [o for o, _ in e.egress] == [1, 2, 3]


def test_staged_state_only_tracks_current_batch(tmp_path):
    store = FlakyStore(str(tmp_path / "d.db"))
    e = SwitchEngine(queues=1, max_pending=4, store=store)
    for seq in range(1, 5):
        e.enqueue("f", seq, 0)
    store.fail_writes = True
    with pytest.raises(StoreError):
        e.deliver(0, count=2)
    assert len(e._staged.packets) == 2  # 只随本批报文增长
    with pytest.raises(StoreError):
        e.deliver(0, count=1)
    assert len(e._staged.packets) == 1  # 被下一批覆盖，不累积
    store.fail_writes = False
    e.deliver(0, count=10)
    assert e._staged is None


def test_reset_clears_disk_and_restarts_order():
    e = SwitchEngine(queues=1)
    e.enqueue("f", 1, 0)
    e.deliver(0)
    e.reset()
    assert e.egress == []
    e.enqueue("f", 1, 0)
    out = e.deliver(0)
    assert [o for o, _ in out.delivered] == [1]  # 交付序号从 1 开始


def test_restart_clears_history(tmp_path):
    path = str(tmp_path / "d.db")
    e1 = SwitchEngine(queues=1, store_path=path)
    e1.enqueue("f", 1, 0)
    e1.deliver(0)
    assert len(e1.egress) == 1
    e1.close()

    e2 = SwitchEngine(queues=1, store_path=path)
    assert e2.egress == []  # 进程重启后清空历史
    e2.enqueue("f", 1, 0)
    out = e2.deliver(0)
    assert [o for o, _ in out.delivered] == [1]  # 交付序号从 1 开始
    e2.close()
