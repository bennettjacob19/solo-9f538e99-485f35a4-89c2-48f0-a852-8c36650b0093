"""接收队列切换演练的核心状态机（与 Web 框架无关，方便单独测试）。

模型要点
--------
* 一个网卡有多条接收队列（默认 2 条）。报文被哪条队列「收下」在接收时就定下来，
  之后永远由这条队列向上一层交付。
* 每条流按包号顺序交付。包号最小的未交付报文是队头；队头还在等前序包时，这条流
  后面的报文（即使已在别的队列里）都要等候，其他流不受影响。
* 一条流最多积压 ``max_pending`` 个「已接收但未交付」的报文；满了只拒收这一流的
  新报文，别的流照常收包。推进队列腾出位置后可以重试。
* 切换接收队列只影响之后新到的报文；已经收下的报文仍留在原队列，由原队列交付。

交付记录的存放
--------------
* 逐包交付记录（全局交付序号、流、包号、来源队列）整批写入本地磁盘
  （:class:`~app.store.DeliveryStore`），内存里不保留全量历史。
* 每条流在内存只留固定大小的交付摘要（交付总数、最新交付包号、最近若干个
  包号），用来快速判断重复包/过期包；摘要窗口里找不到的老包回查磁盘。
* 推进一批报文时先选批、暂存，再整批写盘；写盘成功后才从接收队列移除报文。
  写盘失败抛 :class:`~app.store.StoreError`，整批留在各自的接收队列，
  恢复后重试即可，交付序号接着上次成功记录。为恢复暂存的状态只随本批涉及
  的流和报文增长，写盘成功即清空。
* 重置或进程重启后清空历史，交付序号从 1 重新开始。
"""

from __future__ import annotations

import os
import tempfile
import weakref
from collections import deque
from dataclasses import dataclass, field
from threading import RLock
from typing import Deque, Dict, List, Optional, Tuple

from .store import DeliveryStore, StoreError

DEFAULT_QUEUES = 2
DEFAULT_MAX_PENDING = 4
DEFAULT_SUMMARY_SIZE = 8  # 每条流在内存里保留的最近交付包号个数


class UnknownQueueError(ValueError):
    """队列编号越界。"""


class UnknownFlowError(KeyError):
    """流不存在。"""


class InvalidPacketError(ValueError):
    """报文参数不合法。"""


def _clean_flow_id(flow_id: str) -> str:
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise InvalidPacketError("flow_id 不能为空")
    if any(ch.isspace() for ch in flow_id):
        raise InvalidPacketError("flow_id 不能包含空白字符")
    if len(flow_id) > 64:
        raise InvalidPacketError("flow_id 最长 64 个字符")
    return flow_id


@dataclass
class Packet:
    flow_id: str
    seq: int
    queue_id: int


@dataclass
class DeliverySummary:
    """每条流在内存里只保留的固定大小交付摘要。

    逐包记录全在磁盘上；这里只留交付总数、最新交付的包号和最近
    ``maxlen`` 个包号，足够快速判断「下一个该交谁」「这是重复包还是
    过期包」。窗口里找不到的老包由引擎回查磁盘。
    """

    maxlen: int = DEFAULT_SUMMARY_SIZE
    delivered_count: int = 0
    last_seq: Optional[int] = None
    recent: Deque[int] = field(default_factory=deque)

    def __post_init__(self) -> None:
        self.recent = deque(self.recent, maxlen=self.maxlen)

    def remember(self, seq: int) -> None:
        self.delivered_count += 1
        self.last_seq = seq
        self.recent.append(seq)


@dataclass
class Flow:
    flow_id: str
    pending: List[Packet] = field(default_factory=list)
    summary: DeliverySummary = field(default_factory=DeliverySummary)
    rejected: int = 0
    duplicate_attempts: int = 0
    stale_attempts: int = 0


@dataclass
class EnqueueOutcome:
    accepted: bool
    reason: str
    detail: str
    flow: "Flow"
    packet: Packet
    capacity: int


@dataclass
class DeliveryOutcome:
    queue_id: int
    delivered: List[Tuple[int, Packet]]  # (全局交付序号, 报文)
    remaining: int


@dataclass
class _StagedBatch:
    """一批已选出、写盘尚未提交（或刚失败）的交付。

    只为恢复而暂存：大小只随本批涉及的流和报文增长；写盘成功即清空，
    失败时保留到下一次推进选出新的批次为止，供排查与重试核对。
    """

    queue_id: int
    orders: Tuple[int, ...]
    packets: Tuple[Packet, ...]

    def as_dicts(self) -> List[dict]:
        return [
            {
                "order": order,
                "flow_id": packet.flow_id,
                "seq": packet.seq,
                "queue_id": packet.queue_id,
            }
            for order, packet in zip(self.orders, self.packets)
        ]


def _finalize_store(store: DeliveryStore, owned_path: Optional[str]) -> None:
    try:
        store.close()
    except Exception:
        pass
    if owned_path:
        try:
            os.unlink(owned_path)
        except OSError:
            pass


class SwitchEngine:
    def __init__(
        self,
        queues: int = DEFAULT_QUEUES,
        max_pending: int = DEFAULT_MAX_PENDING,
        summary_size: int = DEFAULT_SUMMARY_SIZE,
        store: Optional[DeliveryStore] = None,
        store_path: Optional[str] = None,
    ) -> None:
        if queues < 1:
            raise ValueError("queues 至少为 1")
        if max_pending < 1:
            raise ValueError("max_pending 至少为 1")
        if summary_size < 1:
            raise ValueError("summary_size 至少为 1")
        self.n_queues = queues
        self.max_pending = max_pending
        self.summary_size = summary_size
        self._lock = RLock()
        self._owned_store_path: Optional[str] = None
        if store is not None:
            self._store = store
        else:
            if store_path is None:
                # 默认也落在本地磁盘：临时文件，引擎回收时自动清理。
                fd, store_path = tempfile.mkstemp(
                    prefix="rxq-deliveries-", suffix=".db"
                )
                os.close(fd)
                self._owned_store_path = store_path
            self._store = DeliveryStore(store_path)
        # 引擎被回收时关闭存储；临时库文件一并删除。
        self._finalizer = weakref.finalize(
            self, _finalize_store, self._store, self._owned_store_path
        )
        self._reset_unlocked()

    # ------------------------------------------------------------------ 内部
    def _reset_unlocked(self) -> None:
        self.flows: Dict[str, Flow] = {}
        self.active_queue: int = 0
        self._staged: Optional[_StagedBatch] = None

    def _check_queue(self, queue_id: int) -> int:
        if isinstance(queue_id, bool) or not isinstance(queue_id, int):
            raise UnknownQueueError(f"队列编号必须是整数：{queue_id!r}")
        if not 0 <= queue_id < self.n_queues:
            raise UnknownQueueError(
                f"队列 {queue_id} 不存在，有效范围 0..{self.n_queues - 1}"
            )
        return queue_id

    def _flow(self, flow_id: str) -> Flow:
        flow_id = _clean_flow_id(flow_id)
        flow = self.flows.get(flow_id)
        if flow is None:
            flow = Flow(
                flow_id=flow_id,
                summary=DeliverySummary(maxlen=self.summary_size),
            )
            self.flows[flow_id] = flow
        return flow

    def _head(self, flow: Flow) -> Optional[Packet]:
        if not flow.pending:
            return None
        return min(flow.pending, key=lambda p: p.seq)

    def _was_delivered(self, flow: Flow, seq: int) -> bool:
        """这个包号是否交付过：先查内存摘要，摘要窗口没有再回查磁盘。"""
        last = flow.summary.last_seq
        if last is None or seq > last:
            return False
        if seq == last or seq in flow.summary.recent:
            return True
        return self._store.was_delivered(flow.flow_id, seq)

    def _select_batch(self, queue_id: int, count: int) -> List[Packet]:
        """模拟推进本队列，选出本次会交付的报文（不改动任何状态）。"""
        pending_by_flow: Dict[str, List[Packet]] = {}
        for flow in self.flows.values():
            if flow.pending:
                pending_by_flow[flow.flow_id] = sorted(
                    flow.pending, key=lambda p: p.seq
                )
        batch: List[Packet] = []
        while len(batch) < count:
            progressed = False
            for plist in pending_by_flow.values():
                if not plist:
                    continue
                head = plist[0]
                if head.queue_id != queue_id:
                    # 队头在别的队列：这条流本轮等候。
                    continue
                plist.pop(0)  # 只在模拟副本上移除
                batch.append(head)
                progressed = True
                if len(batch) >= count:
                    break
            if not progressed:
                break
        return batch

    # ------------------------------------------------------------------ 操作
    def reset(self) -> None:
        """清空全部状态与磁盘上的交付记录；清盘失败则报错、状态不变。"""
        with self._lock:
            self._store.reset()
            self._reset_unlocked()

    def close(self) -> None:
        """关闭磁盘存储；引擎自建的临时库文件一并删除。"""
        with self._lock:
            self._store.close()
            if self._owned_store_path:
                try:
                    os.unlink(self._owned_store_path)
                except OSError:
                    pass
                self._owned_store_path = None

    def ensure_flow(self, flow_id: str) -> Flow:
        with self._lock:
            return self._flow(flow_id)

    def switch(self, queue_id: int) -> int:
        """把新报文的默认接收队列换成 ``queue_id``，返回切换前的队列。

        已经在各队列里的报文不受影响，仍由收下它们的队列交付；可以反复切换。
        """
        with self._lock:
            previous = self.active_queue
            self.active_queue = self._check_queue(queue_id)
            return previous

    def enqueue(
        self, flow_id: str, seq: int, queue_id: Optional[int] = None
    ) -> EnqueueOutcome:
        """让 ``queue_id`` 收下这条新报文；不指定就交给当前接收队列。"""
        with self._lock:
            flow = self._flow(flow_id)
            if isinstance(seq, bool) or not isinstance(seq, int):
                raise InvalidPacketError("seq 必须是整数")
            if seq < 0:
                raise InvalidPacketError("seq 不能为负数")
            if queue_id is None:
                queue_id = self.active_queue
            self._check_queue(queue_id)

            if any(p.seq == seq for p in flow.pending) or self._was_delivered(
                flow, seq
            ):
                flow.duplicate_attempts += 1
                return EnqueueOutcome(
                    accepted=False,
                    reason="duplicate",
                    detail=f"流 {flow.flow_id} 的包 {seq} 已经收到过，按重复包拒收",
                    flow=flow,
                    packet=Packet(flow.flow_id, seq, queue_id),
                    capacity=self.max_pending,
                )

            last = flow.summary.last_seq
            if last is not None and seq < last:
                flow.stale_attempts += 1
                return EnqueueOutcome(
                    accepted=False,
                    reason="stale",
                    detail=(
                        f"流 {flow.flow_id} 已经交付到包 {last}，"
                        f"包 {seq} 来晚了，无法再按顺序交付"
                    ),
                    flow=flow,
                    packet=Packet(flow.flow_id, seq, queue_id),
                    capacity=self.max_pending,
                )

            if len(flow.pending) >= self.max_pending:
                flow.rejected += 1
                return EnqueueOutcome(
                    accepted=False,
                    reason="backlog_full",
                    detail=(
                        f"流 {flow.flow_id} 已积压 {len(flow.pending)} 个包"
                        f"（上限 {self.max_pending}），只拒收这一流的新包；"
                        "推进队列腾出位置后可重试"
                    ),
                    flow=flow,
                    packet=Packet(flow.flow_id, seq, queue_id),
                    capacity=self.max_pending,
                )

            packet = Packet(flow.flow_id, seq, queue_id)
            flow.pending.append(packet)
            return EnqueueOutcome(
                accepted=True,
                reason="accepted",
                detail=f"包 {seq} 由队列 {queue_id} 收下，等待按流内包号顺序交付",
                flow=flow,
                packet=packet,
                capacity=self.max_pending,
            )

    def deliver(self, queue_id: int, count: int = 1) -> DeliveryOutcome:
        """单独推进一条队列，最多向上一层交付 ``count`` 个报文。

        只交付「队头就在本队列」的流；队头卡在别的队列的流整体等候，
        不会挡住其他流。``count`` 用完后提前结束。

        本批交付记录先整批写盘（事务），写盘成功后才从接收队列移除报文、
        更新内存摘要。写盘失败抛 :class:`StoreError`：整批报文留在各自的
        接收队列，其他队列的同流后续包继续等候；写盘恢复后重试即可，
        交付序号接着上次成功记录。
        """
        with self._lock:
            self._check_queue(queue_id)
            if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                raise ValueError("count 必须是大于等于 1 的整数")

            batch = self._select_batch(queue_id, count)
            if not batch:
                return DeliveryOutcome(
                    queue_id=queue_id,
                    delivered=[],
                    remaining=self.queue_pending_unlocked(queue_id),
                )

            # 交付序号接着上次成功落盘的记录。
            first_order = self._store.last_order() + 1
            orders = list(range(first_order, first_order + len(batch)))
            # 为恢复暂存的状态只随本批增长：写盘成功即清空。
            self._staged = _StagedBatch(
                queue_id=queue_id, orders=tuple(orders), packets=tuple(batch)
            )
            try:
                self._store.append_batch(
                    (order, p.flow_id, p.seq, p.queue_id)
                    for order, p in zip(orders, batch)
                )
            except StoreError:
                # 整批留在接收队列，状态不变；暂存保留本批供排查，
                # 下一次推进会选出新的批次覆盖它。
                raise
            self._staged = None

            for packet in batch:
                flow = self.flows[packet.flow_id]
                flow.pending.remove(packet)
                flow.summary.remember(packet.seq)

            return DeliveryOutcome(
                queue_id=queue_id,
                delivered=list(zip(orders, batch)),
                remaining=self.queue_pending_unlocked(queue_id),
            )

    # ------------------------------------------------------------- 只读查询
    @property
    def egress(self) -> List[Tuple[int, Packet]]:
        """上层已收到的全部报文（从磁盘读取），按交付顺序。"""
        with self._lock:
            return [
                (row["order"], Packet(row["flow_id"], row["seq"], row["queue_id"]))
                for row in self._store.list()
            ]

    def queue_pending_unlocked(self, queue_id: int) -> int:
        return sum(
            1
            for flow in self.flows.values()
            for packet in flow.pending
            if packet.queue_id == queue_id
        )

    def _queue_blocked_count(self, queue_id: int) -> int:
        """本队列里因前序包在别的队列而暂时无法交付的报文数。"""
        blocked = 0
        for flow in self.flows.values():
            waiting_on_other_queue = False
            for packet in sorted(flow.pending, key=lambda p: p.seq):
                if waiting_on_other_queue:
                    if packet.queue_id == queue_id:
                        blocked += 1
                elif packet.queue_id != queue_id:
                    waiting_on_other_queue = True
        return blocked

    def queue_view(self, queue_id: int) -> dict:
        self._check_queue(queue_id)
        pending = sum(
            1
            for flow in self.flows.values()
            for packet in flow.pending
            if packet.queue_id == queue_id
        )
        blocked = self._queue_blocked_count(queue_id)
        flow_counts: Dict[str, int] = {}
        packets = []
        for flow in self.flows.values():
            for packet in sorted(flow.pending, key=lambda p: p.seq):
                if packet.queue_id == queue_id:
                    flow_counts[flow.flow_id] = flow_counts.get(flow.flow_id, 0) + 1
                    packets.append(
                        {
                            "flow_id": packet.flow_id,
                            "seq": packet.seq,
                            "is_flow_head": self._head(flow) is packet,
                        }
                    )
        return {
            "id": queue_id,
            "pending": pending,
            "ready": pending - blocked,
            "blocked_by_other_queue": blocked,
            "receiving": queue_id == self.active_queue,
            "flows": flow_counts,
            "packets": packets,
        }

    def stream_view(self, flow: Flow) -> dict:
        pending = sorted(flow.pending, key=lambda p: p.seq)
        head = pending[0] if pending else None
        summary = flow.summary
        return {
            "flow_id": flow.flow_id,
            "pending": [
                {"seq": p.seq, "queue_id": p.queue_id} for p in pending
            ],
            "head": (
                {"seq": head.seq, "queue_id": head.queue_id}
                if head is not None
                else None
            ),
            "pending_count": len(pending),
            "capacity": self.max_pending,
            "waiting_behind_head": max(0, len(pending) - 1),
            # 全量交付包号从磁盘读取，字段、范围和顺序与原来一致。
            "delivered": self._store.flow_seqs(flow.flow_id),
            "next_expected": (
                summary.last_seq + 1 if summary.last_seq is not None else None
            ),
            "rejected": flow.rejected,
            "duplicate_attempts": flow.duplicate_attempts,
            "stale_attempts": flow.stale_attempts,
        }

    def egress_view(self, flow_id: Optional[str] = None) -> List[dict]:
        if flow_id is not None:
            _clean_flow_id(flow_id)
        with self._lock:
            # 只返回已提交写盘的交付记录；按流查时只取该流的记录。
            return self._store.list(flow_id)

    def state(self) -> dict:
        with self._lock:
            return {
                "config": {
                    "queues": self.n_queues,
                    "active_queue": self.active_queue,
                    "max_pending_per_flow": self.max_pending,
                },
                "streams": [self.stream_view(f) for f in self.flows.values()],
                "queues": [self.queue_view(q) for q in range(self.n_queues)],
                "egress": self.egress_view(),
            }

    def stream_state(self, flow_id: str) -> dict:
        with self._lock:
            flow = self.flows.get(flow_id)
            if flow is None:
                raise UnknownFlowError(flow_id)
            return self.stream_view(flow)
