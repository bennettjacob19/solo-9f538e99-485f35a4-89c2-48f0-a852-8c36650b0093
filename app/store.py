"""逐包交付记录的本地磁盘存储（SQLite，仅依赖标准库）。

每一次交付（全局交付序号、流、包号、来源队列）都落到这里，内存里不再
保留全量历史。写盘以事务提交：一批记录要么全部落盘，要么全部回滚；
失败抛 :class:`StoreError`，连接保持可用，写盘恢复后可原样重试。

历史不跨进程保留：打开存储时会重建空表，交付序号从 1 重新开始；
``reset()`` 也会清空全部记录。
"""

from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple

# 一条交付记录：(全局交付序号, 流, 包号, 收下它的队列)
DeliveryRow = Tuple[int, str, int, int]


class StoreError(RuntimeError):
    """交付记录读写盘失败。"""


_SCHEMA = """
CREATE TABLE deliveries (
    seq_order INTEGER PRIMARY KEY,
    flow_id   TEXT NOT NULL,
    seq       INTEGER NOT NULL,
    queue_id  INTEGER NOT NULL
)
"""
_INDEX = "CREATE INDEX idx_deliveries_flow ON deliveries(flow_id, seq)"


class DeliveryStore:
    """本地磁盘上的逐包交付记录。

    重新打开同一个文件会清空旧记录：进程重启后历史清零，交付序号从 1
    重新开始。所有方法失败都抛 :class:`StoreError`。
    """

    def __init__(self, path: str) -> None:
        self.path = os.fspath(path)
        try:
            if self.path != ":memory:":
                parent = os.path.dirname(os.path.abspath(self.path))
                os.makedirs(parent, exist_ok=True)
            # 引擎侧用锁串行化所有访问，这里允许多线程共用同一连接。
            self._conn = sqlite3.connect(self.path, check_same_thread=False)
        except sqlite3.Error as exc:
            raise StoreError(f"无法打开交付记录存储 {self.path!r}：{exc}") from exc
        self._wipe()

    # -------------------------------------------------------------- 内部
    def _wipe(self) -> None:
        try:
            with self._conn:
                self._conn.execute("DROP TABLE IF EXISTS deliveries")
                self._conn.execute(_SCHEMA)
                self._conn.execute(_INDEX)
        except sqlite3.Error as exc:
            raise StoreError(f"初始化交付记录存储失败：{exc}") from exc

    # -------------------------------------------------------------- 写入
    def append_batch(self, rows: Iterable[DeliveryRow]) -> None:
        """把一批交付记录一次性落盘；任何一条失败整批回滚。"""
        batch = list(rows)
        try:
            with self._conn:  # 事务：全部成功才提交，失败自动回滚
                self._conn.executemany(
                    "INSERT INTO deliveries(seq_order, flow_id, seq, queue_id)"
                    " VALUES (?, ?, ?, ?)",
                    batch,
                )
        except sqlite3.Error as exc:
            raise StoreError(f"交付记录写盘失败：{exc}") from exc

    # -------------------------------------------------------------- 查询
    def last_order(self) -> int:
        """最后一次成功落盘的交付序号；没有记录时返回 0。"""
        try:
            cur = self._conn.execute(
                "SELECT COALESCE(MAX(seq_order), 0) FROM deliveries"
            )
            return int(cur.fetchone()[0])
        except sqlite3.Error as exc:
            raise StoreError(f"读取交付序号失败：{exc}") from exc

    def was_delivered(self, flow_id: str, seq: int) -> bool:
        """这条流的这个包号是否交付过（用来区分重复包与过期包）。"""
        try:
            cur = self._conn.execute(
                "SELECT 1 FROM deliveries WHERE flow_id = ? AND seq = ? LIMIT 1",
                (flow_id, seq),
            )
            return cur.fetchone() is not None
        except sqlite3.Error as exc:
            raise StoreError(f"查询交付记录失败：{exc}") from exc

    def flow_seqs(self, flow_id: str) -> List[int]:
        """这条流已交付的全部包号，按交付先后排列。"""
        try:
            cur = self._conn.execute(
                "SELECT seq FROM deliveries WHERE flow_id = ? ORDER BY seq_order",
                (flow_id,),
            )
            return [row[0] for row in cur.fetchall()]
        except sqlite3.Error as exc:
            raise StoreError(f"查询流的交付记录失败：{exc}") from exc

    def list(self, flow_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """交付记录，按全局交付序号升序；可按流过滤，只取该流的记录。"""
        sql = "SELECT seq_order, flow_id, seq, queue_id FROM deliveries"
        args: Tuple[Any, ...] = ()
        if flow_id is not None:
            sql += " WHERE flow_id = ?"
            args = (flow_id,)
        sql += " ORDER BY seq_order"
        try:
            cur = self._conn.execute(sql, args)
            return [
                {"order": order, "flow_id": fid, "seq": seq, "queue_id": qid}
                for order, fid, seq, qid in cur.fetchall()
            ]
        except sqlite3.Error as exc:
            raise StoreError(f"查询交付记录失败：{exc}") from exc

    # -------------------------------------------------------------- 维护
    def reset(self) -> None:
        """清空全部交付记录（之后的交付序号从 1 重新开始）。"""
        try:
            with self._conn:
                self._conn.execute("DELETE FROM deliveries")
        except sqlite3.Error as exc:
            raise StoreError(f"清空交付记录失败：{exc}") from exc

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DeliveryStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
