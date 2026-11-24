"""分群成员共用取数。

分群引擎的重叠计算与分群对比的属性差异计算原先各写了一遍
`SELECT * FROM segment_members WHERE segment_id=?`。这里收拢为唯一入口，
保证两个调用方拿到的成员编号与属性出自同一份已确认快照。

取数规则：
- 只读取已发布的 ``segment_members``，重算暂存区
  ``segment_member_staging`` 永远不参与计算，避免把重算中的半成品当成群。
- 成员、成员属性、所属计算版 ``member_version`` 由同一条 SQL 取得，
  不会出现成员编号来自旧版、属性来自新版之类的混版。
- ``status='计算中'`` 时：
    * 仍有上一次已发布的完整成员 → ``recomputing``，继续使用该完整集合；
    * 没有任何已发布成员 → ``recomputing_empty``，这是重算清空态，
      与“确实零成员”的 ``confirmed_empty`` 明确区分，调用方不得按空群出结果。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Sequence

from app.database import db

STATE_CONFIRMED = "confirmed"
STATE_CONFIRMED_EMPTY = "confirmed_empty"
STATE_RECOMPUTING = "recomputing"
STATE_RECOMPUTING_EMPTY = "recomputing_empty"
STATE_MISSING = "missing"


@dataclass(frozen=True)
class SegmentMember:
    """一个已确认的分群成员。"""

    member_key: str
    attrs: dict[str, Any] | None
    raw_attrs: Any


@dataclass(frozen=True)
class SegmentMemberSnapshot:
    """一次取数得到的单个分群成员快照。"""

    segment_id: int
    exists: bool
    state: str
    member_version: int
    members: tuple[SegmentMember, ...]

    @property
    def member_keys(self) -> set[str]:
        return {member.member_key for member in self.members}

    def audit(self, include_attrs: bool = False) -> dict[str, Any]:
        """返回可供复核的取数快照：用的哪一版、哪些成员。"""
        payload: dict[str, Any] = {
            "segment_id": self.segment_id,
            "state": self.state,
            "member_version": self.member_version,
            "member_count": len(self.members),
            "member_keys": sorted(self.member_keys),
        }
        if include_attrs:
            payload["members"] = [
                {"member_key": member.member_key, "member_attrs": member.attrs}
                for member in self.members
            ]
        return payload


def _parse_attrs(raw: Any) -> dict[str, Any] | None:
    """解析成员属性；无法解析时返回 None，由调用方记录过滤原因。"""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def _build_state(status: str | None, member_count: int) -> str:
    recomputing = status == "计算中"
    if member_count:
        return STATE_RECOMPUTING if recomputing else STATE_CONFIRMED
    return STATE_RECOMPUTING_EMPTY if recomputing else STATE_CONFIRMED_EMPTY


def fetch_segment_member_snapshots(segment_ids: Sequence[int | None]) -> dict[int, SegmentMemberSnapshot]:
    """按分群编号批量取数，同一条 SQL 内完成成员与版本的一致性读取。

    返回 ``{segment_id: snapshot}``；不存在的分群也会得到 ``missing`` 快照，
    保持原有“不存在即按空集合处理”的行为。
    """
    normalized: list[int] = []
    for segment_id in segment_ids:
        if segment_id is None:
            continue
        try:
            value = int(segment_id)
        except (TypeError, ValueError):
            continue
        if value not in normalized:
            normalized.append(value)

    if not normalized:
        return {}

    placeholders = ", ".join("?" for _ in normalized)
    rows = db.fetchall(
        f"""
        SELECT s.id AS segment_id,
               s.status AS segment_status,
               s.member_version AS member_version,
               m.id AS member_row_id,
               m.member_key AS member_key,
               m.member_attrs AS member_attrs
        FROM segments s
        LEFT JOIN segment_members m
               ON m.segment_id = s.id
              AND m.member_version = s.member_version
        WHERE s.id IN ({placeholders})
        ORDER BY s.id, m.id
        """,
        normalized,
    )

    metadata: dict[int, dict[str, Any]] = {}
    members: dict[int, list[SegmentMember]] = {segment_id: [] for segment_id in normalized}
    for row in rows:
        segment_id = int(row["segment_id"])
        metadata[segment_id] = row
        if row["member_key"] is None:
            continue
        raw_attrs = row["member_attrs"]
        members[segment_id].append(SegmentMember(
            member_key=str(row["member_key"]),
            attrs=_parse_attrs(raw_attrs),
            raw_attrs=raw_attrs,
        ))

    snapshots: dict[int, SegmentMemberSnapshot] = {}
    for segment_id in normalized:
        meta = metadata.get(segment_id)
        if meta is None:
            snapshots[segment_id] = SegmentMemberSnapshot(
                segment_id=segment_id,
                exists=False,
                state=STATE_MISSING,
                member_version=0,
                members=(),
            )
            continue
        segment_members = tuple(members.get(segment_id, ()))
        status = meta.get("segment_status")
        snapshots[segment_id] = SegmentMemberSnapshot(
            segment_id=segment_id,
            exists=True,
            state=_build_state(status, len(segment_members)),
            member_version=int(meta.get("member_version") or 0),
            members=segment_members,
        )
    return snapshots


def fetch_segment_member_snapshot(segment_id: int | None) -> SegmentMemberSnapshot:
    """单个分群取数，语义与批量版本一致。"""
    snapshots = fetch_segment_member_snapshots([segment_id])
    if snapshots:
        return next(iter(snapshots.values()))
    return SegmentMemberSnapshot(
        segment_id=int(segment_id) if segment_id is not None else 0,
        exists=False,
        state=STATE_MISSING,
        member_version=0,
        members=(),
    )


def snapshot_for(segment_id: Any) -> SegmentMemberSnapshot:
    """按原样传入的分群标识取数，非法标识按不存在处理，不抛异常。"""
    try:
        key = int(segment_id)
    except (TypeError, ValueError):
        return SegmentMemberSnapshot(
            segment_id=0, exists=False, state=STATE_MISSING, member_version=0, members=(),
        )
    return fetch_segment_member_snapshot(key)
