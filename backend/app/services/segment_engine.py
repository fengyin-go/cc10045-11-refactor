"""分群引擎领域服务：按行为标签和属性条件自动生成分群结果。

真实数据流：定义分群条件 → 调用 pipeline.compute_segment 从 events 表匹配成员
→ 去重写入 segment_members 表 → 下游分群对比和漏斗分析消费。
"""
from __future__ import annotations

from typing import Any

from app.database import db
from app.pipeline import compute_segment
from app.segment_store import fetch_segment_member_snapshots, snapshot_for
from app.services.base import BaseModuleService


class SegmentEngineService(BaseModuleService):
    MODULE = "segment_engine"
    ENTITY = "分群定义"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["分群名称", "分群条件", "所属人口特征", "预设大小", "最近更新时间"]
    REQUIRED_FIELDS = ["分群名称", "分群条件"]
    STATUS_ORDER = ["草稿", "计算中", "已生成", "已过期"]
    ACTION_RULES = {"启动计算": "已生成", "标记过期": "已过期"}
    NEGATIVE_ACTIONS = []

    def run_computation(self, segment_id: int) -> dict[str, Any]:
        """执行分群计算。"""
        segment = db.fetchone("SELECT * FROM segments WHERE id=?", [segment_id])
        if segment is None:
            return {"ok": False, "message": f"分群 {segment_id} 不存在"}

        db.update("segments", {"status": "计算中"}, "id=?", [segment_id])
        count = compute_segment(dict(segment))
        return {
            "ok": True,
            "segment_name": segment["segment_name"],
            "members": count,
            "status": "已生成",
        }

    def list_members(self, segment_id: int, page: int = 1, size: int = 50) -> tuple[list[dict[str, Any]], int]:
        """列出分群成员。

        只列已确认版本：成员与分群当前 ``member_version`` 一致才算数，
        重算暂存区和旧版残留不会混进列表。
        """
        where = (
            "segment_id=? AND member_version="
            "(SELECT member_version FROM segments WHERE id=?)"
        )
        params = [segment_id, segment_id]
        total = db.count("segment_members", where, params)
        rows = db.fetchall(
            f"SELECT * FROM segment_members WHERE {where} ORDER BY id LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return rows, total

    def member_overlap(self, segment_id_a: int, segment_id_b: int) -> dict[str, Any]:
        """计算两个分群的成员重叠度。

        成员走共用取数：只用已确认的完整成员集合，重算中的暂存成员不参与，
        重算清空态不与“确实为零成员”混同。输出字段与原有口径保持不变。
        """
        snapshots = fetch_segment_member_snapshots([segment_id_a, segment_id_b])
        snap_a = snapshots.get(int(segment_id_a)) or snapshot_for(segment_id_a)
        snap_b = snapshots.get(int(segment_id_b)) or snapshot_for(segment_id_b)

        members_a = snap_a.member_keys
        members_b = snap_b.member_keys

        intersection = members_a & members_b
        union = members_a | members_b
        jaccard = round(len(intersection) / max(len(union), 1), 4)
        overlap_a = round(len(intersection) / max(len(members_a), 1), 4)
        overlap_b = round(len(intersection) / max(len(members_b), 1), 4)

        return {
            "segment_a_size": len(members_a),
            "segment_b_size": len(members_b),
            "intersection": len(intersection),
            "jaccard_similarity": jaccard,
            "overlap_rate_a": overlap_a,
            "overlap_rate_b": overlap_b,
            "overlap_members": sorted(intersection),
            "sample_basis": {
                "segment_a": snap_a.audit(),
                "segment_b": snap_b.audit(),
            },
        }

    def attribute_summary(self, segment_id: int) -> dict[str, Any]:
        """汇总分群成员的属性分布。"""
        snapshot = fetch_segment_member_snapshots([segment_id])[int(segment_id)]
        rows = snapshot.members
        if not rows:
            return {"count": 0, "attributes": {}}

        from collections import Counter
        attr_counts: dict[str, Counter[str]] = {}
        for r in rows:
            attrs = r.attrs
            if attrs is None:
                continue
            for k, v in attrs.items():
                if k not in attr_counts:
                    attr_counts[k] = Counter()
                attr_counts[k][str(v)] += 1

        return {
            "count": len(rows),
            "attributes": {
                k: dict(v.most_common(10)) for k, v in attr_counts.items()
            },
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_segments"] = db.count("segments")
        snapshot["total_members"] = db.count("segment_members")
        snapshot["generated_count"] = db.count("segments", "status='已生成'")
        return snapshot