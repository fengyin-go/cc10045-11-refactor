"""分群对比领域服务：执行两个分群在多个维度上的统计差异对比。

真实数据流：指定分群A/分群B和对比维度
→ pipeline.compute_cohort_comparison 从 segment_members 读取成员属性
→ 计算均值差异/标准差/t-test显著性 → 写入 cohort_comparisons。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import compute_cohort_comparison
from app.services.base import BaseModuleService


class CohortCompareService(BaseModuleService):
    MODULE = "cohort_compare"
    ENTITY = "对比报告"
    OPERATION_TERM = "对比"
    LIST_FIELDS = ["对比名称", "分群A", "分群B", "对比维度", "差异显著性"]
    REQUIRED_FIELDS = ["对比名称", "分群A", "分群B", "对比维度"]
    STATUS_ORDER = ["待计算", "计算中", "已完成", "计算失败"]
    ACTION_RULES = {"启动对比": "已完成", "取消对比": "计算失败"}
    NEGATIVE_ACTIONS = []

    def run_comparison(self, entry_id: int) -> dict[str, Any]:
        """执行分群对比。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            db_entry = db.fetchone("SELECT * FROM cohort_comparisons WHERE id=?", [entry_id])
            if db_entry is None:
                return {"ok": False, "message": f"对比记录 {entry_id} 不存在"}
            entry = dict(db_entry)

        db.update("cohort_comparisons", {"status": "计算中"}, "id=?", [entry_id])

        result = compute_cohort_comparison(entry)

        if not result.get("ok", True):
            # 重算清空态没有已确认成员，不能把临时成员当确认的群出结果；
            # 对比状态落为计算失败，错误摘要一并留档供复核。
            db.update("cohort_comparisons", {
                "status": "计算失败",
                "result_summary": result["result_summary"],
            }, "id=?", [entry_id])
            return {
                "ok": False,
                "message": result.get("message", "分群成员尚未确认，无法对比"),
                "comparison_name": result["comparison_name"],
                "status": "计算失败",
            }

        db.update("cohort_comparisons", {
            "status": "已完成",
            "result_summary": result["result_summary"],
        }, "id=?", [entry_id])

        return {
            "ok": True,
            "comparison_name": result["comparison_name"],
            "significant_count": result["significant_count"],
            "dimensions": result["dimension_results"],
        }

    def comparison_result(self, entry_id: int) -> dict[str, Any] | None:
        """获取对比结果。"""
        entry = db.fetchone("SELECT * FROM cohort_comparisons WHERE id=?", [entry_id])
        if entry is None:
            return None
        result = {"status": entry["status"]}
        if entry["result_summary"]:
            try:
                result["summary"] = json.loads(entry["result_summary"])
            except json.JSONDecodeError:
                result["summary"] = entry["result_summary"]
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_comparisons"] = db.count("cohort_comparisons")
        snapshot["completed"] = db.count("cohort_comparisons", "status='已完成'")
        return snapshot