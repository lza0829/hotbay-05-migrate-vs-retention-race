from __future__ import annotations
from datetime import date
from fastapi import HTTPException
from app.db import db
from app.services.core import now
from app.services.migrate import cancel_open_job

# 全局争用口径：保留可取消迁移（保留优先）。
# 命中在册分区即先取消其执行中迁移单（同事务），再按策略销毁或标过期；
# 销毁流水备注登记「取消迁移单#J」，迁移单备注回登记「见销毁流水#R」，
# 双向互链，争用结局可重放；目录与元数据跟销毁结果。


def list_policies():
    with db() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM retention_policies ORDER BY id").fetchall()]


def list_destroys():
    with db() as conn:
        rows = conn.execute(
            '''SELECT d.*, p.part_key, p.title, pol.name AS policy_name
               FROM destroy_runs d
               JOIN partitions p ON p.id=d.partition_id
               JOIN retention_policies pol ON pol.id=d.policy_id
               ORDER BY d.id DESC'''
        ).fetchall()
        return [dict(r) for r in rows]


def run_retention(policy_id: int, mode: str = "连目录销毁"):
    if mode not in ("连目录销毁", "只删元数据"):
        raise HTTPException(400, detail="销毁模式无效")
    with db() as conn:
        pol = conn.execute("SELECT * FROM retention_policies WHERE id=? AND active=1", (policy_id,)).fetchone()
        if not pol:
            raise HTTPException(404, detail="保留策略不存在")
        biz = conn.execute("SELECT value FROM settings WHERE key='营业日'").fetchone()
        today = date.fromisoformat(biz["value"] if biz else date.today().isoformat())
        parts = conn.execute("SELECT * FROM partitions WHERE status='在册'").fetchall()
        done = []
        for p in parts:
            age = (today - date.fromisoformat(p["release_day"])).days
            if age < pol["max_age_days"]:
                continue
            # 保留优先：命中即取消执行中迁移单（带守卫的原子认领），同事务再处置。
            # 认领落空（并发下完成已抢先）则不算取消，销毁照常入账。
            cancelled = cancel_open_job(conn, p["id"], "保留命中取消")
            note = f"片龄{age}天"
            if cancelled:
                note += f"；取消迁移单#{cancelled}"
            if pol["action"] == "只标过期":
                conn.execute("UPDATE partitions SET status='已过期' WHERE id=?", (p["id"],))
                run_status = "已标记过期"
            else:
                clips = conn.execute("SELECT object_key FROM clips WHERE partition_id=?", (p["id"],)).fetchall()
                if mode == "连目录销毁":
                    for c in clips:
                        conn.execute("DELETE FROM inventory_objects WHERE object_key=?", (c["object_key"],))
                conn.execute("DELETE FROM clips WHERE partition_id=?", (p["id"],))
                conn.execute("UPDATE partitions SET status='已销毁' WHERE id=?", (p["id"],))
                run_status = "已销毁"
            cur = conn.execute(
                "INSERT INTO destroy_runs(partition_id, policy_id, mode, status, created_at, note) VALUES (?,?,?,?,?,?)",
                (p["id"], policy_id, mode, run_status, now(), note),
            )
            run_id = cur.lastrowid
            if cancelled:
                conn.execute(
                    "UPDATE migrate_jobs SET note=? WHERE id=?",
                    (f"保留命中取消，见销毁流水#{run_id}", cancelled),
                )
            done.append({
                "partition_id": p["id"],
                "part_key": p["part_key"],
                "run_id": run_id,
                "status": run_status,
                "cancelled_job_id": cancelled,
            })
        return {"policy": pol["name"], "affected": done}
