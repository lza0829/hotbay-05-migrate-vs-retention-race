from __future__ import annotations
from fastapi import HTTPException
from app.db import db
from app.services.core import can_migrate, now, partition_row, require_tier

# 全局争用口径：保留可取消迁移（保留优先）。
# 执行中迁移与保留扫描撞上同一分区时，保留命中即取消执行中单再按策略处置；
# 已取消单不得再被点完成改温层；目录与元数据跟销毁结果。
# 取消与销毁的记账：迁移单置「已取消」并备注「见销毁流水#R」，
# 销毁流水备注登记「取消迁移单#J」，双向互链，争用结局可重放。


def list_jobs():
    with db() as conn:
        rows = conn.execute(
            '''SELECT j.*, p.part_key, p.title FROM migrate_jobs j
               JOIN partitions p ON p.id=j.partition_id ORDER BY j.id DESC'''
        ).fetchall()
        return [dict(r) for r in rows]


def start_migrate(partition_id: int, to_tier: str):
    to_tier = require_tier(to_tier)
    with db() as conn:
        p = partition_row(conn, partition_id)
        if p["status"] != "在册":
            raise HTTPException(400, detail="仅在册分区可迁移")
        if not can_migrate(p["tier"], to_tier):
            raise HTTPException(400, detail="目标温层与现行相同")
        open_job = conn.execute(
            "SELECT 1 FROM migrate_jobs WHERE partition_id=? AND status='执行中'",
            (partition_id,),
        ).fetchone()
        if open_job:
            raise HTTPException(400, detail="已有执行中的迁移，拒绝双迁")
        cur = conn.execute(
            "INSERT INTO migrate_jobs(partition_id, from_tier, to_tier, status, created_at) VALUES (?,?,?,?,?)",
            (partition_id, p["tier"], to_tier, "执行中", now()),
        )
        return {"job_id": cur.lastrowid, "status": "执行中"}


def cancel_open_job(conn, partition_id: int, note: str) -> int | None:
    """保留命中时取消该分区的执行中迁移单，返回被取消的单号；无单或认领落空返回 None。

    UPDATE 带 status='执行中' 守卫：并发下若单已被完成/取消，认领落空，
    调用方不得按已取消记账——争用结局以实际落库者为准，可重放。
    """
    row = conn.execute(
        "SELECT id FROM migrate_jobs WHERE partition_id=? AND status='执行中' ORDER BY id DESC LIMIT 1",
        (partition_id,),
    ).fetchone()
    if not row:
        return None
    cur = conn.execute(
        "UPDATE migrate_jobs SET status='已取消', finished_at=?, note=? WHERE id=? AND status='执行中'",
        (now(), note, row["id"]),
    )
    return row["id"] if cur.rowcount else None


def finish_migrate(job_id: int, success: bool = True):
    healed = None
    result = None
    with db() as conn:
        j = conn.execute("SELECT * FROM migrate_jobs WHERE id=?", (job_id,)).fetchone()
        if not j:
            raise HTTPException(404, detail="迁移单不存在")
        if j["status"] == "已取消":
            raise HTTPException(409, detail="迁移单已被保留命中取消，不得再完成改温层")
        if j["status"] != "执行中":
            raise HTTPException(400, detail="迁移单不在执行中")
        p = partition_row(conn, j["partition_id"])
        if p["status"] != "在册":
            # 毁完仍占坑的半截态：分区已被保留处置而单未取消。
            # 同事务补记取消（回滚到争用前可解释快照：单已取消、分区随保留终态），
            # 提交后硬拒本次完成。
            cur = conn.execute(
                "UPDATE migrate_jobs SET status='已取消', finished_at=?, note=? WHERE id=? AND status='执行中'",
                (now(), f"分区{p['status']}，执行中态自愈取消", job_id),
            )
            if not cur.rowcount:
                raise HTTPException(409, detail="迁移单状态已变更，请刷新后核对")
            healed = p["status"]
        elif success:
            # 温层落账与单结清均带状态守卫：任一落空说明保留并发已抢先，
            # 抛错整体回滚到争用前快照，不留半截。
            cur = conn.execute(
                "UPDATE partitions SET tier=? WHERE id=? AND status='在册'",
                (j["to_tier"], j["partition_id"]),
            )
            if not cur.rowcount:
                raise HTTPException(409, detail="分区已被保留处置，本次完成已回滚")
            cur = conn.execute(
                "UPDATE migrate_jobs SET status='已完成', finished_at=? WHERE id=? AND status='执行中'",
                (now(), job_id),
            )
            if not cur.rowcount:
                raise HTTPException(409, detail="迁移单状态已变更（疑似保留并发），本次完成已回滚")
            result = {"job_id": job_id, "status": "已完成", "tier": j["to_tier"]}
        else:
            cur = conn.execute(
                "UPDATE migrate_jobs SET status='已失败', finished_at=?, note='执行失败回滚温层' WHERE id=? AND status='执行中'",
                (now(), job_id),
            )
            if not cur.rowcount:
                raise HTTPException(409, detail="迁移单状态已变更（疑似保留并发），本次失败回滚未入账")
            result = {"job_id": job_id, "status": "已失败"}
    if healed:
        raise HTTPException(409, detail=f"分区{healed}，执行中迁移单已自愈取消，不得再完成")
    return result
