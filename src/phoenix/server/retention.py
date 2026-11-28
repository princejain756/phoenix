from __future__ import annotations

import logging
from asyncio import create_task, gather, sleep
from datetime import datetime, timedelta, timezone
from time import time
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import selectinload

from phoenix.db.constants import DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID
from phoenix.db.helpers import SupportedSQLDialect
from phoenix.db.models import Project, ProjectSession, ProjectTraceRetentionPolicy, Trace
from phoenix.db.types.trace_retention import _MaxCount, _MaxDays
from phoenix.server.dml_event import SpanDeleteEvent
from phoenix.server.dml_event_handler import DmlEventHandler
from phoenix.server.prometheus import (
    RETENTION_POLICY_EXECUTIONS,
    RETENTION_SWEEPER_LAST_RUN,
)
from phoenix.server.types import DaemonTask, DbSessionFactory
from phoenix.utilities import hour_of_week

logger = logging.getLogger(__name__)

_ORPHAN_SESSION_DELETE_BATCH_SIZE = 1000
_TRACE_DELETE_BATCH_SIZE = 1000
_ORPHAN_SESSION_GRACE_PERIOD = timedelta(hours=1)


class TraceDataSweeper(DaemonTask):
    def __init__(self, db: DbSessionFactory, dml_event_handler: DmlEventHandler):
        super().__init__()
        self._db = db
        self._dml_event_handler = dml_event_handler

    async def _run(self) -> None:
        """Check hourly, apply policies, then clean up orphaned sessions."""
        while self._running:
            await self._sleep_until_next_hour()
            RETENTION_SWEEPER_LAST_RUN.set(time())
            try:
                if policies := await self._get_policies():
                    current_hour = self._current_hour()
                    if tasks := [
                        create_task(self._apply(policy))
                        for policy in policies
                        if self._should_apply(policy, current_hour)
                    ]:
                        await gather(*tasks, return_exceptions=True)
            except Exception:
                logger.exception("Unexpected error in retention sweeper main loop")
            try:
                await self._delete_orphan_sessions()
            except Exception:
                logger.exception("Failed to delete orphaned project sessions")

    async def _get_policies(self) -> list[ProjectTraceRetentionPolicy]:
        stmt = sa.select(ProjectTraceRetentionPolicy).options(
            selectinload(ProjectTraceRetentionPolicy.projects).load_only(Project.id)
        )
        async with self._db() as session:
            result = await session.scalars(stmt)
        # filter out no-op policies, e.g. max_days == 0
        return [policy for policy in result if bool(policy.rule)]

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _current_hour(self) -> int:
        return hour_of_week(self._now())

    def _should_apply(self, policy: ProjectTraceRetentionPolicy, current_hour: int) -> bool:
        if current_hour != policy.cron_expression.get_hour_of_prev_run():
            return False
        if policy.id != DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID and not policy.projects:
            return False
        return True

    async def _project_ids_for_policy(self, policy: ProjectTraceRetentionPolicy) -> list[int]:
        if policy.id == DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID:
            stmt = sa.select(Project.id).where(Project.trace_retention_policy_id.is_(None))
            async with self._db() as session:
                return list((await session.scalars(stmt)).all())
        return [p.id for p in policy.projects]

    async def _max_count_cutoff(
        self,
        project_id: int,
        max_count: int,
        *,
        min_start_time: Optional[datetime] = None,
    ) -> Optional[tuple[datetime, int]]:
        stmt = (
            sa.select(Trace.start_time, Trace.id)
            .where(Trace.project_rowid == project_id)
            .order_by(Trace.start_time.desc(), Trace.id.desc())
            .offset(max_count)
            .limit(1)
        )
        if min_start_time is not None:
            stmt = stmt.where(Trace.start_time >= min_start_time)
        async with self._db() as session:
            row = (await session.execute(stmt)).first()
        return (row[0], row[1]) if row is not None else None

    async def _delete_in_batches(self, deletion_filter: sa.ColumnElement[bool]) -> bool:
        while self._running:
            candidate_ids = (
                sa.select(Trace.id).where(deletion_filter).limit(_TRACE_DELETE_BATCH_SIZE)
            )
            if self._db.dialect is SupportedSQLDialect.POSTGRESQL:
                candidate_ids = candidate_ids.with_for_update(skip_locked=True)
            stmt = (
                sa.delete(Trace).where(Trace.id.in_(candidate_ids)).returning(Trace.project_rowid)
            )
            async with self._db() as session:
                deleted_project_rowids = (await session.scalars(stmt)).all()
            # Publish only committed changes, including batches before a later failure.
            # RETURNING is bounded by the batch size even for a large backlog.
            if deleted_project_rowids:
                self._dml_event_handler.put(
                    SpanDeleteEvent(tuple(sorted(set(deleted_project_rowids))))
                )
            if len(deleted_project_rowids) < _TRACE_DELETE_BATCH_SIZE:
                return True
        return False

    async def _apply(self, policy: ProjectTraceRetentionPolicy) -> None:
        """Delete matching traces in independently committed batches.

        When ``max_count`` is active, each project's ``(start_time, id)`` cutoff
        is computed once per sweep before batched deletion so large projects do
        not re-rank all traces on every batch. PostgreSQL skips locked candidates.
        """
        try:
            if not self._running:
                return
            rule = policy.rule.root
            max_days = rule.max_days if isinstance(rule, _MaxDays) else 0.0
            max_count = rule.max_count if isinstance(rule, _MaxCount) else 0
            max_days_cutoff = self._now() - timedelta(days=max_days) if max_days > 0 else None

            if max_count <= 0:
                if max_days_cutoff is not None:
                    project_rowids = (
                        sa.select(Project.id).where(Project.trace_retention_policy_id.is_(None))
                        if policy.id == DEFAULT_PROJECT_TRACE_RETENTION_POLICY_ID
                        else [p.id for p in policy.projects]
                    )
                    if isinstance(project_rowids, list) and not project_rowids:
                        RETENTION_POLICY_EXECUTIONS.labels(status="success").inc()
                        return
                    deletion_filter = sa.and_(
                        Trace.project_rowid.in_(project_rowids),
                        Trace.start_time < max_days_cutoff,
                    )
                    if not await self._delete_in_batches(deletion_filter):
                        return
            else:
                project_ids = await self._project_ids_for_policy(policy)
                for project_id in project_ids:
                    if not self._running:
                        return
                    # Scope the offset lookup to non-expired traces when max_days is also set;
                    # whenever a cutoff row exists, cutoff_start_time >= max_days_cutoff, so
                    # traces at or before the cutoff subsume start_time < max_days_cutoff.
                    cutoff = await self._max_count_cutoff(
                        project_id,
                        max_count,
                        min_start_time=max_days_cutoff,
                    )
                    if cutoff is not None:
                        cutoff_start_time, cutoff_id = cutoff
                        # Explicit start_time <= cutoff_start_time enables an index range scan on
                        # ix_traces_project_rowid_start_time while id breaks ties deterministically.
                        deletion_filter = sa.and_(
                            Trace.project_rowid == project_id,
                            Trace.start_time <= cutoff_start_time,
                            sa.or_(
                                Trace.start_time < cutoff_start_time,
                                Trace.id <= cutoff_id,
                            ),
                        )
                    elif max_days_cutoff is not None:
                        deletion_filter = sa.and_(
                            Trace.project_rowid == project_id,
                            Trace.start_time < max_days_cutoff,
                        )
                    else:
                        continue
                    if not await self._delete_in_batches(deletion_filter):
                        return

            RETENTION_POLICY_EXECUTIONS.labels(status="success").inc()
        except Exception:
            logger.exception(f"Failed to apply retention policy '{policy.name}' (id={policy.id})")
            RETENTION_POLICY_EXECUTIONS.labels(status="error").inc()

    async def _delete_orphan_sessions(self) -> None:
        """Delete session rows whose traces are all gone and whose last activity is old.

        Trace deletion (retention rules, bulk deletes) does not cascade upward to
        ``project_sessions``, so sessions whose traces have all been deleted
        linger as ghosts in the sessions UI. Only sessions quiet for the grace
        period are deleted: ``end_time`` advances with each new trace, so an
        active session never qualifies, and the grace period keeps the sweep
        clear of in-flight ingestion (a session row is created just before its
        first trace commits). Deleting a session cascades to its session
        annotations, consistent with trace deletion cascading to span/trace
        annotations. Deletes run in batches to bound transaction time.

        Two guards close a race with a session resuming mid-sweep (ingestion
        get-or-creates session rows without locking, and the traces FK cascades
        on session delete, so an unguarded delete could cascade away a
        just-committed trace):

        - ``end_time < cutoff`` is repeated as a direct qual on the DELETE.
          Under READ COMMITTED, if the DELETE blocks on a concurrent ingestion
          transaction that advanced the row's ``end_time``, the recheck against
          the new row version excludes it (the subquery's snapshot would not).
        - On PostgreSQL, candidates are locked with FOR UPDATE SKIP LOCKED, so
          any session row currently locked by in-flight ingestion (its UPDATE,
          or a trace INSERT's KEY SHARE on the parent) is skipped this sweep
          and reconsidered next hour. SQLite serializes writers, so the race
          cannot occur there.

        A narrower race remains if the sweeper commits after ingestion selects
        an old orphaned session but before ingestion flushes its trace. In that
        case the single span insert fails inside the bulk inserter's per-span
        savepoint, is logged and metered, and the batch continues. We accept
        that tradeoff here because it is the same pre-existing race carried by
        explicit session/project deletes; a retry in the ingestion path would
        address all such deletes together.
        """
        cutoff = self._now() - _ORPHAN_SESSION_GRACE_PERIOD
        total_deleted = 0
        while self._running:
            candidate_ids = (
                sa.select(ProjectSession.id)
                .where(ProjectSession.end_time < cutoff)
                .where(
                    ~sa.select(sa.literal(1))
                    .where(Trace.project_session_rowid == ProjectSession.id)
                    .exists()
                )
                .limit(_ORPHAN_SESSION_DELETE_BATCH_SIZE)
            )
            if self._db.dialect is SupportedSQLDialect.POSTGRESQL:
                candidate_ids = candidate_ids.with_for_update(skip_locked=True)
            stmt = (
                sa.delete(ProjectSession)
                .where(ProjectSession.id.in_(candidate_ids))
                .where(ProjectSession.end_time < cutoff)
                .returning(ProjectSession.id)
            )
            async with self._db() as session:
                deleted = len((await session.scalars(stmt)).all())
            total_deleted += deleted
            if deleted < _ORPHAN_SESSION_DELETE_BATCH_SIZE:
                break
        if total_deleted:
            logger.info(
                f"Deleted {total_deleted} orphaned project session(s) with no remaining "
                f"traces and no activity since {cutoff.isoformat()}"
            )

    async def _sleep_until_next_hour(self) -> None:
        next_hour = self._now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        await sleep((next_hour - self._now()).total_seconds())
