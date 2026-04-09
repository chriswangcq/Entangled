"""
Database Lock Management

Provides flexible lock interfaces for database operations.
Current implementation: FIFO queue
Future: Can be upgraded to priority queue or other strategies
"""

import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)


class LockStrategy(ABC):
    """
    Abstract lock strategy interface.

    Allows different lock implementations (FIFO, Priority, etc.)
    """

    @abstractmethod
    @contextmanager
    def acquire(self, **kwargs):
        """
        Acquire lock with parameters.

        Args:
            **kwargs: Strategy-specific parameters
                - resource_id: Resource identifier (for sharding)
                - priority: Priority level (for future priority queue)
                - timeout: Maximum wait time
        """
        pass


class FIFOLock(LockStrategy):
    """
    FIFO (First-In-First-Out) lock implementation.

    Guarantees:
    - Requests are served in order of arrival
    - No starvation
    - Fairness

    Example:
        lock = FIFOLock()
        with lock.acquire(resource_id="msg_123", timeout=5.0):
            # Do database operation
            pass
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._queue = deque()
        self._metrics = {
            "total_acquires": 0,
            "current_waiting": 0,
            "max_waiting": 0,
        }

    @contextmanager
    def acquire(self, **kwargs):
        """
        Acquire FIFO lock.

        Args:
            resource_id (str, optional): Resource identifier (for logging)
            timeout (float, optional): Maximum wait time in seconds

        Raises:
            TimeoutError: If timeout is exceeded
        """
        resource_id = kwargs.get("resource_id", "unknown")
        timeout = kwargs.get("timeout")

        event = threading.Event()
        start_time = time.time()

        # Add to queue
        with self._lock:
            self._queue.append(event)
            self._metrics["total_acquires"] += 1
            self._metrics["current_waiting"] = len(self._queue)
            self._metrics["max_waiting"] = max(
                self._metrics["max_waiting"],
                self._metrics["current_waiting"]
            )

            # If first in queue, start immediately
            if len(self._queue) == 1:
                event.set()

        # Wait for my turn
        if timeout:
            acquired = event.wait(timeout=timeout)
            if not acquired:
                # Remove from queue on timeout
                with self._lock:
                    try:
                        self._queue.remove(event)
                        self._metrics["current_waiting"] = len(self._queue)
                    except ValueError:
                        pass  # Already removed

                wait_time = time.time() - start_time
                raise TimeoutError(
                    f"Failed to acquire lock for {resource_id} after {wait_time:.2f}s"
                )
        else:
            event.wait()

        wait_time = time.time() - start_time
        if wait_time > 1.0:
            logger.warning(
                f"[FIFO Lock] Long wait: {resource_id} waited {wait_time:.2f}s, "
                f"queue_size={self._metrics['current_waiting']}"
            )

        try:
            yield
        finally:
            # Release lock and notify next
            with self._lock:
                try:
                    self._queue.popleft()
                    self._metrics["current_waiting"] = len(self._queue)

                    # Notify next in queue
                    if self._queue:
                        self._queue[0].set()
                except IndexError:
                    # Queue already empty
                    pass

    def get_metrics(self) -> Dict[str, Any]:
        """Get lock metrics for monitoring."""
        with self._lock:
            return self._metrics.copy()


class ShardedFIFOLock(LockStrategy):
    """
    Sharded FIFO lock for better concurrency.

    Different resource_ids are distributed across multiple locks,
    allowing parallel execution while maintaining FIFO within each shard.

    Example:
        lock = ShardedFIFOLock(num_shards=8)

        # Different messages can be processed in parallel
        with lock.acquire(resource_id="msg_1"):
            pass

        with lock.acquire(resource_id="msg_2"):
            pass
    """

    def __init__(self, num_shards: int = 8):
        self.num_shards = num_shards
        self.shards = [FIFOLock() for _ in range(num_shards)]

    def _get_shard(self, resource_id: str) -> FIFOLock:
        """Get shard for resource_id using hash."""
        shard_idx = hash(resource_id) % self.num_shards
        return self.shards[shard_idx]

    @contextmanager
    def acquire(self, **kwargs):
        """
        Acquire sharded FIFO lock.

        Args:
            resource_id (str): Resource identifier (REQUIRED for sharding)
            timeout (float, optional): Maximum wait time in seconds

        Raises:
            ValueError: If resource_id is not provided
            TimeoutError: If timeout is exceeded
        """
        resource_id = kwargs.get("resource_id")
        if not resource_id:
            raise ValueError("resource_id is required for ShardedFIFOLock")

        shard = self._get_shard(resource_id)
        with shard.acquire(**kwargs):
            yield

    def get_metrics(self) -> Dict[str, Any]:
        """Get aggregated metrics from all shards."""
        total_acquires = 0
        current_waiting = 0
        max_waiting = 0

        for shard in self.shards:
            metrics = shard.get_metrics()
            total_acquires += metrics["total_acquires"]
            current_waiting += metrics["current_waiting"]
            max_waiting = max(max_waiting, metrics["max_waiting"])

        return {
            "total_acquires": total_acquires,
            "current_waiting": current_waiting,
            "max_waiting": max_waiting,
            "num_shards": self.num_shards,
        }


class DatabaseLockManager:
    """
    Centralized lock manager for database operations.

    Provides different lock strategies for different types of operations:
    - Global lock: For cross-table transactions
    - Message locks: For message operations (sharded by message_id)
    - Agent locks: For agent operations (sharded by agent_id)
    - Task locks: For task operations (sharded by task_id)

    Example:
        lock_manager = DatabaseLockManager()

        # Message operation
        with lock_manager.acquire("message", resource_id=message_id):
            db.execute("UPDATE chat_messages ...")

        # Agent operation
        with lock_manager.acquire("agent", resource_id=agent_id):
            db.execute("UPDATE subagents ...")

        # Global transaction
        with lock_manager.acquire("global"):
            db.execute("UPDATE chat_messages ...")
            db.execute("UPDATE tq_sagas ...")
    """

    def __init__(self):
        # Global lock for cross-table operations
        self.global_lock = FIFOLock()

        # Sharded locks for specific resource types
        self.message_locks = ShardedFIFOLock(num_shards=8)
        self.agent_locks = ShardedFIFOLock(num_shards=4)
        self.task_locks = ShardedFIFOLock(num_shards=8)
        self.saga_locks = ShardedFIFOLock(num_shards=8)

        self._lock_map = {
            "global": self.global_lock,
            "message": self.message_locks,
            "agent": self.agent_locks,
            "task": self.task_locks,
            "saga": self.saga_locks,
        }

    @contextmanager
    def acquire(self, lock_type: str, **kwargs):
        """
        Acquire lock by type.

        Args:
            lock_type (str): Lock type ("global", "message", "agent", "task", "saga")
            **kwargs: Lock-specific parameters
                - resource_id (str): Resource identifier (required for sharded locks)
                - timeout (float): Maximum wait time in seconds

        Raises:
            ValueError: If lock_type is invalid
        """
        lock = self._lock_map.get(lock_type)
        if not lock:
            raise ValueError(
                f"Invalid lock_type: {lock_type}. "
                f"Valid types: {list(self._lock_map.keys())}"
            )

        with lock.acquire(**kwargs):
            yield

    def get_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics from all locks."""
        return {
            lock_type: lock.get_metrics()
            for lock_type, lock in self._lock_map.items()
        }

    def log_metrics(self):
        """Log current lock metrics."""
        metrics = self.get_metrics()
        for lock_type, lock_metrics in metrics.items():
            if lock_metrics["current_waiting"] > 0:
                logger.info(
                    f"[Lock Metrics] {lock_type}: "
                    f"waiting={lock_metrics['current_waiting']}, "
                    f"total={lock_metrics['total_acquires']}, "
                    f"max_waiting={lock_metrics['max_waiting']}"
                )
