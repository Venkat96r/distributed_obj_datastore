import time
import threading
from collections import OrderedDict
from typing import Any, Dict, Optional, Tuple


class LRUCache:
    """
    Thread-safe bounded LRU cache supporting entry limits, byte budgets,
    optional TTL, and stale-fill protection via per-key & global version tokens.
    """

    def __init__(
        self,
        max_entries: Optional[int] = 1000,
        max_bytes: Optional[int] = None,
        ttl_seconds: Optional[float] = None,
    ):
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.ttl_seconds = ttl_seconds

        self._lock = threading.RLock()
        # key -> (value, size_bytes, expires_at)
        self._store: OrderedDict[str, Tuple[Any, int, Optional[float]]] = OrderedDict()
        self._bytes_used: int = 0

        # Stale-fill protection tokens
        self._global_gen: int = 0
        self._key_versions: Dict[str, int] = {}

        # Metrics
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0
        self.invalidations: int = 0
        self.stale_fills_rejected: int = 0

    def begin_load(self, key: str) -> Tuple[int, int]:
        """
        Capture a version token before reading from the authoritative source.
        Pass this token to `put_if_fresh` so concurrent invalidations are never overwritten.
        """
        with self._lock:
            return (self._global_gen, self._key_versions.get(key, 0))

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self.misses += 1
                return None

            value, size_bytes, expires_at = entry
            if expires_at is not None and time.monotonic() >= expires_at:
                self._remove_locked(key)
                self.misses += 1
                return None

            self._store.move_to_end(key)
            self.hits += 1
            return value

    def put(self, key: str, value: Any, size_bytes: int = 0) -> bool:
        """
        Unconditional write-through insert (bumps key version so older loads cannot overwrite it).
        """
        with self._lock:
            self._key_versions[key] = self._key_versions.get(key, 0) + 1
            return self._insert_locked(key, value, size_bytes)

    def put_if_fresh(
        self,
        key: str,
        value: Any,
        token: Tuple[int, int],
        size_bytes: int = 0,
    ) -> bool:
        """
        Insert into cache only if neither the global generation nor this key's version
        changed since `begin_load(key)`.
        """
        with self._lock:
            gen, ver = token
            if gen != self._global_gen or self._key_versions.get(key, 0) != ver:
                self.stale_fills_rejected += 1
                return False
            return self._insert_locked(key, value, size_bytes)

    def invalidate(self, key: str) -> None:
        with self._lock:
            self._key_versions[key] = self._key_versions.get(key, 0) + 1
            if key in self._store:
                self._remove_locked(key)
                self.invalidations += 1

    def invalidate_all(self) -> None:
        with self._lock:
            self._global_gen += 1
            count = len(self._store)
            self._store.clear()
            self._key_versions.clear()
            self._bytes_used = 0
            self.invalidations += count

    def _insert_locked(self, key: str, value: Any, size_bytes: int) -> bool:
        if self.max_bytes is not None and size_bytes > self.max_bytes:
            # Single item exceeds total cache budget; do not store
            if key in self._store:
                self._remove_locked(key)
            return False

        if key in self._store:
            self._remove_locked(key)

        expires_at = (
            time.monotonic() + self.ttl_seconds if self.ttl_seconds is not None else None
        )
        self._store[key] = (value, size_bytes, expires_at)
        self._bytes_used += size_bytes

        # Enforce bounds
        while self._over_budget_locked():
            oldest_key, (_, old_size, _) = self._store.popitem(last=False)
            self._bytes_used -= old_size
            self.evictions += 1

        return True

    def _over_budget_locked(self) -> bool:
        if self.max_entries is not None and len(self._store) > self.max_entries:
            return True
        if self.max_bytes is not None and self._bytes_used > self.max_bytes:
            return True
        return False

    def _remove_locked(self, key: str) -> None:
        entry = self._store.pop(key, None)
        if entry is not None:
            self._bytes_used -= entry[1]

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self.hits + self.misses
            hit_rate = round((self.hits / total) * 100.0, 2) if total > 0 else 0.0
            return {
                "entries": len(self._store),
                "max_entries": self.max_entries,
                "bytes_used": self._bytes_used,
                "max_bytes": self.max_bytes,
                "hits": self.hits,
                "misses": self.misses,
                "hit_rate_pct": hit_rate,
                "evictions": self.evictions,
                "invalidations": self.invalidations,
                "stale_fills_rejected": self.stale_fills_rejected,
            }
