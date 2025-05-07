from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from itertools import combinations
from statistics import median
from typing import Any, Callable, Coroutine, Generator, Iterable, Mapping

import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.community.quality import modularity
from networkx.exception import NetworkXError

from .constants import MAX_CATEGORY_CHANNELS, MIN_CATEGORY_CHANNELS
from .logger_util import get_logger

log = get_logger(__name__)

# --------------------------------------------------------------------------- #
# Type aliases - keep public surface identical to earlier versions
# --------------------------------------------------------------------------- #
CourseUsers = dict[str, set[int]]
CourseMetadata = dict[str, dict[str, Any]]
OverlapKey = tuple[str, str]
Cluster = set[str]
ClusterList = list[Cluster]
CategoryMapping = dict[str, str]

# --------------------------------------------------------------------------- #
# Tunables / magic-number constants
# --------------------------------------------------------------------------- #
MIN_DYNAMIC_THRESHOLD = 1
MIN_SPARSE_OVERLAP = 1


class CourseChannelClustering:
    """Compute *course → Discord-category* mappings."""

    # --------------------------------------------------------------------- #
    # Construction
    # --------------------------------------------------------------------- #
    def __init__(
        self,
        *,
        grouping_threshold: int = 2,
        category_prefix: str = "COURSES",
        clustering_func: Callable[[nx.Graph], ClusterList] | None = None,
        optimize_overlap: bool = True,
        adaptive_threshold: bool = False,
        threshold_factor: float = 1.0,
        sparse_overlap: int = MIN_SPARSE_OVERLAP,
    ) -> None:
        if grouping_threshold < MIN_DYNAMIC_THRESHOLD:
            msg = "grouping_threshold must be at least 1"
            raise ValueError(msg)
        self.grouping_threshold: int = grouping_threshold
        self.category_prefix: str = category_prefix
        self.clustering_func: Callable[[nx.Graph], ClusterList] = (
            clustering_func or self._default_clustering
        )
        self.optimize_overlap: bool = optimize_overlap
        self.adaptive_threshold: bool = adaptive_threshold
        self.threshold_factor: float = threshold_factor
        self.sparse_overlap: int = sparse_overlap

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #
    @staticmethod
    def _chunk_list(items: list[Any], size: int) -> Generator[list[Any], None, None]:
        """Yield *size*‑length chunks from *items* (preserves order)."""
        for i in range(0, len(items), size):
            yield items[i : i + size]

    @staticmethod
    def _warn_empty_courses(course_users: Mapping[str, Iterable[int]]) -> None:
        """Emit a warning for courses without user engagement."""
        for course, users in course_users.items():
            if not users:
                log.warning("Course %s has no user engagements", course)

    def _add_sparse_overlaps(
        self,
        overlaps: defaultdict[OverlapKey, int],
        sorted_courses: list[str],
        metadata: CourseMetadata,
    ) -> None:
        """Inject minimal overlap when two courses share the same department."""
        for c1, c2 in combinations(sorted_courses, 2):
            if (c1, c2) in overlaps:
                continue
            dept_a = metadata.get(c1, {}).get("department")
            dept_b = metadata.get(c2, {}).get("department")
            if dept_a and dept_b and dept_a == dept_b:
                overlaps[(c1, c2)] = self.sparse_overlap

    def _calculate_overlaps(
        self,
        course_users: CourseUsers,
        course_metadata: CourseMetadata | None = None,
    ) -> dict[OverlapKey, int]:
        """Return pairwise user‑overlap counts between courses."""
        overlaps: defaultdict[OverlapKey, int] = defaultdict(int)

        if self.optimize_overlap:
            # Build inverted index: user → courses
            user_to_courses: defaultdict[int, set[str]] = defaultdict(set)
            for course, users in course_users.items():
                for uid in users:
                    user_to_courses[uid].add(course)
            for courses in user_to_courses.values():
                for a, b in combinations(sorted(courses), 2):
                    overlaps[(a, b)] += 1
            method = "inverted‑index"
        else:
            for a, b in combinations(sorted(course_users), 2):
                if cnt := len(course_users[a] & course_users[b]):
                    overlaps[(a, b)] = cnt
            method = "direct‑combinations"

        if course_metadata:
            self._add_sparse_overlaps(overlaps, sorted(course_users), course_metadata)

        if log.isEnabledFor(logging.DEBUG):
            log.debug(
                "Calculated overlaps using %s for %d courses",
                method,
                len(course_users),
            )
        return dict(overlaps)

    def _compute_dynamic_threshold(self, overlaps: Mapping[OverlapKey, int]) -> int:
        """Return adaptive threshold = max(median · factor, MIN)."""
        if not overlaps:
            return self.grouping_threshold
        med = median(overlaps.values())
        threshold = max(int(med * self.threshold_factor), MIN_DYNAMIC_THRESHOLD)
        log.debug("Dynamic threshold → %s (median %s)", threshold, med)
        return threshold

    def _build_graph(
        self,
        course_users: CourseUsers,
        course_metadata: CourseMetadata | None = None,
    ) -> nx.Graph:
        """Construct weighted graph where edge weight = user overlap."""
        graph: nx.Graph = nx.Graph()
        graph.add_nodes_from(course_users)

        self._warn_empty_courses(course_users)

        overlaps = self._calculate_overlaps(course_users, course_metadata)
        threshold = (
            self._compute_dynamic_threshold(overlaps)
            if self.adaptive_threshold
            else self.grouping_threshold
        )

        for (a, b), weight in overlaps.items():
            if weight >= threshold:
                graph.add_edge(a, b, weight=weight)

        log.debug(
            "Graph built with %d nodes & %d edges",
            graph.number_of_nodes(),
            graph.number_of_edges(),
        )
        return graph

    # --------------------------------------------------------------------- #
    # Clustering
    # --------------------------------------------------------------------- #
    @staticmethod
    def _default_clustering(graph: nx.Graph) -> ClusterList:
        """Louvain clustering, with singleton fallback."""
        if graph.number_of_edges() == 0:  # fully disconnected
            return [{node} for node in graph.nodes()]
        try:
            return louvain_communities(graph, weight="weight")
        except NetworkXError:
            log.exception("Louvain clustering failed; using single cluster")
            return [set(graph.nodes())]

    def _perform_clustering(self, graph: nx.Graph) -> ClusterList:
        """Invoke custom clustering with guarded error handling."""
        try:
            clusters = self.clustering_func(graph)
        except NetworkXError:
            log.exception("Custom clustering failed; using single cluster")
            return [set(graph.nodes())]
        else:
            log.debug("Clustering produced %d clusters", len(clusters))
            return clusters

    # --------------------------------------------------------------------- #
    # Mapping utilities
    # --------------------------------------------------------------------- #
    def _prelim_buckets(self, clusters: ClusterList) -> list[list[str]]:
        """Split each cluster into chunks of size ≤ MAX_CATEGORY_CHANNELS."""
        return [
            chunk
            for cluster in clusters
            for chunk in self._chunk_list(sorted(cluster), MAX_CATEGORY_CHANNELS)
        ]

    def _partition_buckets(
        self,
        prelim: list[list[str]],
    ) -> tuple[list[list[str]], list[str]]:
        """Return (large_buckets, orphans) based on MIN_CATEGORY_CHANNELS."""
        large = [b for b in prelim if len(b) >= MIN_CATEGORY_CHANNELS]
        orphans = [c for b in prelim if len(b) < MIN_CATEGORY_CHANNELS for c in b]
        return large, orphans

    def _merge_orphans(self, large: list[list[str]], orphans: list[str]) -> None:
        """Pack orphans into existing large buckets up to capacity."""
        for course in list(orphans):
            for bucket in large:
                if len(bucket) < MAX_CATEGORY_CHANNELS:
                    bucket.append(course)
                    orphans.remove(course)
                    break

    def _label_buckets(
        self,
        buckets: list[list[str]],
        *,
        orphans_exist: bool,
    ) -> CategoryMapping:
        """Assign names to buckets; use –MISC for overflow bucket if needed."""
        mapping: CategoryMapping = {}
        total = len(buckets)
        width = len(str(total))
        for idx, bucket in enumerate(
            sorted(buckets, key=lambda b: (-len(b), b)),
            start=1,
        ):
            if idx == total and orphans_exist:
                label = f"{self.category_prefix}-MISC"
            else:
                label = (
                    f"{self.category_prefix}-{idx:0{width}}"
                    if total > 1
                    else self.category_prefix
                )
            for course in bucket:
                mapping[course] = label
            log.debug("Bucket → %s : %d channels", label, len(bucket))
        return mapping

    def _map_clusters_to_categories(self, clusters: ClusterList) -> CategoryMapping:
        """Orchestrate bucket creation, merging, and labeling."""
        prelim = self._prelim_buckets(clusters)
        large, orphans = self._partition_buckets(prelim)
        self._merge_orphans(large, orphans)
        if orphans:
            large.append(sorted(orphans))
            log.debug("Added overflow bucket with %d singleton courses", len(orphans))
        return self._label_buckets(large, orphans_exist=bool(orphans))

    # --------------------------------------------------------------------- #
    # Public surface
    # --------------------------------------------------------------------- #
    def evaluate_clusters(
        self,
        graph: nx.Graph,
        clusters: ClusterList,
    ) -> dict[str, float]:
        """Compute clustering‑quality metrics (currently just modularity)."""
        return {"modularity": modularity(graph, clusters, weight="weight")}

    def cluster_courses(
        self,
        course_users: CourseUsers,
        course_metadata: CourseMetadata | None = None,
    ) -> CategoryMapping:
        """End‑to‑end pipeline: graph → clusters → mapping.

        Preserves original signature to avoid breaking callers.
        """
        graph = self._build_graph(course_users, course_metadata)
        clusters = self._perform_clustering(graph)
        log.info("Cluster quality: %s", self.evaluate_clusters(graph, clusters))
        mapping = self._map_clusters_to_categories(clusters)
        log.info("Final mapping: %s", mapping)
        return mapping

    async def run_periodic(
        self,
        interval: int,
        fetch_course_users: Callable[
            [],
            CourseUsers | Coroutine[Any, Any, CourseUsers],
        ],
        persist_mapping: Callable[
            [CategoryMapping],
            Coroutine[Any, Any, Any] | Any,
        ],
        shutdown_event: asyncio.Event,
        course_metadata: CourseMetadata | None = None,
    ) -> None:
        """Background task: refresh mapping every *interval* seconds."""
        log.info("Periodic clustering task started")
        iteration = 1

        while not shutdown_event.is_set():
            log.info("Clustering cycle #%d", iteration)

            # --------------------------- fetch --------------------------- #
            try:
                raw = fetch_course_users()
                users = await raw if asyncio.iscoroutine(raw) else raw
            except Exception:
                log.exception("Fetching users failed; skipping cycle")
                users = {}

            # ------------------------ compute / persist ------------------ #
            if users:
                mapping = self.cluster_courses(users, course_metadata)
                try:
                    persisted = persist_mapping(mapping)
                    if asyncio.iscoroutine(persisted):
                        await persisted
                    log.info("Mapping persisted")
                except Exception:
                    log.exception("Persisting mapping failed")
            else:
                log.info("No user data; nothing persisted")

            iteration += 1

            # --------------------------- sleep --------------------------- #
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

        log.info("Periodic clustering task shutdown complete")
