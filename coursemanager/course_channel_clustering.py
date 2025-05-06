from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from statistics import median
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple

import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.community.quality import modularity

from .constants import MAX_CATEGORY_CHANNELS, MIN_CATEGORY_CHANNELS
from .logger_util import get_logger

log = get_logger("red.course_channel_clustering")


class CourseChannelClustering:
    def __init__(
        self,
        grouping_threshold: int = 2,
        category_prefix: str = "COURSES",
        clustering_func: Optional[Callable[[nx.Graph], List[Set[str]]]] = None,
        optimize_overlap: bool = True,
        adaptive_threshold: bool = False,
        threshold_factor: float = 1.0,
        sparse_overlap: int = 1,
    ) -> None:
        if grouping_threshold < 1:
            raise ValueError("grouping_threshold must be at least 1.")
        self.grouping_threshold: int = grouping_threshold
        self.category_prefix: str = category_prefix
        self.clustering_func: Callable[[nx.Graph], List[Set[str]]] = (
            clustering_func or self._default_clustering
        )
        self.optimize_overlap: bool = optimize_overlap
        self.adaptive_threshold: bool = adaptive_threshold
        self.threshold_factor: float = threshold_factor
        self.sparse_overlap: int = sparse_overlap

    def _add_sparse_overlaps(
        self,
        overlaps: Dict[Tuple[str, str], int],
        courses_sorted: List[str],
        course_metadata: Dict[str, Dict[str, Any]],
    ) -> None:
        for course1, course2 in combinations(courses_sorted, 2):
            if (course1, course2) not in overlaps:
                meta1: Optional[str] = course_metadata.get(course1, {}).get(
                    "department",
                )
                meta2: Optional[str] = course_metadata.get(course2, {}).get(
                    "department",
                )
                if meta1 and meta2 and meta1 == meta2:
                    overlaps[(course1, course2)] = self.sparse_overlap

    def _calculate_overlaps(
        self,
        course_users: Dict[str, Set[int]],
        course_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[Tuple[str, str], int]:
        overlaps: Dict[Tuple[str, str], int] = defaultdict(int)
        courses_sorted: List[str] = sorted(course_users.keys())
        if self.optimize_overlap:
            user_to_courses: Dict[int, Set[str]] = defaultdict(set)
            for course, users in course_users.items():
                for user in users:
                    user_to_courses[user].add(course)
            for courses in user_to_courses.values():
                for course1, course2 in combinations(sorted(courses), 2):
                    overlaps[(course1, course2)] += 1
            method_used: str = "inverted index"
        else:
            for course1, course2 in combinations(courses_sorted, 2):
                count: int = len(course_users[course1] & course_users[course2])
                if count > 0:
                    overlaps[(course1, course2)] = count
            method_used = "combinations"
        if course_metadata is not None:
            self._add_sparse_overlaps(overlaps, courses_sorted, course_metadata)
        log.debug(
            f"Calculated overlaps using {method_used} for {len(course_users)} courses: {dict(overlaps)}",
        )
        return dict(overlaps)

    def _compute_dynamic_threshold(self, overlaps: Dict[Tuple[str, str], int]) -> int:
        counts = sorted(overlaps.values())
        if not counts:
            return self.grouping_threshold
        med = median(counts)
        effective_threshold: int = max(int(med * self.threshold_factor), 1)
        log.debug(
            f"Dynamic threshold computed: median={med}, threshold_factor={self.threshold_factor}, "
            f"effective_threshold={effective_threshold}",
        )
        return effective_threshold

    def _build_graph(
        self,
        course_users: Dict[str, Set[int]],
        course_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> nx.Graph:
        graph: nx.Graph = nx.Graph()
        graph.add_nodes_from(sorted(course_users.keys()))
        for course, users in sorted(course_users.items()):
            if not users:
                log.warning(f"Course '{course}' has no user engagements.")
        overlaps = self._calculate_overlaps(course_users, course_metadata)
        threshold: int = (
            self._compute_dynamic_threshold(overlaps)
            if self.adaptive_threshold and overlaps
            else self.grouping_threshold
        )
        for (course1, course2), weight in overlaps.items():
            if weight >= threshold:
                graph.add_edge(course1, course2, weight=weight)
        log.debug(
            f"Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges.",
        )
        return graph

    def _default_clustering(self, graph: nx.Graph) -> List[Set[str]]:
        if graph.number_of_edges() == 0:
            clusters: List[Set[str]] = [{node} for node in graph.nodes()]
            log.debug("Graph has no edges; each course is its own cluster.")
            return clusters
        try:
            clusters = louvain_communities(graph, weight="weight")
            log.debug(f"Louvain algorithm detected {len(clusters)} clusters.")
            return clusters
        except Exception as exc:
            log.exception(f"Default clustering failed: {exc}")
            return [set(graph.nodes())]  # type: ignore[arg-type]

    def _perform_clustering(self, graph: nx.Graph) -> List[Set[str]]:
        try:
            clusters = self.clustering_func(graph)
            log.debug(f"Clustering performed, obtained {len(clusters)} clusters.")
            return clusters
        except Exception as exc:
            log.exception(f"Error during clustering: {exc}")
            return [set(graph.nodes())]  # type: ignore[arg-type]

    @staticmethod
    def _chunk_list(
        lst: List[Any],
        chunk_size: int,
    ) -> Generator[List[Any], None, None]:
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

    def _map_clusters_to_categories(self, clusters: List[Set[str]]) -> Dict[str, str]:
        """1. Split clusters into ≤ MAX_CATEGORY_CHANNELS buckets.
        2. Merge buckets that land < MIN_CATEGORY_CHANNELS where possible.
        3. Sort buckets by size DESC, label sequentially with zero‑padded suffix.
        """
        # ── 1 .  Split large clusters ────────────────────────────────────────
        buckets: list[list[str]] = []
        for cluster in clusters:
            sorted_cluster = sorted(cluster)
            for i in range(0, len(sorted_cluster), MAX_CATEGORY_CHANNELS):
                buckets.append(sorted_cluster[i : i + MAX_CATEGORY_CHANNELS])

        # ── 2 .  Merge under‑filled buckets (greedy, largest‑fit) ────────────
        buckets.sort(key=len, reverse=True)  # work big → small
        i = len(buckets) - 1  # start from tail
        while i >= 0 and len(buckets) > 1:
            if len(buckets[i]) >= MIN_CATEGORY_CHANNELS:
                i -= 1
                continue

            # find biggest bucket that can absorb it
            for j in range(len(buckets)):
                if i == j:
                    continue
                if len(buckets[j]) + len(buckets[i]) <= MAX_CATEGORY_CHANNELS:
                    buckets[j].extend(buckets.pop(i))
                    break
            i -= 1

        # ── 3 .  Rename buckets & build mapping ──────────────────────────────
        buckets.sort(key=len, reverse=True)  # final order
        pad = max(2, len(str(len(buckets))))  # 01, 02 … or wider
        mapping: Dict[str, str] = {}
        for idx, bucket in enumerate(buckets, start=1):
            label = f"{self.category_prefix}-{idx:0{pad}d}"
            mapping |= dict.fromkeys(bucket, label)
            log.debug("Bucket → %s : %d channels", label, len(bucket))
        return mapping

    def evaluate_clusters(
        self,
        graph: nx.Graph,
        clusters: List[Set[str]],
    ) -> Dict[str, float]:
        """Compute and return modularity (just for logging/metrics)."""
        return {"modularity": modularity(graph, clusters, weight="weight")}

    def cluster_courses(
        self,
        course_users: Dict[str, Set[int]],
        course_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Dict[str, str]:
        """Build the overlap graph, run your clustering function,
        optionally log metrics, then map clusters → category names.
        """
        graph = self._build_graph(course_users, course_metadata)
        clusters = self._perform_clustering(graph)
        metrics = self.evaluate_clusters(graph, clusters)
        log.info(f"Cluster quality metrics: {metrics}")
        mapping = self._map_clusters_to_categories(clusters)
        log.info(f"Final course-to-category mapping: {mapping}")
        return mapping

    async def run_periodic(
        self,
        interval: int,
        get_course_users: Callable[[], Dict[str, Set[int]]],
        persist_mapping: Callable[[Dict[str, str]], Any],
        shutdown_event: asyncio.Event,
        course_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        log.info("Starting periodic course clustering task.")
        iteration: int = 1
        while not shutdown_event.is_set():
            log.info(f"Starting clustering cycle iteration {iteration}")
            try:
                if course_users_data := get_course_users():
                    mapping = self.cluster_courses(course_users_data, course_metadata)
                else:
                    log.warning("No course user data available; no mapping produced.")
                    mapping = {}
                persist_mapping(mapping)
                log.info("Clustering cycle complete; mapping persisted.")
            except Exception as exc:
                log.exception(
                    f"Error during clustering cycle iteration {iteration}: {exc}",
                )
            iteration += 1
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
        log.info("Clustering task received shutdown signal; terminating gracefully.")
