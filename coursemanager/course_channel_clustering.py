import asyncio
from collections import defaultdict
from itertools import combinations
from math import ceil
from statistics import median
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple

import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.community.quality import modularity

from .logger_util import get_logger

log = get_logger("red.course_channel_clustering")


class CourseChannelClustering:
    def __init__(
        self,
        grouping_threshold: int = 2,
        max_category_channels: int = 5,
        category_prefix: str = "COURSES",
        clustering_func: Optional[Callable[[nx.Graph], List[Set[int]]]] = None,
        optimize_overlap: bool = True,
        adaptive_threshold: bool = False,
        threshold_factor: float = 1.0,
        sparse_overlap: int = 1,
    ) -> None:
        if grouping_threshold < 1:
            raise ValueError("grouping_threshold must be at least 1.")
        if max_category_channels < 1:
            raise ValueError("max_category_channels must be at least 1.")
        self.grouping_threshold: int = grouping_threshold
        self.max_category_channels: int = max_category_channels
        self.category_prefix: str = category_prefix
        self.clustering_func: Callable[[nx.Graph], List[Set[int]]] = (
            clustering_func or self._default_clustering
        )
        self.optimize_overlap: bool = optimize_overlap
        self.adaptive_threshold: bool = adaptive_threshold
        self.threshold_factor: float = threshold_factor
        self.sparse_overlap: int = sparse_overlap

    @staticmethod
    def _normalize_key(key: Any) -> int:
        try:
            return int(key)
        except Exception as exc:
            raise ValueError(f"Key {key} is not convertible to int.") from exc

    def _normalize_course_users(
        self, course_users: Dict[Any, Set[Any]]
    ) -> Dict[int, Set[int]]:
        return {
            self._normalize_key(course): {self._normalize_key(user) for user in users}
            for course, users in course_users.items()
        }

    def _normalize_course_metadata(
        self, course_metadata: Dict[Any, Dict[str, Any]]
    ) -> Dict[int, Dict[str, Any]]:
        return {
            self._normalize_key(course): meta
            for course, meta in course_metadata.items()
        }

    def _add_sparse_overlaps(
        self,
        overlaps: Dict[Tuple[int, int], int],
        courses_sorted: List[int],
        course_metadata: Dict[int, Dict[str, Any]],
    ) -> None:
        for course1, course2 in combinations(courses_sorted, 2):
            if (course1, course2) not in overlaps:
                meta1: Optional[str] = course_metadata.get(course1, {}).get(
                    "department"
                )
                meta2: Optional[str] = course_metadata.get(course2, {}).get(
                    "department"
                )
                if meta1 and meta2 and meta1 == meta2:
                    overlaps[(course1, course2)] = self.sparse_overlap

    def _calculate_overlaps(
        self,
        course_users: Dict[int, Set[int]],
        course_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> Dict[Tuple[int, int], int]:
        overlaps: Dict[Tuple[int, int], int] = defaultdict(int)
        courses_sorted: List[int] = sorted(course_users.keys())
        if self.optimize_overlap:
            user_to_courses: Dict[int, Set[int]] = defaultdict(set)
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
            f"Calculated overlaps using {method_used} for {len(course_users)} courses: {dict(overlaps)}"
        )
        return dict(overlaps)

    def _compute_dynamic_threshold(self, overlaps: Dict[Tuple[int, int], int]) -> int:
        counts = sorted(overlaps.values())
        if not counts:
            return self.grouping_threshold
        med = median(counts)
        effective_threshold: int = max(int(med * self.threshold_factor), 1)
        log.debug(
            f"Dynamic threshold computed: median={med}, threshold_factor={self.threshold_factor}, "
            f"effective_threshold={effective_threshold}"
        )
        return effective_threshold

    def _build_graph(
        self,
        course_users: Dict[int, Set[int]],
        course_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
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
            f"Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges."
        )
        return graph

    def _default_clustering(self, graph: nx.Graph) -> List[Set[int]]:
        if graph.number_of_edges() == 0:
            clusters: List[Set[int]] = [{node} for node in graph.nodes()]
            log.debug("Graph has no edges; each course is its own cluster.")
            return clusters
        try:
            clusters = louvain_communities(graph, weight="weight")
            log.debug(f"Louvain algorithm detected {len(clusters)} clusters.")
            return clusters
        except Exception as exc:
            log.exception(f"Default clustering failed: {exc}")
            return [set(graph.nodes())]

    def _perform_clustering(self, graph: nx.Graph) -> List[Set[int]]:
        try:
            clusters = self.clustering_func(graph)
            log.debug(f"Clustering performed, obtained {len(clusters)} clusters.")
            return clusters
        except Exception as exc:
            log.exception(f"Error during clustering: {exc}")
            return [set(graph.nodes())]

    @staticmethod
    def _chunk_list(
        lst: List[Any], chunk_size: int
    ) -> Generator[List[Any], None, None]:
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

    def _map_clusters_to_categories(self, clusters: List[Set[int]]) -> Dict[int, str]:
        mapping: Dict[int, str] = {}
        total_subgroups: int = sum(
            (ceil(len(cluster) / self.max_category_channels) for cluster in clusters)
        )
        use_suffix: bool = total_subgroups > 1
        subgroup_counter: int = 1
        for cluster in sorted(clusters, key=lambda c: min(c) if c else 0):
            courses: List[int] = sorted(cluster)
            chunks = list(self._chunk_list(courses, self.max_category_channels))
            log.debug(
                f"Mapping cluster with {len(courses)} courses into {len(chunks)} subgroup(s)."
            )
            for chunk in chunks:
                category_label: str = (
                    f"{self.category_prefix}-{subgroup_counter}"
                    if use_suffix
                    else self.category_prefix
                )
                mapping |= {course: category_label for course in chunk}
                log.debug(f"Assigned courses {chunk} to category '{category_label}'.")
                subgroup_counter += 1
        return mapping

    def evaluate_clusters(
        self, graph: nx.Graph, clusters: List[Set[int]]
    ) -> Dict[str, float]:
        mod: float = modularity(graph, clusters, weight="weight")
        return {"modularity": mod}

    def cluster_courses(
        self,
        course_users: Dict[Any, Set[Any]],
        course_metadata: Optional[Dict[Any, Dict[str, Any]]] = None,
    ) -> Dict[int, str]:
        normalized_course_users: Dict[int, Set[int]] = self._normalize_course_users(
            course_users
        )
        normalized_course_metadata: Optional[Dict[int, Dict[str, Any]]] = (
            self._normalize_course_metadata(course_metadata)
            if course_metadata is not None
            else None
        )
        graph: nx.Graph = self._build_graph(
            normalized_course_users, normalized_course_metadata
        )
        clusters: List[Set[int]] = self._perform_clustering(graph)
        metrics: Dict[str, float] = self.evaluate_clusters(graph, clusters)
        log.info(f"Cluster quality metrics: {metrics}")
        mapping: Dict[int, str] = self._map_clusters_to_categories(clusters)
        log.info(f"Final course-to-category mapping: {mapping}")
        return mapping

    async def run_periodic(
        self,
        interval: int,
        get_course_users: Callable[[], Dict[Any, Set[Any]]],
        persist_mapping: Callable[[Dict[int, str]], Any],
        shutdown_event: asyncio.Event,
        course_metadata: Optional[Dict[Any, Dict[str, Any]]] = None,
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
                    f"Error during clustering cycle iteration {iteration}: {exc}"
                )
            iteration += 1
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
        log.info("Clustering task received shutdown signal; terminating gracefully.")
