import asyncio
from collections import defaultdict
from itertools import combinations
from math import ceil
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Generator

import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.community.quality import modularity
from .logger_util import get_logger

log = get_logger("red.course_channel_clustering")


class CourseChannelClustering:
    """
    Clusters course channels based on overlapping user memberships.

    This version optimizes performance by converting course and user IDs to integers.
    It supports dynamic thresholds (via median overlap adjusted by a threshold factor),
    handles sparse data using course metadata, and evaluates clustering quality via modularity.

    Clustering pipeline:
      1. Normalize IDs (convert to int).
      2. Compute pairwise user overlap between courses (with optional metadata augmentation).
      3. Build a weighted undirected graph.
      4. Optionally compute an adaptive grouping threshold.
      5. Apply a clustering algorithm (default: Louvain).
      6. Evaluate cluster quality.
      7. Map clusters to Discord category labels (splitting clusters into subgroups if needed).
    """

    def __init__(
        self,
        grouping_threshold: int = 2,
        max_category_channels: int = 50,
        category_prefix: str = "COURSES",
        clustering_func: Optional[Callable[[nx.Graph], List[Set[int]]]] = None,
        optimize_overlap: bool = True,
        adaptive_threshold: bool = False,
        threshold_factor: float = 1.0,
        sparse_overlap: int = 1,
    ) -> None:
        """
        :param grouping_threshold: Base minimum user overlap to connect two courses.
        :param max_category_channels: Maximum channels allowed per category.
        :param category_prefix: Base prefix for generated category names.
        :param clustering_func: Custom clustering function (defaults to Louvain if not provided).
        :param optimize_overlap: If True, use an inverted-index algorithm for overlap calculation.
        :param adaptive_threshold: If True, compute grouping threshold dynamically from overlap distribution.
        :param threshold_factor: Factor to adjust the computed median for adaptive threshold.
        :param sparse_overlap: Minimal overlap score to assign when metadata indicates a connection in sparse data.
        """
        if grouping_threshold < 1:
            raise ValueError("grouping_threshold must be at least 1.")
        if max_category_channels < 1:
            raise ValueError("max_category_channels must be at least 1.")

        self.grouping_threshold = grouping_threshold
        self.max_category_channels = max_category_channels
        self.category_prefix = category_prefix
        self.clustering_func = clustering_func or self._default_clustering
        self.optimize_overlap = optimize_overlap
        self.adaptive_threshold = adaptive_threshold
        self.threshold_factor = threshold_factor
        self.sparse_overlap = sparse_overlap

    def _normalize_course_users(
        self, course_users: Dict[Any, Set[Any]]
    ) -> Dict[int, Set[int]]:
        """
        Convert course IDs and user IDs to integers for performance optimization.
        Assumes that all IDs are convertible to int.
        """
        normalized = {}
        for course, users in course_users.items():
            try:
                course_id = int(course)
            except Exception as e:
                raise ValueError(
                    f"Course ID {course} is not convertible to int."
                ) from e
            normalized_users = set()
            for user in users:
                try:
                    user_id = int(user)
                except Exception as e:
                    raise ValueError(
                        f"User ID {user} is not convertible to int."
                    ) from e
                normalized_users.add(user_id)
            normalized[course_id] = normalized_users
        return normalized

    def _normalize_course_metadata(
        self, course_metadata: Dict[Any, Dict[str, Any]]
    ) -> Dict[int, Dict[str, Any]]:
        """
        Convert course metadata keys to integers.
        """
        normalized = {}
        for course, meta in course_metadata.items():
            try:
                course_id = int(course)
            except Exception as e:
                raise ValueError(
                    f"Course metadata key {course} is not convertible to int."
                ) from e
            normalized[course_id] = meta
        return normalized

    def _calculate_overlaps(
        self,
        course_users: Dict[int, Set[int]],
        course_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> Dict[Tuple[int, int], int]:
        """
        Calculate the number of common users between every pair of courses.
        Uses an inverted index approach if optimize_overlap is True.
        If course_metadata is provided, pairs with zero user overlap but matching departments
        are assigned a minimal overlap score.
        """
        overlaps: Dict[Tuple[int, int], int] = defaultdict(int)
        if self.optimize_overlap:
            user_to_courses: Dict[int, Set[int]] = defaultdict(set)
            for course, users in course_users.items():
                for user in users:
                    user_to_courses[user].add(course)
            for user, courses in user_to_courses.items():
                sorted_courses = sorted(courses)
                for course1, course2 in combinations(sorted_courses, 2):
                    overlaps[(course1, course2)] += 1
            method_used = "inverted index"
        else:
            course_ids = sorted(course_users.keys())
            for course1, course2 in combinations(course_ids, 2):
                count = len(course_users[course1] & course_users[course2])
                if count > 0:
                    overlaps[(course1, course2)] = count
            method_used = "combinations"

        # Handle sparse data by checking course metadata.
        if course_metadata is not None:
            course_ids = sorted(course_users.keys())
            for course1, course2 in combinations(course_ids, 2):
                if (course1, course2) not in overlaps:
                    meta1 = course_metadata.get(course1)
                    meta2 = course_metadata.get(course2)
                    if (
                        meta1
                        and meta2
                        and meta1.get("department")
                        and meta2.get("department")
                        and meta1.get("department") == meta2.get("department")
                    ):
                        overlaps[(course1, course2)] = self.sparse_overlap

        log.debug(
            f"Calculated overlaps using {method_used} for {len(course_users)} courses: {dict(overlaps)}"
        )
        return dict(overlaps)

    def _compute_dynamic_threshold(self, overlaps: Dict[Tuple[int, int], int]) -> int:
        """
        Compute a dynamic grouping threshold from the overlap distribution using the median,
        then adjust it with threshold_factor.
        """
        counts = sorted(overlaps.values())
        if not counts:
            return self.grouping_threshold
        n = len(counts)
        median = (
            counts[n // 2] if n % 2 == 1 else (counts[n // 2 - 1] + counts[n // 2]) / 2
        )
        effective_threshold = max(int(median * self.threshold_factor), 1)
        log.debug(
            f"Dynamic threshold computed: median={median}, threshold_factor={self.threshold_factor}, effective_threshold={effective_threshold}"
        )
        return effective_threshold

    def _build_graph(
        self,
        course_users: Dict[int, Set[int]],
        course_metadata: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> nx.Graph:
        """
        Build a weighted graph where nodes represent courses and
        edges represent the user overlap (if above the grouping threshold).
        Uses a dynamic threshold if adaptive_threshold is enabled.
        """
        graph = nx.Graph()
        for course, users in sorted(course_users.items()):
            graph.add_node(course)
            if not users:
                log.warning(f"Course '{course}' has no user engagements.")
        overlaps = self._calculate_overlaps(course_users, course_metadata)
        if self.adaptive_threshold and overlaps:
            threshold = self._compute_dynamic_threshold(overlaps)
        else:
            threshold = self.grouping_threshold

        for (course1, course2), weight in overlaps.items():
            if weight >= threshold:
                graph.add_edge(course1, course2, weight=weight)
        log.debug(
            f"Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges."
        )
        return graph

    def _default_clustering(self, graph: nx.Graph) -> List[Set[int]]:
        """
        Default clustering using the Louvain algorithm.
        Falls back to treating each course as its own cluster if necessary.
        """
        if graph.number_of_edges() == 0:
            clusters = [{node} for node in graph.nodes()]
            log.debug("Graph has no edges; each course is its own cluster.")
            return clusters
        try:
            clusters = louvain_communities(graph, weight="weight")
            log.debug(f"Louvain algorithm detected {len(clusters)} clusters.")
            return clusters
        except Exception as e:
            log.exception(f"Default clustering failed: {e}")
            return [set(graph.nodes())]

    def _perform_clustering(self, graph: nx.Graph) -> List[Set[int]]:
        """
        Perform clustering using the provided (or default) clustering function.
        """
        return self.clustering_func(graph)

    @staticmethod
    def _chunk_list(
        lst: List[Any], chunk_size: int
    ) -> Generator[List[Any], None, None]:
        """
        DRY: Helper function to split a list into chunks of a fixed size.
        """
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

    def _map_clusters_to_categories(self, clusters: List[Set[int]]) -> Dict[int, str]:
        """
        Map clusters (and their subgroups) to unique Discord category labels.
        Each subgroup (of size up to max_category_channels) receives a unique label.
        """
        mapping: Dict[int, str] = {}
        total_subgroups = sum(
            ceil(len(cluster) / self.max_category_channels) for cluster in clusters
        )
        use_suffix = total_subgroups > 1

        subgroup_counter = 1
        for cluster in sorted(clusters, key=lambda c: min(c) if c else 0):
            courses = sorted(cluster)
            chunks = list(self._chunk_list(courses, self.max_category_channels))
            log.debug(
                f"Mapping cluster with {len(courses)} courses into {len(chunks)} subgroup(s)."
            )
            for chunk in chunks:
                category_label = (
                    f"{self.category_prefix}-{subgroup_counter}"
                    if use_suffix
                    else self.category_prefix
                )
                for course in chunk:
                    mapping[course] = category_label
                log.debug(f"Assigned courses {chunk} to category '{category_label}'.")
                subgroup_counter += 1
        return mapping

    def evaluate_clusters(
        self, graph: nx.Graph, clusters: List[Set[int]]
    ) -> Dict[str, float]:
        """
        Evaluate the quality of the clusters using modularity.
        Returns a dictionary with quality metrics.
        """
        mod = modularity(graph, clusters, weight="weight")
        return {"modularity": mod}

    def cluster_courses(
        self,
        course_users: Dict[Any, Set[Any]],
        course_metadata: Optional[Dict[Any, Dict[str, Any]]] = None,
    ) -> Dict[int, str]:
        """
        Run the full clustering pipeline to map course IDs (converted to int)
        to Discord category labels. Optionally incorporates course metadata.
        Logs clustering quality metrics.
        """
        normalized_course_users = self._normalize_course_users(course_users)
        normalized_course_metadata = (
            self._normalize_course_metadata(course_metadata)
            if course_metadata is not None
            else None
        )
        graph = self._build_graph(normalized_course_users, normalized_course_metadata)
        clusters = self._perform_clustering(graph)
        metrics = self.evaluate_clusters(graph, clusters)
        log.info(f"Cluster quality metrics: {metrics}")
        mapping = self._map_clusters_to_categories(clusters)
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
        """
        Periodically run the clustering process until a shutdown signal is received.
        """
        log.info("Starting periodic course clustering task.")
        iteration = 1
        while not shutdown_event.is_set():
            log.info(f"Starting clustering cycle iteration {iteration}")
            try:
                course_users = get_course_users()
                if course_users:
                    mapping = self.cluster_courses(course_users, course_metadata)
                else:
                    log.warning("No course user data available; no mapping produced.")
                    mapping = {}
                persist_mapping(mapping)
                log.info("Clustering cycle complete; mapping persisted.")
            except Exception as e:
                log.exception(f"Error during clustering cycle: {e}")
            iteration += 1
            try:
                await asyncio.wait_for(shutdown_event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
        log.info("Clustering task received shutdown signal; terminating gracefully.")
