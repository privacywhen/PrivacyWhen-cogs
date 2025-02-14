"""
Module for clustering course channels based on user overlap and metadata.
"""

import asyncio
from collections import defaultdict
from itertools import combinations
from math import ceil
from typing import Any, Callable, Dict, Generator, List, Optional, Set, Tuple

import networkx as nx
from networkx.algorithms.community import louvain_communities
from networkx.algorithms.community.quality import modularity

from .logger_util import get_logger

log = get_logger("red.course_channel_clustering")


class CourseChannelClustering:
    """
    Class for clustering course channels based on user interactions and metadata.

    Attributes:
        grouping_threshold (int): Minimum overlap required to group courses.
        max_category_channels (int): Maximum number of courses per category.
        category_prefix (str): Prefix for generated categories.
        clustering_func (Callable[[nx.Graph], List[Set[int]]]): Function to perform clustering.
        optimize_overlap (bool): Flag to use optimized overlap calculation.
        adaptive_threshold (bool): Whether to compute dynamic grouping threshold.
        threshold_factor (float): Factor for dynamic threshold computation.
        sparse_overlap (int): Overlap value for courses with similar departments.
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
        Initialize the CourseChannelClustering.

        Args:
            grouping_threshold (int): Minimum overlap required to group courses.
            max_category_channels (int): Maximum number of courses per category.
            category_prefix (str): Prefix for generated categories.
            clustering_func (Optional[Callable[[nx.Graph], List[Set[int]]]]): Custom clustering function.
            optimize_overlap (bool): Flag to use optimized overlap calculation.
            adaptive_threshold (bool): Whether to compute dynamic grouping threshold.
            threshold_factor (float): Factor for dynamic threshold computation.
            sparse_overlap (int): Overlap value for courses with similar departments.
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
        Normalize course user data by converting course and user identifiers to integers.

        Args:
            course_users (Dict[Any, Set[Any]]): Raw course user mapping.

        Returns:
            Dict[int, Set[int]]: Normalized course user mapping.
        """
        normalized: Dict[int, Set[int]] = {}
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
        Normalize course metadata keys to integers.

        Args:
            course_metadata (Dict[Any, Dict[str, Any]]): Raw course metadata.

        Returns:
            Dict[int, Dict[str, Any]]: Normalized course metadata.
        """
        normalized: Dict[int, Dict[str, Any]] = {}
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
        Calculate user overlaps between courses, optionally using metadata for sparse overlaps.

        Args:
            course_users (Dict[int, Set[int]]): Mapping of course IDs to sets of user IDs.
            course_metadata (Optional[Dict[int, Dict[str, Any]]]): Optional course metadata for department matching.

        Returns:
            Dict[Tuple[int, int], int]: Mapping of course pair tuples to overlap count.
        """
        overlaps: Dict[Tuple[int, int], int] = defaultdict(int)
        if self.optimize_overlap:
            user_to_courses: Dict[int, Set[int]] = defaultdict(set)
            for course, users in course_users.items():
                for user in users:
                    user_to_courses[user].add(course)
            for courses in user_to_courses.values():
                sorted_courses = sorted(courses)
                for course1, course2 in combinations(sorted_courses, 2):
                    overlaps[course1, course2] += 1
            method_used = "inverted index"
        else:
            course_ids = sorted(course_users.keys())
            for course1, course2 in combinations(course_ids, 2):
                count = len(course_users[course1] & course_users[course2])
                if count > 0:
                    overlaps[course1, course2] = count
            method_used = "combinations"
        if course_metadata is not None:
            course_ids = sorted(course_users.keys())
            for course1, course2 in combinations(course_ids, 2):
                if (course1, course2) not in overlaps:
                    meta1 = (
                        course_metadata.get("department")
                        if (meta1 := course_metadata.get(course1))
                        else None
                    )
                    meta2 = (
                        course_metadata.get("department")
                        if (meta2 := course_metadata.get(course2))
                        else None
                    )
                    if meta1 and meta2 and meta1 == meta2:
                        overlaps[course1, course2] = self.sparse_overlap
        log.debug(
            f"Calculated overlaps using {method_used} for {len(course_users)} courses: {dict(overlaps)}"
        )
        return dict(overlaps)

    def _compute_dynamic_threshold(self, overlaps: Dict[Tuple[int, int], int]) -> int:
        """
        Compute a dynamic grouping threshold based on the median overlap.

        Args:
            overlaps (Dict[Tuple[int, int], int]): Mapping of course pairs to overlap count.

        Returns:
            int: The computed dynamic threshold.
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
        Build a graph where nodes represent courses and edges represent user overlaps.

        Args:
            course_users (Dict[int, Set[int]]): Normalized course user mapping.
            course_metadata (Optional[Dict[int, Dict[str, Any]]]): Optional normalized course metadata.

        Returns:
            nx.Graph: The constructed graph.
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
        Default clustering method using the Louvain algorithm.

        Args:
            graph (nx.Graph): The graph to cluster.

        Returns:
            List[Set[int]]: A list of clusters (sets of course IDs).
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
        Perform clustering using the specified clustering function.

        Args:
            graph (nx.Graph): The graph to cluster.

        Returns:
            List[Set[int]]: A list of clusters.
        """
        return self.clustering_func(graph)

    @staticmethod
    def _chunk_list(
        lst: List[Any], chunk_size: int
    ) -> Generator[List[Any], None, None]:
        """
        Yield successive chunks of a list.

        Args:
            lst (List[Any]): The list to chunk.
            chunk_size (int): The size of each chunk.

        Yields:
            Generator[List[Any], None, None]: Chunks of the list.
        """
        for i in range(0, len(lst), chunk_size):
            yield lst[i : i + chunk_size]

    def _map_clusters_to_categories(self, clusters: List[Set[int]]) -> Dict[int, str]:
        """
        Map clusters of courses to category labels.

        Args:
            clusters (List[Set[int]]): A list of clusters.

        Returns:
            Dict[int, str]: Mapping from course ID to category label.
        """
        mapping: Dict[int, str] = {}
        total_subgroups = sum(
            (ceil(len(cluster) / self.max_category_channels) for cluster in clusters)
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
        Evaluate the quality of clusters using modularity.

        Args:
            graph (nx.Graph): The graph that was clustered.
            clusters (List[Set[int]]): The clusters produced.

        Returns:
            Dict[str, float]: A dictionary with clustering quality metrics.
        """
        mod = modularity(graph, clusters, weight="weight")
        return {"modularity": mod}

    def cluster_courses(
        self,
        course_users: Dict[Any, Set[Any]],
        course_metadata: Optional[Dict[Any, Dict[str, Any]]] = None,
    ) -> Dict[int, str]:
        """
        Cluster courses based on user overlaps and map them to category labels.

        Args:
            course_users (Dict[Any, Set[Any]]): Raw course user data.
            course_metadata (Optional[Dict[Any, Dict[str, Any]]]): Optional raw course metadata.

        Returns:
            Dict[int, str]: Mapping from course ID to category label.
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
        Run the clustering process periodically until a shutdown event is set.

        Args:
            interval (int): Time interval in seconds between clustering cycles.
            get_course_users (Callable[[], Dict[Any, Set[Any]]]): Function to retrieve course user data.
            persist_mapping (Callable[[Dict[int, str]], Any]): Function to persist the mapping.
            shutdown_event (asyncio.Event): Event to signal shutdown.
            course_metadata (Optional[Dict[Any, Dict[str, Any]]]): Optional course metadata.
        """
        log.info("Starting periodic course clustering task.")
        iteration = 1
        while not shutdown_event.is_set():
            log.info(f"Starting clustering cycle iteration {iteration}")
            try:
                if course_users := get_course_users():
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
