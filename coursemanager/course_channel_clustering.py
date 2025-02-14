import asyncio
from collections import defaultdict
from itertools import combinations
from math import ceil
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Generator

import networkx as nx
from networkx.algorithms.community import louvain_communities
from .logger_util import get_logger

log = get_logger("red.course_channel_clustering")


class CourseChannelClustering:
    """
    Clusters course channels based on overlapping user memberships.

    Clustering pipeline:
      1. Compute pairwise user overlap between courses.
      2. Build a weighted undirected graph.
      3. Apply a clustering algorithm (default: Louvain).
      4. Map clusters to Discord category labels—splitting clusters into subgroups
         if they exceed the max channels limit, and ensuring each subgroup gets a unique label.
    """

    def __init__(
        self,
        grouping_threshold: int = 2,
        max_category_channels: int = 50,
        category_prefix: str = "COURSES",
        clustering_func: Optional[Callable[[nx.Graph], List[Set[str]]]] = None,
        optimize_overlap: bool = True,
    ) -> None:
        """
        :param grouping_threshold: Minimum user overlap to connect two courses.
        :param max_category_channels: Maximum channels allowed per category.
        :param category_prefix: Base prefix for generated category names.
        :param clustering_func: Custom clustering function (if not provided, defaults to Louvain).
        :param optimize_overlap: If True, use an inverted-index algorithm for overlap calculation.
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

    def _calculate_overlaps(
        self, course_users: Dict[str, Set[str]]
    ) -> Dict[Tuple[str, str], int]:
        """
        Calculate the number of common users between every pair of courses.
        Uses an inverted index approach if optimize_overlap is True.
        """
        overlaps: Dict[Tuple[str, str], int] = defaultdict(int)
        if self.optimize_overlap:
            # Build an inverted index: user -> set of courses the user is in
            user_to_courses: Dict[str, Set[str]] = defaultdict(set)
            for course, users in course_users.items():
                for user in users:
                    user_to_courses[user].add(course)
            # For each user, count overlaps among courses they share
            for courses in user_to_courses.values():
                sorted_courses = sorted(courses)
                for course1, course2 in combinations(sorted_courses, 2):
                    overlaps[(course1, course2)] += 1
            method_used = "inverted index"  # Set method used for logging
        else:
            # Fallback to direct pairwise comparison
            course_ids = sorted(course_users.keys())
            for course1, course2 in combinations(course_ids, 2):
                count = len(course_users[course1] & course_users[course2])
                if count > 0:
                    overlaps[(course1, course2)] = count
            method_used = "combinations"  # Set method used for logging

        # Single debug log message after computing overlaps
        log.debug(
            f"Calculated overlaps using {method_used} for {len(course_users)} courses: {dict(overlaps)}"
        )
        return dict(overlaps)

    def _build_graph(self, course_users: Dict[str, Set[str]]) -> nx.Graph:
        """
        Build a weighted graph where nodes represent courses and
        edges represent the user overlap (if above the grouping threshold).
        """
        graph = nx.Graph()
        # DRY: Iterate over sorted (course, users) pairs to avoid repeated lookups.
        for course, users in sorted(course_users.items()):
            graph.add_node(course)
            if not users:
                log.warning(f"Course '{course}' has no user engagements.")
        overlaps = self._calculate_overlaps(course_users)
        # Add an edge if the overlap meets the threshold
        for (course1, course2), weight in overlaps.items():
            if weight >= self.grouping_threshold:
                graph.add_edge(course1, course2, weight=weight)
        log.debug(
            f"Graph built: {graph.number_of_nodes()} nodes, {graph.number_of_edges()} edges."
        )
        return graph

    def _default_clustering(self, graph: nx.Graph) -> List[Set[str]]:
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

    def _perform_clustering(self, graph: nx.Graph) -> List[Set[str]]:
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

    def _map_clusters_to_categories(self, clusters: List[Set[str]]) -> Dict[str, str]:
        """
        Map clusters (and their subgroups) to unique Discord category labels.

        Each subgroup (of size up to max_category_channels) receives a unique label.
        If there is more than one subgroup overall, suffixes are added to the base prefix.
        """
        mapping: Dict[str, str] = {}
        # Pre-calculate total subgroups to decide if suffixes are needed.
        total_subgroups = sum(
            ceil(len(cluster) / self.max_category_channels) for cluster in clusters
        )
        use_suffix = total_subgroups > 1

        subgroup_counter = 1
        # Process clusters in a stable order (by smallest course ID)
        for cluster in sorted(clusters, key=lambda c: min(c) if c else ""):
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

    def cluster_courses(self, course_users: Dict[str, Set[str]]) -> Dict[str, str]:
        """
        Run the full clustering pipeline to map course IDs to Discord category labels.
        """
        graph = self._build_graph(course_users)
        clusters = self._perform_clustering(graph)
        log.info(f"Detected {len(clusters)} clusters.")
        mapping = self._map_clusters_to_categories(clusters)
        log.info(f"Final course-to-category mapping: {mapping}")
        return mapping

    async def run_periodic(
        self,
        interval: int,
        get_course_users: Callable[[], Dict[str, Set[str]]],
        persist_mapping: Callable[[Dict[str, str]], Any],
        shutdown_event: asyncio.Event,
    ) -> None:
        """
        Periodically run the clustering process until a shutdown signal is received.
        """
        log.info("Starting periodic course clustering task.")
        iteration = 1
        while not shutdown_event.is_set():
            log.info(f"Starting clustering cycle iteration {iteration}")
            try:
                if course_users := get_course_users():
                    mapping = self.cluster_courses(course_users)
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
