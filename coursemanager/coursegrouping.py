import asyncio
import logging
from itertools import combinations
from typing import Dict, List, Tuple

import networkx as nx
import community.community_louvain as community_louvain
from redbot.core import Config


class CourseGrouping:
    """
    Handles dynamic grouping of courses based on user enrollments.

    This class computes the co-occurrence of courses among user enrollments,
    builds a weighted graph, and then applies the Louvain algorithm for community
    detection. The computed clusters are cached in `course_clusters`.
    """

    def __init__(self, config: Config, logger: logging.Logger) -> None:
        """
        Initialize the CourseGrouping instance.

        Args:
            config: The RedBot config instance.
            logger: A logger instance for debug and error messages.
        """
        self.config = config
        self.logger = logger
        self.grouping_threshold: int = 2  # Default threshold; updated from config.
        self.course_clusters: Dict[int, List[str]] = {}

    async def compute_groups(self) -> Dict[int, List[str]]:
        """
        Compute course clusters based on user enrollments.

        This method reads the enrollment data from the config's "enrollments" key,
        builds a weighted undirected graph from the co-occurrence counts, and then
        detects communities (clusters) using the Louvain algorithm.

        Returns:
            A dictionary mapping community IDs to lists of course codes.
        """
        enrollments_data = await self.config.enrollments.all()
        all_courses = set()
        co_occurrence: Dict[Tuple[str, str], int] = {}

        # Process each user's enrollment list (only if it's a list)
        for courses in (v for v in enrollments_data.values() if isinstance(v, list)):
            unique_courses = set(courses)
            all_courses.update(unique_courses)
            # Count co-occurrences for every unique pair in sorted order.
            for course_pair in combinations(sorted(unique_courses), 2):
                co_occurrence[course_pair] = co_occurrence.get(course_pair, 0) + 1

        # Build the weighted undirected graph.
        graph = nx.Graph()
        graph.add_nodes_from(all_courses)
        # Filter and add only those edges where count meets or exceeds the threshold.
        edges = [
            (a, b, count)
            for (a, b), count in co_occurrence.items()
            if count >= self.grouping_threshold
        ]
        if edges:
            graph.add_weighted_edges_from(edges)

        self.logger.debug(
            "Graph built with %s nodes and %s edges",
            graph.number_of_nodes(),
            graph.number_of_edges(),
        )

        if graph.number_of_nodes() == 0:
            self.course_clusters = {}
            return {}

        # Run community detection using the Louvain algorithm.
        partition = community_louvain.best_partition(graph)
        groups: Dict[int, List[str]] = {}
        for course, community_id in partition.items():
            groups.setdefault(community_id, []).append(course)

        self.logger.debug("Detected %s clusters", len(groups))
        self.course_clusters = groups
        return groups

    async def update_groups(self) -> Dict[int, List[str]]:
        """
        Update the grouping threshold from config and recompute course clusters.

        Returns:
            The newly computed clusters as a dictionary mapping community IDs to lists of course codes.
        """
        # Retrieve the threshold from config using the coroutine-call syntax.
        threshold = await self.config.grouping_threshold()
        self.grouping_threshold = threshold
        return await self.compute_groups()

    async def schedule_group_update(self, interval: int = 604800) -> None:
        """
        Periodically update course groupings on a set interval.

        Args:
            interval: Time in seconds between updates (default is one week: 604800 seconds).
        """
        while True:
            try:
                updated_groups = await self.update_groups()
                self.logger.debug("Course groupings updated: %s", updated_groups)
            except Exception as e:
                self.logger.error("Error updating course groupings: %s", e)
            await asyncio.sleep(interval)
