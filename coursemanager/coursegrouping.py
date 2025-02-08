import asyncio
import logging
from itertools import combinations
from typing import Dict, List, Tuple

import networkx as nx
import community.community_louvain as community_louvain
from redbot.core import Config


class CourseGrouping:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        # Default threshold; can be updated via config (see below)
        self.grouping_threshold: int = 2
        self.course_clusters: Dict[int, List[str]] = {}

    async def compute_groups(self) -> Dict[int, List[str]]:
        """
        Compute course clusters based on user enrollments.
        Uses the enrollment data stored in the "enrollments" key of the config.
        Returns a mapping of community_id → list of course codes.
        """
        enrollments_data = await self.config.enrollments.all()
        all_courses = set()
        co_occurrence: Dict[Tuple[str, str], int] = {}

        # Count co-occurrences for every unique pair of courses a user is enrolled in.
        for user_id, courses in enrollments_data.items():
            if not isinstance(courses, list):
                continue
            unique_courses = set(courses)
            all_courses |= unique_courses
            for course_pair in combinations(sorted(unique_courses), 2):
                co_occurrence[course_pair] = co_occurrence.get(course_pair, 0) + 1

        # Build the weighted undirected graph.
        graph = nx.Graph()
        for course in all_courses:
            graph.add_node(course)
        for (course_a, course_b), count in co_occurrence.items():
            if count >= self.grouping_threshold:
                graph.add_edge(course_a, course_b, weight=count)
        self.logger.debug(
            "Graph built with %s nodes and %s edges",
            graph.number_of_nodes(),
            graph.number_of_edges(),
        )

        if not graph.nodes:
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
        Retrieve the threshold from config and recompute the groups.
        """
        threshold = await self.config.grouping_threshold.get()
        self.grouping_threshold = threshold
        return await self.compute_groups()

    async def schedule_group_update(self, interval: int = 604800) -> None:
        """
        Run group updates on a schedule.
        The default interval is one week (604800 seconds).
        """
        while True:
            try:
                await self.update_groups()
                self.logger.debug("Course groupings updated: %s", self.course_clusters)
            except Exception as e:
                self.logger.error("Error updating course groupings: %s", e)
            await asyncio.sleep(interval)
