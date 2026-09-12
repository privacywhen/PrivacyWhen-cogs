import asyncio
import unittest
from unittest.mock import patch

from coursemanager import utils
from coursemanager.course_code_resolver import CourseCodeResolver


class CourseInputNormalizationTests(unittest.TestCase):
    def test_unwrap_course_input_contract(self) -> None:
        cases = (
            ("socwork 2cc3", "socwork 2cc3"),
            (" socwork 2cc3 ", " socwork 2cc3 "),
            ("<socwork 2cc3>", "socwork 2cc3"),
            (" <socwork 2cc3> ", "socwork 2cc3"),
            ("< socwork 2cc3 >", "socwork 2cc3"),
            ("<SOCWORK-2CC3>", "SOCWORK-2CC3"),
            ("<socwork_2cc3>", "socwork_2cc3"),
            ("<socwork2cc3>", "socwork2cc3"),
            ("<<socwork 2cc3>>", "<<socwork 2cc3>>"),
            ("<socwork <2cc3>>", "<socwork <2cc3>>"),
            ("<socwork 2cc3", "<socwork 2cc3"),
            ("socwork 2cc3>", "socwork 2cc3>"),
            ("socwork <2cc3>", "socwork <2cc3>"),
            ("<>", "<>"),
            ("<   >", "<   >"),
        )
        for raw_input, expected in cases:
            with self.subTest(raw_input=raw_input):
                self.assertEqual(utils._unwrap_course_input(raw_input), expected)

    def test_wrapped_and_unwrapped_valid_inputs_resolve_identically(self) -> None:
        listings = {"SOCWORK-2CC3": "test listing"}

        unwrapped = asyncio.run(
            utils.validate_and_resolve_course_code(
                object(), "socwork 2cc3", listings, None
            )
        )
        wrapped = asyncio.run(
            utils.validate_and_resolve_course_code(
                object(), "<socwork 2cc3>", listings, None
            )
        )

        self.assertIsNotNone(unwrapped)
        self.assertIsNotNone(wrapped)
        self.assertEqual(unwrapped.canonical(), "SOCWORK-2CC3")
        self.assertEqual(wrapped.canonical(), unwrapped.canonical())

    def test_wrapped_suffix_preserves_canonical_and_channel_identity(self) -> None:
        listings = {"SOCWORK-2A06A": "test listing"}

        resolved = asyncio.run(
            utils.validate_and_resolve_course_code(
                object(), "<socwork 2a06a>", listings, None
            )
        )

        self.assertIsNotNone(resolved)
        self.assertEqual(resolved.canonical(), "SOCWORK-2A06A")
        self.assertEqual(resolved.formatted_channel_name(), "socwork-2a06")

    def test_fuzzy_fallback_receives_semantic_input(self) -> None:
        queries: list[str] = []

        async def record_query(self, ctx, query):
            queries.append(query)
            return None, None

        with patch.object(CourseCodeResolver, "fallback_fuzzy_lookup", record_query):
            wrapped_result = asyncio.run(
                utils.validate_and_resolve_course_code(
                    object(), "<notacourse>", {}, None
                )
            )
            ordinary_result = asyncio.run(
                utils.validate_and_resolve_course_code(object(), "notacourse", {}, None)
            )
            nested_result = asyncio.run(
                utils.validate_and_resolve_course_code(
                    object(), "<<notacourse>>", {}, None
                )
            )

        self.assertIsNone(wrapped_result)
        self.assertIsNone(ordinary_result)
        self.assertIsNone(nested_result)
        self.assertEqual(queries, ["notacourse", "notacourse", "<<notacourse>>"])


if __name__ == "__main__":
    unittest.main()
