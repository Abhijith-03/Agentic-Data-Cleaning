"""Tests for the LangGraph workflow assembly."""

from src.graph.workflow import build_graph, should_reclean, should_human_review


class TestConditionalRouting:
    def test_reclean_when_failed_and_iterations_remain(self):
        state = {"validation_passed": False, "iteration_count": 1}
        assert should_reclean(state) == "reclean"

    def test_score_when_passed(self):
        state = {"validation_passed": True, "iteration_count": 1}
        assert should_reclean(state) == "score"

    def test_score_when_max_iterations(self):
        state = {"validation_passed": False, "iteration_count": 3}
        assert should_reclean(state) == "score"

    def test_output_when_no_low_confidence(self):
        state = {"low_confidence_fixes": []}
        assert should_human_review(state) == "output"

    def test_output_when_human_loop_disabled(self):
        state = {"low_confidence_fixes": [{"some": "fix"}]}
        assert should_human_review(state) == "output"


class TestGraphBuild:
    def test_graph_compiles(self):
        graph = build_graph()
        compiled = graph.compile()
        assert compiled is not None

    def test_graph_has_all_nodes(self):
        graph = build_graph()
        node_names = set(graph.nodes.keys())
        expected = {
            "ingest",
            "reconstruction_schema_planner",
            "structure_reconstruction",
            "schema_analysis",
            "data_profiling",
            "anomaly_detection",
            "cleaning",
            "validation",
            "confidence_scoring",
            "human_review",
            "output",
        }
        assert expected.issubset(node_names)
