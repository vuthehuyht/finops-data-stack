"""Tests for src.nlp.evaluation."""

import pytest

from src.nlp.evaluation import (
    macro_f1,
    model_version_prefix,
    promotion_basis,
    promotion_score,
    should_promote,
)


def test_should_promote_when_no_champion_exists() -> None:
    assert should_promote(challenger_score=0.70, champion_score=None) is True


def test_should_promote_when_challenger_beats_champion() -> None:
    assert should_promote(challenger_score=0.75, champion_score=0.70) is True


def test_should_not_promote_when_challenger_worse() -> None:
    assert should_promote(challenger_score=0.65, champion_score=0.70) is False


def test_should_not_promote_on_tie() -> None:
    assert should_promote(challenger_score=0.70, champion_score=0.70) is False


def test_promotion_score_prefers_in_domain_score() -> None:
    metadata = {"val_macro_f1": 0.9, "domain_macro_f1": 0.6}
    assert promotion_score(metadata) == 0.6


def test_promotion_score_falls_back_to_validation_score() -> None:
    assert promotion_score({"val_macro_f1": 0.9, "domain_macro_f1": None}) == 0.9
    assert promotion_score({"val_macro_f1": 0.9}) == 0.9


def test_promotion_score_requires_a_score() -> None:
    with pytest.raises(KeyError):
        promotion_score({})


def test_macro_f1_perfect_predictions() -> None:
    assert macro_f1([0, 1, 2, 1], [0, 1, 2, 1]) == 1.0


def test_macro_f1_averages_per_class_scores() -> None:
    # class 0: tp=1 fp=1 fn=0 -> f1 = 2/3; class 1: tp=1 fp=0 fn=1 -> f1 = 2/3
    # class 2 absent from both -> ignored.
    score = macro_f1([0, 1, 1], [0, 0, 1])
    assert score == pytest.approx(2 / 3)


def test_macro_f1_counts_missed_class_as_zero() -> None:
    # class 1 is never predicted -> its f1 is 0, dragging the average down.
    assert macro_f1([0, 1], [0, 0]) == pytest.approx((2 / 3 + 0.0) / 2)


def test_macro_f1_rejects_length_mismatch() -> None:
    with pytest.raises(ValueError, match="length"):
        macro_f1([0, 1], [0])


def test_model_version_prefix() -> None:
    assert model_version_prefix("job-1") == "nlp-sentiment/versions/job-1/"


def test_promotion_basis_reports_the_evaluation_set() -> None:
    assert promotion_basis({"val_macro_f1": 0.9, "domain_macro_f1": 0.6}) == "domain"
    assert promotion_basis({"val_macro_f1": 0.9, "domain_macro_f1": None}) == "val"
    assert promotion_basis({"val_macro_f1": 0.9}) == "val"
