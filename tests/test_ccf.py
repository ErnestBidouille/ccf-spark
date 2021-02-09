from ccf_spark import Ccf
import pytest


def test_ccf_iterate_map():
    assert set(Ccf.Iterate.map((12, 14))) == set([(12, 14), (14, 12)])
    with pytest.raises(ValueError):
        Ccf.Iterate.map((1, 2, 3))


def test_ccf_iterate_reduce():
    assert set(Ccf.Iterate.reduce((5, [4, 2, 3, 1]))) == set([(2, 1), (3, 1),
                                                              (4, 1), (5, 1)])
    with pytest.raises(ValueError):
        list(Ccf.Iterate.reduce((1, 2, 3)))


def test_ccf_iterate_secondary_sorting_map():
    assert set(Ccf.IterateSecondarySorting.map((12, 14))) == set([(12, 14),
                                                                  (14, 12)])
    with pytest.raises(ValueError):
        Ccf.IterateSecondarySorting.map((1, 2, 3))


def test_ccf_iterate_secondary_sorting_reduce():
    assert set(Ccf.IterateSecondarySorting.reduce(
        (5, [1, 2, 3, 4]), ), ) == set([(2, 1), (3, 1), (4, 1), (5, 1)])
    with pytest.raises(ValueError):
        list(Ccf.IterateSecondarySorting.reduce((1, 2, 3)))


def test_ccf_dedup_map():
    assert Ccf.Dedup.map((1, 2)) == ((1, 2), None)
    with pytest.raises(ValueError):
        Ccf.Dedup.map((1, 2, 3))


def test_ccf_dedup_reduce():
    assert Ccf.Dedup.reduce(((1, 2), [4, 6, 8])) == (1, 2)
    with pytest.raises(ValueError):
        Ccf.Dedup.reduce((1, 2, 3))
