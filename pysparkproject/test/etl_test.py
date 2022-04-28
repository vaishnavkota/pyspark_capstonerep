import datetime
import pytest

format = "%Y-%m-%d"


@pytest.mark.one
def test_begin_year_one():
    testcases_start_one = "2017-01-01"
    assert bool(datetime.datetime.strptime(testcases_start_one, format)) == True


@pytest.mark.two
def test_begin_year_two():
    testcases_start_two = "2019-12-11"
    assert bool(datetime.datetime.strptime(testcases_start_two, format)) == True


@pytest.mark.three
def test_begin_year_three():
    testcases_start_three = "2018/01-01"
    assert bool(datetime.datetime.strptime(testcases_start_three, format)) == True


@pytest.mark.four
def test_begin_year_four():
    testcases_start_four = "2017/01/01"
    assert bool(datetime.datetime.strptime(testcases_start_four, format)) == True


@pytest.mark.five
def test_begin_year_five():
    testcases_start_five = "2018"
    assert bool(datetime.datetime.strptime(testcases_start_five, format)) == True
    print("testing end_year")


@pytest.mark.six
def test_ending_year_one():
    testcases_end_one = "2018-01-01"
    assert bool(datetime.datetime.strptime(testcases_end_one, format)) == True


@pytest.mark.seven
def test_ending_year_two():
    testcases_end_two = "2018-11-12"
    assert bool(datetime.datetime.strptime(testcases_end_two, format)) == True


@pytest.mark.eight
def test_ending_year_three():
    testcases_end_three = "2019_01-01"
    assert bool(datetime.datetime.strptime(testcases_end_three, format)) == True


@pytest.mark.nine
def test_ending_year_four():
    testcases_end_four = "01-01-2019"
    assert bool(datetime.datetime.strptime(testcases_end_four, format)) == True


@pytest.mark.ten
def test_ending_year_five():
    testcases_end_five = "3000-01-01"
    assert bool(datetime.datetime.strptime(testcases_end_five, format)) == True
