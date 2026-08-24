from alertmesh.simclock import Scheduler


def test_runs_in_time_order():
    s = Scheduler()
    seen = []
    s.at(3.0, lambda: seen.append("c"))
    s.at(1.0, lambda: seen.append("a"))
    s.at(2.0, lambda: seen.append("b"))
    s.run()
    assert seen == ["a", "b", "c"]


def test_ties_break_by_insertion_order():
    s = Scheduler()
    seen = []
    s.at(1.0, lambda: seen.append("first"))
    s.at(1.0, lambda: seen.append("second"))
    s.run()
    assert seen == ["first", "second"]


def test_run_until_stops_and_advances_clock():
    s = Scheduler()
    seen = []
    s.at(1.0, lambda: seen.append(1))
    s.at(5.0, lambda: seen.append(5))
    s.run(until=2.0)
    assert seen == [1]
    assert s.now == 2.0
    s.run()
    assert seen == [1, 5]
    assert s.now == 5.0


def test_after_schedules_relative_to_now():
    s = Scheduler()
    times = []
    s.at(2.0, lambda: s.after(3.0, lambda: times.append(s.now)))
    s.run()
    assert times == [5.0]


def test_past_scheduling_clamps_to_now():
    s = Scheduler()
    times = []
    s.at(4.0, lambda: s.at(1.0, lambda: times.append(s.now)))
    s.run()
    assert times == [4.0]
