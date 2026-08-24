import os

from alertmesh.journal import Journal


def make(tmp_path, name="j.wal"):
    return Journal(str(tmp_path / name))


def test_append_then_replay_round_trip(tmp_path):
    j = make(tmp_path)
    recs = [{"alert_id": "a:%d" % i, "kind": "help_button"} for i in range(10)]
    for r in recs:
        j.append(r)
    j.close()
    got, skipped = j.replay()
    assert got == recs
    assert skipped == 0


def test_replay_preserves_append_order(tmp_path):
    j = make(tmp_path)
    for i in range(100):
        j.append({"i": i})
    j.close()
    got, _ = j.replay()
    assert [r["i"] for r in got] == list(range(100))


def test_empty_journal_replays_empty(tmp_path):
    j = make(tmp_path)
    j.close()
    got, skipped = j.replay()
    assert got == []
    assert skipped == 0


def test_missing_file_replays_empty(tmp_path):
    j = Journal(str(tmp_path / "never.wal"))
    j.close()
    os.remove(str(tmp_path / "never.wal"))
    got, skipped = j.replay()
    assert got == []


def test_torn_tail_is_skipped_not_fatal(tmp_path):
    j = make(tmp_path)
    j.append({"i": 1})
    j.append({"i": 2})
    j.close()
    with open(j.path, "a") as fh:
        fh.write('{"i": 3}|dead')  # torn write: bad crc, no newline
    got, skipped = j.replay()
    assert [r["i"] for r in got] == [1, 2]
    assert skipped == 1


def test_corrupt_middle_line_is_skipped(tmp_path):
    j = make(tmp_path)
    j.append({"i": 1})
    j.append({"i": 2})
    j.append({"i": 3})
    j.close()
    lines = open(j.path).read().splitlines()
    lines[1] = lines[1][:-2] + "zz"  # corrupt crc of the middle record
    with open(j.path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
    got, skipped = j.replay()
    assert [r["i"] for r in got] == [1, 3]
    assert skipped == 1


def test_append_after_reopen_continues_file(tmp_path):
    j = make(tmp_path)
    j.append({"i": 1})
    j.close()
    j.reopen()
    j.append({"i": 2})
    j.close()
    got, _ = j.replay()
    assert [r["i"] for r in got] == [1, 2]
