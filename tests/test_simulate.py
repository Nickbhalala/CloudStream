from cloudstream import simulate


def test_simulation_runs(capsys):
    assert simulate.main(["--events", "200", "--duplicates", "20", "--bad", "3"]) == 0
    out = capsys.readouterr().out
    assert "'AppliedRecords': 200" in out
    assert "'DuplicateRecords': 20" in out
    assert "dead-lettered: 3" in out
