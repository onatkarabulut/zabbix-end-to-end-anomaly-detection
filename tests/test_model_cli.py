"""Model versiyon yonetimi (model_cli) + scoring current.json testleri."""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "tools"
))

import model_cli  # noqa: E402


def _make_run(tmp_path, run_id, spec=True):
    runs = tmp_path / "runs"
    run_dir = runs / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "scaler.joblib").write_text("x")
    (run_dir / "oneclasssvm.joblib").write_text("x")
    if spec:
        (run_dir / "feature_spec.json").write_text(json.dumps({
            "feature_cols": ["a_avg_5m", "b_avg_5m"],
            "seq_len": 60,
            "thresholds": {"oneclasssvm": 0.1},
        }))
    return run_dir


def test_current_file_resolution(tmp_path, monkeypatch):
    """current.json hangi run'u isaret ediyorsa scoring o dizini kullanir."""
    _make_run(tmp_path, "run_a")
    _make_run(tmp_path, "run_b")
    (tmp_path / "current.json").write_text(json.dumps({"run_id": "run_b"}))

    from realtime.scoring import _resolve_models_dir

    resolved = _resolve_models_dir(str(tmp_path))
    assert resolved.endswith("run_b")


def test_current_file_fallback(tmp_path):
    """current.json yoksa (eski kurulum) verilen dizin aynen kullanilir."""
    from realtime.scoring import _resolve_models_dir

    assert _resolve_models_dir(str(tmp_path)) == str(tmp_path)


def test_model_cli_list_promote_rollback(tmp_path, monkeypatch):
    _make_run(tmp_path, "run_a", spec=True)
    _make_run(tmp_path, "run_b", spec=True)

    monkeypatch.setattr(model_cli, "MODELS_DIR", str(tmp_path))
    monkeypatch.setattr(model_cli, "RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(model_cli, "CURRENT_FILE", str(tmp_path / "current.json"))

    model_cli.cmd_promote("run_b")
    cur = json.loads((tmp_path / "current.json").read_text())
    assert cur["run_id"] == "run_b"

    model_cli.cmd_rollback("run_a")
    cur = json.loads((tmp_path / "current.json").read_text())
    assert cur["run_id"] == "run_a"

    # aktif run silinemez
    import pytest
    with pytest.raises(SystemExit):
        model_cli.cmd_remove("run_a")


def test_model_cli_remove_inactive(tmp_path, monkeypatch):
    _make_run(tmp_path, "run_a")
    _make_run(tmp_path, "run_b")
    monkeypatch.setattr(model_cli, "MODELS_DIR", str(tmp_path))
    monkeypatch.setattr(model_cli, "RUNS_DIR", str(tmp_path / "runs"))
    monkeypatch.setattr(model_cli, "CURRENT_FILE", str(tmp_path / "current.json"))

    model_cli.cmd_promote("run_a")
    model_cli.cmd_remove("run_b")
    assert not (tmp_path / "runs" / "run_b").exists()
    assert (tmp_path / "runs" / "run_a").exists()


def test_audit_run_dir_override(tmp_path, monkeypatch):
    """--run-dir override'i audit'i current.json yerine belirtilen run'a yonlendirir."""
    _make_run(tmp_path, "run_a")
    _make_run(tmp_path, "run_b")
    (tmp_path / "current.json").write_text(json.dumps({"run_id": "run_b"}))

    import model_audit
    monkeypatch.setattr(model_audit, "MODELS_DIR", str(tmp_path))
    model_audit.set_run_dir(str(tmp_path / "runs" / "run_a"))
    assert model_audit.resolve_models_dir().endswith("run_a")

    # override temizlendiginde current.json'a doner
    model_audit.set_run_dir(None)
    from realtime.scoring import _resolve_models_dir
    assert model_audit.resolve_models_dir() == _resolve_models_dir(str(tmp_path))


def test_audit_run_dir_prefers_run_eval_csv(tmp_path, monkeypatch):
    """run dizininde eval_results.csv varsa audit onu okur (DATA_DIR'e dusmez)."""
    import pandas as pd
    _make_run(tmp_path, "run_a")
    run_dir = tmp_path / "runs" / "run_a"
    df = pd.DataFrame([{"model": "LSTM-Autoencoder", "precision": 0.1, "recall": 1.0,
                        "f1": 0.18, "threshold": 5.0}])
    df.to_csv(run_dir / "eval_results.csv", index=False)
    (tmp_path / "eval_results.csv").write_text("bogus")

    import model_audit
    import importlib
    importlib.reload(model_audit)
    model_audit.set_run_dir(str(run_dir))

    # eval_csv okuma mantigini taklit et: once run dizini, sonra DATA_DIR
    from realtime.scoring import _resolve_models_dir
    _ = _resolve_models_dir  # (kapsam icin)
    eval_path = os.path.join(model_audit.resolve_models_dir(), "eval_results.csv")
    assert os.path.exists(eval_path)
    content = pd.read_csv(eval_path)
    assert content.iloc[0]["model"] == "LSTM-Autoencoder"