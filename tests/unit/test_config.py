from __future__ import annotations

import re
from pathlib import Path

import pytest

from sluicebox import ConfigurationError, InfluxSettings, load_settings
from sluicebox._units import parse_duration
from sluicebox.types import FieldType

CONFIG = """
name = "ingest"

[connection]
url = "http://localhost:8181/"
version = "v3"
database = "telemetry"

[write]
batch_size = 5000
flush_interval = "250ms"
max_batch_bytes = "4MiB"

[write.retry]
max_attempts = 3

[measurements.cpu]
fields = { usage = "double", cores = "int" }
tags = ["host"]
required_tags = ["host"]

[tags]
static = { env = "dev" }

[[tags.rules]]
when.tags = { device = '^(?P<site>[a-z]+)-\\d+$' }
set = { site = "{site}" }
"""


@pytest.fixture
def config_file(tmp_path: Path) -> Path:
    path = tmp_path / "sluicebox.toml"
    path.write_text(CONFIG)
    (tmp_path / ".env").write_text(
        "SLUICEBOX_TOKEN=secret-from-dotenv\nSLUICEBOX_WRITE__CONCURRENCY=8\nOTHER_APP=1\n"
    )
    return path


def test_layered_loading(config_file: Path) -> None:
    settings = load_settings()  # finds ./sluicebox.toml and ./.env
    assert settings.name == "ingest"
    assert settings.connection.url == "http://localhost:8181"
    assert settings.connection.version == 3
    assert settings.write.batch_size == 5000
    assert settings.write.flush_interval == 0.25
    assert settings.write.max_batch_bytes == 4 * 1024 * 1024
    assert settings.write.concurrency == 8  # from .env
    assert settings.write.retry.max_attempts == 3
    assert settings.write.retry.initial_delay == 0.5  # default kept inside a partially specified table
    assert settings.token is not None
    assert settings.token.get_secret_value() == "secret-from-dotenv"
    assert settings.measurements["cpu"].fields == {"usage": FieldType.FLOAT, "cores": FieldType.INTEGER}
    assert settings.write_api == "v3"
    assert settings.query_language == "sql"


def test_precedence(config_file: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SLUICEBOX_WRITE__BATCH_SIZE", "777")
    assert load_settings().write.batch_size == 777  # env beats .env and TOML
    settings = load_settings(write={"batch_size": 999})
    assert settings.write.batch_size == 999  # keyword overrides beat everything
    assert settings.write.flush_interval == 0.25  # ... and are merged, not replacing the table
    assert settings.write.concurrency == 8


def test_secret_never_rendered(config_file: Path) -> None:
    settings = load_settings()
    for text in (repr(settings), str(settings), settings.model_dump_json()):
        assert "secret-from-dotenv" not in text


def test_token_in_toml_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "bad.toml"
    path.write_text('token = "oops"\n[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\n')
    with pytest.raises(ConfigurationError, match="secrets must not be stored"):
        load_settings(path)


@pytest.mark.parametrize(
    ("toml", "message"),
    [
        (
            '[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\n[write]\nbatchsize = 5\n',
            "batchsize",
        ),
        ('[connection]\nurl = "ftp://x"\nversion = 3\ndatabase = "d"\n', "http"),
        ('[connection]\nurl = "http://x"\nversion = 4\ndatabase = "d"\n', "version"),
        ('[connection]\nurl = "http://x"\nversion = 2\ndatabase = "d"\n', "org is required"),
        (
            '[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\n[write]\nflush_interval = "soon"\n',
            "duration",
        ),
        (
            '[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\n[[tags.rules]]\nset = {a = "{x}"}\n',
            "{x}",
        ),
        (
            '[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\n[measurements.m]\nfields = {v = "decimal"}\n',
            "field type",
        ),
        (
            "[connection]\nurl = 'http://x'\nversion = 3\ndatabase = 'd'\n[measurements.m]\ntags = ['a']\nrequired_tags = ['b']\n",
            "b",
        ),
        ("[write]\nbatch_size = 1\n", "connection"),
        ("not toml [", "invalid TOML"),
    ],
)
def test_invalid_configs(tmp_path: Path, toml: str, message: str) -> None:
    path = tmp_path / "c.toml"
    path.write_text(toml)
    with pytest.raises(
        ConfigurationError, match=message.replace("[", r"\[").replace("{", r"\{").replace("}", r"\}")
    ):
        load_settings(path, env_file=None, token="t")


def test_v2_needs_a_token(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text('[connection]\nurl = "http://x"\nversion = 2\ndatabase = "b"\norg = "o"\n')
    with pytest.raises(ConfigurationError, match="SLUICEBOX_TOKEN"):
        load_settings(path, env_file=None)
    assert load_settings(path, env_file=None, token="t").token is not None


def test_typo_in_dotenv_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text('[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\n')
    env = tmp_path / ".env.test"
    env.write_text("SLUICEBOX_TOKN=x\n")
    with pytest.raises(ConfigurationError, match="sluicebox_tokn"):
        load_settings(path, env_file=env)


def test_section_and_env_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "app.toml"
    path.write_text(
        '[app]\nx = 1\n[services.influx.connection]\nurl = "http://y:8181"\nversion = 3\ndatabase = "nested"\n'
    )
    monkeypatch.setenv("PRIMARY_TOKEN", "primary-secret")
    settings = load_settings(path, section="services.influx", env_prefix="PRIMARY_", env_file=None)
    assert settings.connection.database == "nested"
    assert settings.token is not None
    with pytest.raises(ConfigurationError, match="section"):
        load_settings(path, section="services.missing", env_file=None)


def test_config_path_from_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "elsewhere.toml"
    path.write_text('[connection]\nurl = "http://z"\nversion = 3\ndatabase = "from-env-path"\n')
    monkeypatch.setenv("SLUICEBOX_CONFIG", str(path))
    assert load_settings(env_file=None).connection.database == "from-env-path"
    with pytest.raises(ConfigurationError, match="not found"):
        load_settings(tmp_path / "missing.toml")


def test_secrets_dir(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "sluicebox_token").write_text("from-secrets-dir")
    settings = load_settings(
        None,
        env_file=None,
        secrets_dir=secrets,
        connection={"url": "http://x", "version": 3, "database": "d"},
    )
    assert settings.token is not None
    assert settings.token.get_secret_value() == "from-secrets-dir"


def test_bucket_alias_is_not_needed_but_database_works_for_v2(tmp_path: Path) -> None:
    settings = InfluxSettings(
        token="t", connection={"url": "http://x", "version": 2, "database": "bucket", "org": "o"}
    )
    assert settings.write_api == "v2"
    assert settings.query_language == "flux"


@pytest.mark.parametrize(
    ("text", "seconds"),
    [
        ("250ms", 0.25),
        ("1h30m", 5400.0),
        ("2d", 172800.0),
        ("10", 10.0),
        (3, 3.0),
        ("1.5s", 1.5),
        ("100us", 1e-4),
    ],
)
def test_durations(text: object, seconds: float) -> None:
    assert parse_duration(text) == pytest.approx(seconds)


@pytest.mark.parametrize("text", ["soon", "5 parsecs", "1h banana", "ms"])
def test_bad_durations(text: str) -> None:
    with pytest.raises(ValueError, match="invalid duration"):
        parse_duration(text)


def test_proxy_password_in_toml_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text(
        '[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\nproxy = "http://u:secret@proxy:3128"\n'
    )
    with pytest.raises(ConfigurationError, match=r"connection\.proxy"):
        load_settings(path, env_file=None)
    path.write_text(
        '[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\nproxy = "http://proxy:3128"\n'
    )
    assert load_settings(path, env_file=None).connection.proxy == "http://proxy:3128"


def test_example_config_is_valid(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """The documented example must always load (and therefore document real options)."""
    example = Path(__file__).resolve().parents[2] / "sluicebox.example.toml"
    settings = load_settings(example, env_file=None, token="t")  # usable as copied
    assert settings.write.batch_size == 25_000
    assert settings.write.concurrency == 16
    assert not settings.measurements
    assert not settings.tags.rules
    # The commented-out schema and tag examples are valid too.
    text = re.sub(
        r"^# ((\[|static|from_env|fields|tags|required_|extra_fields|name|when\.|set )[^\n]*)$",
        r"\1",
        example.read_text(),
        flags=re.MULTILINE,
    )
    uncommented = tmp_path / "uncommented.toml"
    uncommented.write_text(text)
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    full = load_settings(uncommented, env_file=None, token="t")
    assert full.measurements["cpu"].required_tags == {"host"}
    assert [rule.name for rule in full.tags.rules] == ["site-from-device", "overheating"]
    assert full.tags.static == {"env": "production"}
    defaults = load_settings(
        None, env_file=None, token="t", connection={"url": "http://x", "version": 3, "database": "d"}
    )
    for section in ("write", "query", "validation", "metrics", "profiling", "logging"):
        documented = getattr(settings, section).model_dump(exclude={"retry"})
        default = getattr(defaults, section).model_dump(exclude={"retry"})
        assert documented == default, f"[{section}] in sluicebox.example.toml no longer matches the defaults"
    assert settings.write.retry == defaults.write.retry


def _minimal(tmp_path: Path, extra: str = "") -> Path:
    path = tmp_path / "sluicebox.toml"
    path.write_text(f'[connection]\nurl = "http://localhost:8181"\nversion = 3\ndatabase = "d"\n{extra}')
    return path


def test_typos_get_a_suggestion(tmp_path: Path) -> None:
    path = _minimal(tmp_path, '[write]\nflush_intreval = "1s"\n')
    with pytest.raises(ConfigurationError, match=r"write.flush_intreval: .*did you mean 'flush_interval'\?"):
        load_settings(path, env_file=None)


def test_missing_config_file_says_where_it_looked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ConfigurationError) as info:
        load_settings(env_file=None, token="t")
    message = str(info.value)
    assert "no config file was found" in message
    assert str(tmp_path / "sluicebox.toml") in message
    assert "SLUICEBOX_CONNECTION__URL" in message


def test_token_anywhere_in_the_toml_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "c.toml"
    path.write_text('[connection]\nurl = "http://x"\nversion = 3\ndatabase = "d"\ntoken = "oops"\n')
    with pytest.raises(ConfigurationError, match=r"connection\.token.*secrets must not be stored"):
        load_settings(path, env_file=None)


def test_a_field_or_tag_named_token_is_not_a_secret(tmp_path: Path) -> None:
    path = _minimal(
        tmp_path,
        '[tags.static]\ntoken = "abc"\n[measurements.llm.fields]\ntoken = "string"\n'
        '[measurements.token]\nfields = { n = "integer" }\n',
    )
    settings = load_settings(path, env_file=None)
    assert settings.tags.static == {"token": "abc"}
    assert set(settings.measurements) == {"llm", "token"}


def test_settings_with_overrides_merge_tables(tmp_path: Path) -> None:
    settings = load_settings(_minimal(tmp_path, "[write]\nbatch_size = 10\n"), env_file=None)
    merged = settings.with_overrides(write={"concurrency": 2})
    assert (merged.write.batch_size, merged.write.concurrency) == (10, 2)
    assert merged.origin == settings.origin
    with pytest.raises(ConfigurationError, match="did you mean 'concurrency'"):
        settings.with_overrides(write={"concurency": 2})


def test_environment_selects_section_and_token_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "app.toml"
    path.write_text(
        '[dev.connection]\nurl = "http://dev:8181"\nversion = 3\ndatabase = "d"\n'
        '[prod.connection]\nurl = "http://prod:8181"\nversion = 3\ndatabase = "p"\n'
    )
    secret = tmp_path / "token"
    secret.write_text("from-a-file\n")
    monkeypatch.setenv("SLUICEBOX_CONFIG", str(path))
    monkeypatch.setenv("SLUICEBOX_SECTION", "prod")
    monkeypatch.setenv("SLUICEBOX_TOKEN_FILE", str(secret))
    settings = load_settings(env_file=None)
    assert settings.connection.url == "http://prod:8181"
    assert settings.token is not None
    assert settings.token.get_secret_value() == "from-a-file"


def test_unknown_environment_variables_are_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("SLUICEBOX_DATABASE", "prod_db")
    load_settings(_minimal(tmp_path), env_file=None)
    assert "SLUICEBOX_DATABASE is not an sluicebox setting" in caplog.text
    assert "did you mean SLUICEBOX_CONNECTION__DATABASE?" in caplog.text
    monkeypatch.setenv("SLUICEBOX_WRITE__BATCHSIZE", "1")  # inside a known table: an error
    with pytest.raises(ConfigurationError) as info:
        load_settings(_minimal(tmp_path), env_file=None)
    assert "did you mean 'batch_size'?" in str(info.value)
    assert "[from environment variable SLUICEBOX_WRITE__BATCHSIZE]" in str(info.value)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("http://h:8086/api/v2", "http://h:8086"),
        ("http://h:8181/api/v3/", "http://h:8181"),
        ("https://gateway/influx/", "https://gateway/influx"),
    ],
)
def test_url_paths(url: str, expected: str) -> None:
    settings = load_settings(None, env_file=None, connection={"url": url, "version": 3, "database": "d"})
    assert settings.connection.url == expected


def test_optional_environment_tags() -> None:
    from sluicebox.config import TagsConfig
    from sluicebox.tags import TagInjector

    config = TagsConfig.model_validate(
        {"from_env": {"pod": {"var": "POD_NAME", "default": "local"}, "zone": {"var": "ZONE", "default": ""}}}
    )
    assert TagInjector(config, environ={}).static == {"pod": "local"}
    assert TagInjector(config, environ={"POD_NAME": "p-1", "ZONE": "z"}).static == {"pod": "p-1", "zone": "z"}
    required = TagsConfig.model_validate({"from_env": {"host": "HOSTNAME"}})
    with pytest.raises(ConfigurationError, match=r'host = \{ var = "HOSTNAME", default = "..." \}'):
        TagInjector(required, environ={})


def test_static_tags_argument_is_not_for_settings() -> None:
    from sluicebox import InfluxClient

    with pytest.raises(TypeError, match=r"\['on_conflict'\] are \[tags\] settings"):
        InfluxClient(tags={"on_conflict": "overwrite"})
