import json

import pandas as pd
import pytest
import requests
from sempy.fabric.exceptions import FabricHTTPException

import sempy_labs.report._generate_embed_token as embed

REPORT = "11111111-1111-1111-1111-111111111111"
PROXY = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
CORE = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
ROOT = "cccccccc-cccc-cccc-cccc-cccccccccccc"
PROXY_WS = "0000000a-0000-0000-0000-000000000000"
CORE_WS = "0000000b-0000-0000-0000-000000000000"
ROOT_WS = "0000000c-0000-0000-0000-000000000000"

WORKSPACES = {"Proxy WS": PROXY_WS, "Core Workspace": CORE_WS, "Root WS": ROOT_WS}
DATASETS = {
    (CORE_WS, "Core Model"): CORE,
    (ROOT_WS, "Root Model"): ROOT,
}


def _response(status_code, body):
    response = requests.Response()
    response.status_code = status_code
    response._content = json.dumps(body).encode("utf-8")
    response.reason = "OK" if status_code == 200 else "Bad Request"
    response.url = "https://api.powerbi.com/v1.0/myorg/GenerateToken"
    return response


def _as_source(server, database):
    return {
        "datasourceType": "AnalysisServices",
        "connectionDetails": {"server": server, "database": database},
    }


class FakePowerBI:
    """
    Simulates GenerateToken for chained composite models: every model in the
    chain must be in the request and all must have xmlaPermissions=ReadOnly.
    """

    def __init__(self, chain, datasources=None, expressions=None):
        self.chain = chain
        self.datasources = datasources or {}
        self.expressions = expressions or {}
        self.token_payloads = []
        self.calls = []

    def base_api(self, request, method="get", payload=None, client=None, **_kwargs):
        self.calls.append(request)
        if request == "/v1.0/myorg/GenerateToken":
            self.token_payloads.append(payload)
            datasets = payload["datasets"]
            ids = {d["id"] for d in datasets}
            all_read_only = all(
                d.get("xmlaPermissions") == "ReadOnly" for d in datasets
            )
            if len(self.chain) > 1 and not all_read_only:
                raise FabricHTTPException(
                    _response(
                        400,
                        {
                            "error": {
                                "message": "Cannot connect to dataset with embed token because XMLA permissions are off."
                            }
                        },
                    )
                )
            if not set(self.chain).issubset(ids):
                raise FabricHTTPException(
                    _response(
                        400,
                        {
                            "error": {
                                "message": "Generate token request for proxy model should include the core model ID"
                            }
                        },
                    )
                )
            return _response(200, {"token": "embed-token"})
        if request.endswith("/datasources"):
            dataset_id = request.split("/")[-2]
            return _response(200, {"value": self.datasources.get(dataset_id, [])})
        raise AssertionError(f"Unexpected request: {request}")

    def list_expressions(self, dataset, workspace=None, **_kwargs):
        return pd.DataFrame(
            {"Expression": self.expressions.get((workspace, dataset), [])}
        )


def _resolve_workspace_id(workspace):
    if workspace in WORKSPACES.values():
        return workspace
    if workspace in WORKSPACES:
        return WORKSPACES[workspace]
    raise ValueError(f"Workspace '{workspace}' not found")


def _resolve_dataset_id(dataset, workspace=None):
    return DATASETS[(workspace, dataset)]


@pytest.fixture
def install(monkeypatch):
    def _install(fake):
        monkeypatch.setattr(embed, "_base_api", fake.base_api)
        monkeypatch.setattr(embed.fabric, "list_expressions", fake.list_expressions)
        monkeypatch.setattr(embed, "resolve_workspace_id", _resolve_workspace_id)
        monkeypatch.setattr(embed, "resolve_dataset_id", _resolve_dataset_id)
        return fake

    return _install


def test_generate_embed_token_regular_model_sends_original_request(install):
    fake = install(FakePowerBI(chain=[PROXY]))

    token = embed.generate_embed_token(dataset_ids=[PROXY], report_ids=[REPORT])

    assert token == "embed-token"
    assert fake.calls == ["/v1.0/myorg/GenerateToken"]
    assert fake.token_payloads == [
        {"datasets": [{"id": PROXY}], "reports": [{"id": REPORT, "allowEdit": True}]}
    ]


def test_generate_embed_token_includes_upstream_models_with_read_only_xmla(install):
    fake = install(
        FakePowerBI(
            chain=[PROXY, CORE],
            datasources={
                PROXY: [
                    {
                        "datasourceType": "Sql",
                        "connectionDetails": {"server": "s", "database": "d"},
                    },
                    _as_source(
                        "powerbi://api.powerbi.com/v1.0/myorg/Core%20Workspace",
                        "Core Model",
                    ),
                ]
            },
        )
    )

    token = embed.generate_embed_token(
        dataset_ids=[PROXY], report_ids=[REPORT], workspace=PROXY_WS
    )

    assert token == "embed-token"
    assert fake.token_payloads[-1]["datasets"] == [
        {"id": PROXY, "xmlaPermissions": "ReadOnly"},
        {"id": CORE, "xmlaPermissions": "ReadOnly"},
    ]


def test_generate_embed_token_follows_multi_level_chain(install):
    fake = install(
        FakePowerBI(
            chain=[PROXY, CORE, ROOT],
            datasources={
                PROXY: [
                    _as_source(
                        "powerbi://api.powerbi.com/v1.0/myorg/Core Workspace",
                        "Core Model",
                    )
                ],
                CORE: [
                    _as_source(
                        "powerbi://api.powerbi.com/v1.0/myorg/Root WS", "Root Model"
                    ),
                    # Cycles back to an already-seen model must not loop.
                    _as_source(
                        "pbiazure://api.powerbi.com", f"sobe_wowvirtualserver-{PROXY}"
                    ),
                ],
            },
        )
    )

    token = embed.generate_embed_token(dataset_ids=PROXY, report_ids=REPORT)

    assert token == "embed-token"
    assert [d["id"] for d in fake.token_payloads[-1]["datasets"]] == [PROXY, CORE, ROOT]


def test_generate_embed_token_falls_back_to_model_expressions(install):
    expression = (
        "let\n"
        '    Source = AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/Core Workspace", "Core Model"),\n'
        "    Cubes = Table.Combine(Source[Data])\n"
        "in\n"
        "    Cubes"
    )
    fake = install(
        FakePowerBI(
            chain=[PROXY, CORE], expressions={(PROXY_WS, PROXY): [expression, None]}
        )
    )

    token = embed.generate_embed_token(
        dataset_ids=[PROXY], report_ids=[REPORT], workspace="Proxy WS"
    )

    assert token == "embed-token"
    assert [d["id"] for d in fake.token_payloads[-1]["datasets"]] == [PROXY, CORE]


def test_generate_embed_token_warns_and_falls_back_when_datasources_fail(
    install, capsys
):
    expression = 'AnalysisServices.Database("powerbi://api.powerbi.com/v1.0/myorg/Core Workspace", "Core Model")'
    fake = install(
        FakePowerBI(chain=[PROXY, CORE], expressions={(PROXY_WS, PROXY): [expression]})
    )
    token_api = fake.base_api

    def base_api(request, **kwargs):
        if request.endswith("/datasources"):
            raise FabricHTTPException(_response(404, {"error": {"message": "x"}}))
        return token_api(request, **kwargs)

    fake.base_api = base_api
    install(fake)

    token = embed.generate_embed_token(
        dataset_ids=[PROXY], report_ids=[REPORT], workspace=PROXY_WS
    )

    assert token == "embed-token"
    assert [d["id"] for d in fake.token_payloads[-1]["datasets"]] == [PROXY, CORE]
    out = capsys.readouterr().out
    assert "Unable to retrieve the data sources" in out
    assert PROXY in out


def test_generate_embed_token_warns_when_expressions_fail(install, capsys):
    fake = install(FakePowerBI(chain=[PROXY, CORE]))

    def failing_list_expressions(**_kwargs):
        raise RuntimeError("XMLA endpoint unavailable")

    fake.list_expressions = failing_list_expressions
    install(fake)

    with pytest.raises(FabricHTTPException):
        embed.generate_embed_token(
            dataset_ids=[PROXY], report_ids=[REPORT], workspace=PROXY_WS
        )

    out = capsys.readouterr().out
    assert "Unable to read the expressions" in out
    assert PROXY_WS in out


def test_resolve_power_bi_source_propagates_unexpected_errors(monkeypatch):
    def broken(_workspace):
        raise RuntimeError("unexpected")

    monkeypatch.setattr(embed, "resolve_workspace_id", broken)

    with pytest.raises(RuntimeError, match="unexpected"):
        embed._resolve_power_bi_source(
            "powerbi://api.powerbi.com/v1.0/myorg/Core Workspace", CORE
        )


def test_resolve_power_bi_source_ignores_unresolvable_workspace_for_id(monkeypatch):
    monkeypatch.setattr(embed, "resolve_workspace_id", _resolve_workspace_id)

    assert embed._resolve_power_bi_source(
        "powerbi://api.powerbi.com/v1.0/myorg/Missing WS", CORE
    ) == (CORE, None)


def test_generate_embed_token_reraises_with_hint_when_no_upstream_found(
    install, capsys
):
    fake = install(FakePowerBI(chain=[PROXY, CORE]))

    with pytest.raises(FabricHTTPException, match="XMLA permissions are off"):
        embed.generate_embed_token(dataset_ids=[PROXY], report_ids=[REPORT])

    assert len(fake.token_payloads) == 1
    assert "DirectQuery to other Power BI semantic models" in capsys.readouterr().out


def test_generate_embed_token_does_not_retry_non_400_errors(install, monkeypatch):
    def forbidden(request, **_kwargs):
        raise FabricHTTPException(_response(403, {"error": {"message": "Forbidden"}}))

    install(FakePowerBI(chain=[PROXY]))
    monkeypatch.setattr(embed, "_base_api", forbidden)

    with pytest.raises(FabricHTTPException) as exc_info:
        embed.generate_embed_token(dataset_ids=[PROXY], report_ids=[REPORT])

    assert exc_info.value.status_code == 403


@pytest.mark.parametrize(
    "server, database, expected",
    [
        (
            "powerbi://api.powerbi.com/v1.0/myorg/Sales%20WS",
            "Model",
            ("Sales WS", "Model"),
        ),
        (
            "powerbi://api.powerbi.com/v1.0/contoso.com/Sales WS/",
            "Model",
            ("Sales WS", "Model"),
        ),
        ("pbiazure://api.powerbi.com", f"sobe_wowvirtualserver-{CORE}", (None, CORE)),
        ("my-as-server.database.windows.net", "Model", None),
        ("asazure://westus.asazure.windows.net/srv", "Model", None),
        (None, "Model", None),
    ],
)
def test_parse_power_bi_source(server, database, expected):
    assert embed._parse_power_bi_source(server, database) == expected
