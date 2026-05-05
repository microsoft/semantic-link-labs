# Prep for AI — Read/Write CustomInstructions on a Semantic Model
# Uses the same private applyChange path the Power BI portal uses.
# Based on work by Lukasz Obst (update_prep_for_ai_via_apply_change.py).

from __future__ import annotations
from sempy._utils._log import log

import json
import time
import uuid
from typing import Any, Optional
from uuid import UUID

import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_dataset_from_report,
)


FEATURE_TAG = "CopilotTooling"
READBACK_TIMEOUT_SECONDS = 60
READBACK_INTERVAL_SECONDS = 2


def _get_pbi_token() -> str:
    """Get a Power BI bearer token from the Fabric notebook environment."""
    import notebookutils

    return notebookutils.credentials.getToken("pbi")


def _pbi_headers() -> dict:
    """Standard headers for the private modeling endpoints."""
    return {
        "Accept": "application/json, text/plain, */*",
        "activityid": str(uuid.uuid4()),
        "requestid": str(uuid.uuid4()),
        "x-powerbi-hostenv": "Power BI Web App",
    }


def _get_dataset_info(workspace_id: str, item_id: str, token: str) -> dict:
    """Get dataset info including home cluster and Q&A status."""
    import requests

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{item_id}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=60,
    )
    response.raise_for_status()

    body = response.json() if response.text.strip() else {}
    home_cluster = response.headers.get("home-cluster-uri", "").rstrip("/")
    if not home_cluster:
        odata_context = str(body.get("@odata.context", "")).strip()
        if odata_context.startswith("https://"):
            parts = odata_context.split("/", 3)
            if len(parts) >= 3:
                home_cluster = "/".join(parts[:3])

    return {
        "cluster_uri": home_cluster,
        "qna_enabled": body.get("isQnAEnabled", None),
    }


def _enable_qna(workspace_id: str, item_id: str, token: str) -> None:
    """Enable Q&A on a semantic model via the Power BI REST API."""
    import requests

    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{item_id}"
    response = requests.patch(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"isQnAEnabled": True},
        timeout=60,
    )
    response.raise_for_status()


def _discover_cluster_uri(workspace_id: str, item_id: str, token: str) -> str:
    """Discover the semantic model home cluster from the Power BI dataset API."""
    info = _get_dataset_info(workspace_id, item_id, token)
    cluster = info.get("cluster_uri", "")
    if not cluster:
        raise RuntimeError(
            "Could not discover the home cluster for this semantic model."
        )
    return cluster


def _get_baseline_version(
    cluster_uri: str, item_id: str, culture: str, token: str
) -> str:
    """GET /modeling/getModel/{itemId} to retrieve the current baselineVersion."""
    import requests

    url = f"{cluster_uri}/modeling/getModel/{item_id}?languageLocale={culture}"
    headers = {"Authorization": f"Bearer {token}", **_pbi_headers()}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    model = resp.json()
    for key in ("baselineVersion", "version", "Version"):
        if key in model:
            return str(model[key])
    raise RuntimeError("Could not find baselineVersion in getModel response.")


def _get_linguistic_schema(
    cluster_uri: str,
    workspace_id: str,
    item_id: str,
    token: str,
) -> tuple[dict[str, Any], bool]:
    """Read the linguistic schema (LSDL document) from the Copilot Tooling endpoint."""
    import requests

    url = (
        f"{cluster_uri}/explore/nl2nl/copilotTooling/workspaces/{workspace_id}"
        f"/dataset/{item_id}/linguisticSchema"
    )
    headers = {"Authorization": f"Bearer {token}", **_pbi_headers()}
    resp = requests.get(url, headers=headers, timeout=120)
    resp.raise_for_status()

    payload = resp.json()
    lsdl_document = payload.get("lsdlDocument")
    if not isinstance(lsdl_document, dict):
        raise RuntimeError(
            "The linguisticSchema endpoint did not return an lsdlDocument payload."
        )
    return lsdl_document, bool(payload.get("isLsdlDocumentStale", False))


def _submit_apply_change(
    cluster_uri: str,
    item_id: str,
    culture: str,
    body: dict[str, Any],
    token: str,
):
    """POST /modeling/applyChange/{itemId} to commit linguistic schema changes."""
    import requests

    url = (
        f"{cluster_uri}/modeling/applyChange/{item_id}"
        f"?languageLocale={culture}&updateSchemaPackageClientConfigurableOptions=1"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;charset=UTF-8",
        **_pbi_headers(),
    }
    return requests.post(url, headers=headers, json=body, timeout=120)


# ── Public API ──────────────────────────────────────────────────────


def enable_qna(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> None:
    """Enable Q&A on a semantic model."""
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    from sempy_labs._helper_functions import resolve_item_name_and_id

    _, item_id = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    token = _get_pbi_token()
    _enable_qna(workspace_id, str(item_id), token)


def read_prep_for_ai(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> dict[str, Any]:
    """
    Read the Prep for AI configuration (CustomInstructions + VerifiedAnswers)
    from a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, optional
        Workspace name or ID.

    Returns
    -------
    dict
        Keys: custom_instructions (str), verified_answers (list), is_stale (bool)
    """
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    # Resolve dataset name to ID
    from sempy_labs._helper_functions import resolve_item_name_and_id

    _, item_id = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    item_id = str(item_id)

    token = _get_pbi_token()
    ds_info = _get_dataset_info(workspace_id, item_id, token)
    cluster_uri = ds_info.get("cluster_uri", "")
    if not cluster_uri:
        raise RuntimeError("Could not discover the home cluster for this semantic model.")
    qna_enabled = ds_info.get("qna_enabled")

    try:
        lsdl, is_stale = _get_linguistic_schema(cluster_uri, workspace_id, item_id, token)
    except Exception as e:
        # Linguistic schema may fail (e.g. 500) but Q&A info is still useful
        return {
            "custom_instructions": "",
            "verified_answers": [],
            "is_stale": False,
            "qna_enabled": qna_enabled,
            "error": f"Could not read linguistic schema: {e}",
        }

    return {
        "custom_instructions": str(lsdl.get("CustomInstructions", "")).strip(),
        "verified_answers": lsdl.get("VerifiedAnswers", []),
        "is_stale": is_stale,
        "qna_enabled": qna_enabled,
    }


def write_prep_for_ai(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    instructions: str = "",
    append: bool = False,
    culture: str = "en-US",
) -> None:
    """
    Write CustomInstructions to a semantic model's Prep for AI configuration.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, optional
        Workspace name or ID.
    instructions : str
        The new instructions text.
    append : bool, default=False
        If True, append to existing instructions; otherwise replace.
    culture : str, default="en-US"
        Culture/locale for the modeling endpoint.
    """
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    from sempy_labs._helper_functions import resolve_item_name_and_id

    _, item_id = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    item_id = str(item_id)

    token = _get_pbi_token()
    cluster_uri = _discover_cluster_uri(workspace_id, item_id, token)

    print(f"{icons.in_progress} Reading current linguistic schema…")
    lsdl, is_stale = _get_linguistic_schema(cluster_uri, workspace_id, item_id, token)

    baseline_version = _get_baseline_version(cluster_uri, item_id, culture, token)

    current = str(lsdl.get("CustomInstructions", "")).strip()
    if append and current:
        updated = current + "\n\n" + instructions.strip()
    else:
        updated = instructions.strip()

    lsdl["CustomInstructions"] = updated

    body = {
        "baselineVersion": baseline_version,
        "modelChange": {
            "changes": [
                {
                    "updateLinguisticMetadata": {
                        "linguisticSchemaJson": json.dumps(
                            lsdl, ensure_ascii=False, separators=(",", ":")
                        )
                    }
                },
                {
                    "SetUsedFeatureModelTraitSchemaChange": {
                        "featureTagsToAdd": [FEATURE_TAG]
                    }
                },
            ]
        },
        "validate": False,
        "allowAsyncCommit": True,
        "rollBackChanges": False,
        "clientContext": {"feature": "copilotTooling"},
    }

    print(f"{icons.in_progress} Submitting Prep for AI update…")
    resp = _submit_apply_change(cluster_uri, item_id, culture, body, token)
    resp.raise_for_status()

    # Poll for readback
    deadline = time.monotonic() + READBACK_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        time.sleep(READBACK_INTERVAL_SECONDS)
        lsdl_check, stale = _get_linguistic_schema(
            cluster_uri, workspace_id, item_id, token
        )
        readback = str(lsdl_check.get("CustomInstructions", "")).strip()
        if readback == updated and not stale:
            print(f"{icons.green_dot} Prep for AI instructions updated successfully.")
            return

    print(
        f"{icons.yellow_dot} Update submitted but readback not confirmed within "
        f"{READBACK_TIMEOUT_SECONDS}s. Check the model manually."
    )


def generate_prep_for_ai_text(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
) -> str:
    """
    Auto-generate Prep for AI instructions by analyzing the semantic model
    structure (tables, columns, measures, relationships).

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, optional
        Workspace name or ID.

    Returns
    -------
    str
        Generated CustomInstructions text.
    """
    from sempy_labs.tom import connect_semantic_model

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)

    lines = []

    with connect_semantic_model(dataset=dataset, readonly=True, workspace=workspace_id) as tom:
        model_name = str(tom.model.Name) if tom.model.Name else str(dataset)
        lines.append(f"# Semantic Model: {model_name}")
        lines.append("")

        # ── Classify tables ──
        fact_tables = []
        dim_tables = []
        calc_tables = []
        date_table = None
        measure_table = None

        for t in tom.model.Tables:
            t_name = str(t.Name)
            # Detect date table
            if t.DataCategory == "Time":
                date_table = t_name
                dim_tables.append(t_name)
                continue
            # Detect calculation groups
            try:
                if t.CalculationGroup is not None:
                    calc_tables.append(t_name)
                    continue
            except AttributeError:
                pass
            # Detect measure-only tables (no columns besides RowNumber)
            real_cols = [c for c in t.Columns if not str(c.Name).startswith('RowNumber-')]
            has_measures = any(True for _ in t.Measures)
            if len(real_cols) == 0 and has_measures:
                measure_table = t_name
                continue
            # Heuristic: fact tables typically have many numeric columns and relationships as "from"
            from_rels = sum(1 for r in tom.model.Relationships if str(r.FromTable.Name) == t_name)
            to_rels = sum(1 for r in tom.model.Relationships if str(r.ToTable.Name) == t_name)
            if from_rels > to_rels and from_rels > 0:
                fact_tables.append(t_name)
            elif to_rels > 0:
                dim_tables.append(t_name)
            else:
                # No relationships — classify by column count
                if len(real_cols) > 10:
                    fact_tables.append(t_name)
                else:
                    dim_tables.append(t_name)

        # ── Model overview ──
        lines.append("## Model Structure")
        if fact_tables:
            lines.append(f"- **Fact tables** (transactional data): {', '.join(fact_tables)}")
        if dim_tables:
            lines.append(f"- **Dimension tables** (lookup/reference): {', '.join(dim_tables)}")
        if date_table:
            lines.append(f"- **Date table**: {date_table}")
        if measure_table:
            lines.append(f"- **Measure table**: {measure_table}")
        if calc_tables:
            lines.append(f"- **Calculation groups**: {', '.join(calc_tables)}")
        lines.append("")

        # ── Relationships (star schema) ──
        rels = []
        for r in tom.model.Relationships:
            from_t = str(r.FromTable.Name)
            from_c = str(r.FromColumn.Name)
            to_t = str(r.ToTable.Name)
            to_c = str(r.ToColumn.Name)
            active = "active" if r.IsActive else "inactive"
            cross = str(r.CrossFilteringBehavior)
            rels.append(f"- '{from_t}'[{from_c}] → '{to_t}'[{to_c}] ({active}, {cross})")
        if rels:
            lines.append("## Relationships")
            lines.extend(rels)
            lines.append("")

        # ── Measures ──
        measures_info = []
        for m in tom.all_measures():
            m_name = str(m.Name)
            m_desc = str(m.Description).strip() if m.Description else ""
            m_expr = str(m.Expression).strip() if m.Expression else ""
            m_table = str(m.Parent.Name)
            m_folder = str(m.DisplayFolder).strip() if m.DisplayFolder else ""

            info = f"- **{m_name}**"
            if m_folder:
                info += f" (folder: {m_folder})"
            if m_desc:
                info += f": {m_desc}"
            elif m_expr:
                # Summarize short expressions
                short = m_expr.replace("\n", " ").replace("\r", "").strip()
                if len(short) <= 120:
                    info += f" = `{short}`"
                else:
                    # Extract the top-level function
                    first_line = m_expr.split("\n")[0].strip()
                    if len(first_line) <= 80:
                        info += f" = `{first_line} ...`"
            measures_info.append(info)

        if measures_info:
            lines.append(f"## Measures ({len(measures_info)} total)")
            lines.extend(measures_info)
            lines.append("")

        # ── Key columns with descriptions ──
        described_cols = []
        for t in tom.model.Tables:
            t_name = str(t.Name)
            for c in t.Columns:
                c_name = str(c.Name)
                if c_name.startswith('RowNumber-'):
                    continue
                desc = str(c.Description).strip() if c.Description else ""
                if desc:
                    described_cols.append(f"- '{t_name}'[{c_name}]: {desc}")
        if described_cols:
            lines.append("## Column Descriptions")
            lines.extend(described_cols)
            lines.append("")

        # ── Data categories ──
        categorized = []
        for t in tom.model.Tables:
            for c in t.Columns:
                cat = str(c.DataCategory).strip() if c.DataCategory else ""
                if cat and cat not in ("Uncategorized", "Regular"):
                    categorized.append(f"- '{t.Name}'[{c.Name}]: {cat}")
        if categorized:
            lines.append("## Data Categories")
            lines.extend(categorized)
            lines.append("")

        # ── Hierarchies ──
        hierarchies = []
        for t in tom.model.Tables:
            for h in t.Hierarchies:
                levels = [str(lev.Name) for lev in h.Levels]
                hierarchies.append(f"- '{t.Name}'.{h.Name}: {' → '.join(levels)}")
        if hierarchies:
            lines.append("## Hierarchies")
            lines.extend(hierarchies)
            lines.append("")

        # ── Usage guidance ──
        lines.append("## Usage Guidance")
        lines.append("- When users ask about totals or aggregations, use the pre-defined measures rather than summing columns directly.")
        if date_table:
            lines.append(f"- Time-based analysis should use the '{date_table}' table for filtering.")
        if calc_tables:
            lines.append(f"- Calculation groups ({', '.join(calc_tables)}) modify measure behavior — apply them as filters, not as values.")
        lines.append("- Respect the star schema: filter from dimensions to facts, not the reverse.")
        lines.append("")

    return "\n".join(lines)


@log
def add_prep_for_ai(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> None:
    """
    Auto-generate and write Prep for AI instructions for a semantic model.
    In scan_only mode, shows what would be generated without writing.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, optional
        Workspace name or ID.
    scan_only : bool, default=False
        If True, only shows the generated text without writing.
    """
    # Check current state first
    try:
        result = read_prep_for_ai(dataset=dataset, workspace=workspace)
    except Exception as e:
        print(f"{icons.yellow_dot} Could not read Prep for AI: {e}")
        return

    current = result["custom_instructions"]
    verified = result["verified_answers"]
    va_count = len(verified) if isinstance(verified, list) else 0
    qna_enabled = result.get("qna_enabled")

    # Q&A check
    if qna_enabled is False:
        print(
            f"{icons.yellow_dot} Q&A is disabled on this semantic model. "
            f"Enable it in Power BI Service → Settings → Q&A to use Prep for AI."
        )
    elif qna_enabled is True:
        print(f"{icons.green_dot} Q&A is enabled on this semantic model.")

    if current and len(current) >= 50:
        print(
            f"{icons.green_dot} Prep for AI: CustomInstructions already configured "
            f"({len(current)} chars). Skipping auto-generation."
        )
        if va_count > 0:
            print(f"{icons.green_dot} Prep for AI: {va_count} verified answer(s) configured.")
        return

    # Generate
    print(f"{icons.in_progress} Generating Prep for AI instructions from model structure…")
    try:
        text = generate_prep_for_ai_text(dataset=dataset, workspace=workspace)
    except Exception as e:
        print(f"{icons.yellow_dot} Could not generate Prep for AI text: {e}")
        return

    if scan_only:
        print(
            f"{icons.yellow_dot} Prep for AI: CustomInstructions are "
            f"{'empty' if not current else f'short ({len(current)} chars)'}. "
            f"Would auto-generate {len(text)} chars."
        )
        if va_count == 0:
            print(f"{icons.info} Prep for AI: No verified answers configured.")
        return

    # Write
    try:
        write_prep_for_ai(dataset=dataset, workspace=workspace, instructions=text)
    except Exception as e:
        print(f"{icons.yellow_dot} Could not write Prep for AI: {e}")
        return

    print(f"{icons.green_dot} Prep for AI: Auto-generated {len(text)} chars from model structure.")
