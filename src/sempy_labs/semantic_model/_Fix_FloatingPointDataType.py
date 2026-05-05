# Fix floating point data types — standalone BPA fixer.
# Changes columns using Double data type to Decimal.

from typing import Optional
from uuid import UUID
from sempy._utils._log import log


@log
def fix_floating_point_datatype(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
    scan_only: bool = False,
) -> int:
    """
    Fixes columns that use floating point (Double) data types by changing them to Decimal.

    Parameters
    ----------
    dataset : str | UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
    scan_only : bool, default=False
        If True, only reports what would be fixed without making changes.

    Returns
    -------
    int
        Number of items fixed.
    """
    from sempy_labs.tom import connect_semantic_model

    fixed = 0
    with connect_semantic_model(dataset=dataset, readonly=scan_only, workspace=workspace) as tom:
        import Microsoft.AnalysisServices.Tabular as TOM

        for table in tom.model.Tables:
            for col in table.Columns:
                if col.DataType == TOM.DataType.Double:
                    if scan_only:
                        print(f"  Would fix: '{table.Name}'[{col.Name}] Double → Decimal")
                    else:
                        col.DataType = TOM.DataType.Decimal
                        print(f"  Fixed: '{table.Name}'[{col.Name}] Double → Decimal")
                    fixed += 1

    action = "Would fix" if scan_only else "Fixed"
    print(f"  {action} {fixed} floating point column(s).")
    return fixed
