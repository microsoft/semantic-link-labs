import sempy.fabric as fabric
import pandas as pd
from typing import List, Optional, Union
from sempy._utils._log import log
from uuid import UUID


@log
def translate_semantic_model(
    dataset: str | UUID,
    languages: Union[str, List[str]],
    exclude_characters: Optional[str] = None,
    workspace: Optional[str | UUID] = None,
) -> pd.DataFrame:
    """
    Translates names, descriptions, display folders for all objects in a semantic model.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    languages : str, List[str]
        The language code(s) in which to translate the semantic model.
    exclude_characters : str
        A string specifying characters which will be replaced by a space in the translation text when sent to the translation service.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    pandas.DataFrame
        Shows a pandas dataframe which displays all of the translations in the semantic model.
    """

    return fabric.translate_semantic_model(
        dataset=dataset,
        languages=languages,
        exclude_characters=exclude_characters,
        workspace=workspace,
        model_readonly=False,
    )
