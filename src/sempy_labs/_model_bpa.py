import pandas as pd
from typing import Optional
from sempy._utils._log import log
from uuid import UUID


@log
def run_model_bpa(
    dataset: str | UUID,
    rules: Optional[pd.DataFrame] = None,
    workspace: Optional[str | UUID] = None,
    export: bool = False,
    return_dataframe: bool = False,
    extended: bool = False,
    language: Optional[str] = None,
    check_dependencies: bool = True,
    **kwargs,
):
    """
    Runs the Best Practice Analyzer on a semantic model and displays the interactive analyzer with its results.

    This function opens the semantic model Best Practice Analyzer (``sempy_labs.semantic_model._bpa.bpa``) on the
    semantic model, which groups the rule violations by rule and lets you preview and apply the automatic fix for
    the rules which support one.

    The Best Practice Analyzer rules are based on the rules defined `here <https://github.com/microsoft/Analysis-Services/tree/master/BestPracticeRules>`_. The framework for the Best Practice Analyzer and rules are based on the foundation set by `Tabular Editor <https://github.com/TabularEditor/TabularEditor>`_.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    rules : pandas.DataFrame, default=None
        A pandas dataframe containing rules to be evaluated, in the format of the dataframe produced by the
        :func:`sempy_labs.model_bpa_rules` function.
        Defaults to None which uses the built-in rules.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    export : bool, default=False
        If True, appends the rule violations to the 'modelbparesults' delta table in the lakehouse attached to the
        notebook instead of displaying the analyzer. The exported rows also carry the Capacity Name, Capacity Id,
        Workspace Name, Workspace Id, Dataset Name, Dataset Id, Configured By, Timestamp and RunId columns.
    return_dataframe : bool, default=False
        If True, returns the rule violations as a pandas dataframe instead of displaying the analyzer.
        If 'export' is also True, the violations are exported as well.
    extended : bool, default=False
        If True, runs the set_vertipaq_annotations function to collect Vertipaq Analyzer statistics to be used in the
        analysis of the semantic model (for example by the 'Large tables should be partitioned' rule). The statistics
        are not saved to the semantic model.
    language : str, default=None
        Specifying a language name or code (i.e. 'it-IT' for Italian) will auto-translate the Category, Rule Name and Description into the specified language.
        Defaults to None which resolves to English.
    check_dependencies : bool, default=True
        If True, leverages the model dependencies from get_model_calc_dependencies to evaluate the rules. Set this parameter to False if running the rules against a semantic model in a shared capacity.

    Returns
    -------
    pandas.DataFrame | None
        If 'return_dataframe' is True, a pandas dataframe of the rule violations with the Category, Rule Name,
        Severity, Object Type, Object Name, Description and URL columns. Severity is given as text: 'Error',
        'Warning' or 'Info'. Otherwise None.
    """

    from sempy_labs.semantic_model._bpa import bpa

    if "extend" in kwargs:
        print(
            "The 'extend' parameter has been deprecated. Please remove this parameter from the function going forward."
        )
        del kwargs["extend"]

    return bpa(
        dataset=dataset,
        workspace=workspace,
        rules=rules,
        check_dependencies=check_dependencies,
        return_dataframe=return_dataframe,
        export=export,
        language=language,
        extended=extended,
    )
