from pathlib import Path

SOURCE_PATH = (
    Path(__file__).parents[1]
    / "src"
    / "sempy_labs"
    / "semantic_model"
    / "_lineage_view.py"
)


def _source() -> str:
    return SOURCE_PATH.read_text(encoding="utf-8")


def _scan_section(source: str) -> str:
    return source[
        source.index("// ---------- Local Excel folder scan ----------") : source.index(
            "// ---------- Node width resize ----------"
        )
    ]


def test_one_excel_button_opens_a_folder_or_files_menu():
    source = _source()

    assert 'data-act="excel-menu"' in source
    assert 'data-act="scan-folder"' in source
    assert 'data-act="scan-files"' in source
    assert 'data-act="clear-excel"' in source
    assert (
        "xm.onclick = () => { excelMenuOpen = !excelMenuOpen; renderAll(); };" in source
    )
    assert (
        "xl.onclick = () => { excelMenuOpen = false; openFolderPicker(); };" in source
    )
    assert "xf.onclick = () => { excelMenuOpen = false; openFilePicker(); };" in source
    assert ".slls-lv-menuwrap { position: relative;" in source
    # The menu dismisses on Escape and on a click outside it.
    assert (
        "if (excelMenuOpen) { excelMenuOpen = false; renderAll(); return; }" in source
    )
    assert 't.closest(".slls-lv-menuwrap")' in source
    # The native folder chooser comes from a directory-enabled file input.
    assert 'input.setAttribute("webkitdirectory", "");' in source
    assert 'input.setAttribute("directory", "");' in source
    assert 'input.accept = ".xlsx,.xlsm,.xlsb,.xltx,.xltm,.xls";' in source
    assert "h.appendChild(folderInput);" in source
    assert "h.appendChild(fileInput);" in source


def test_scans_add_to_the_diagram_instead_of_replacing_it():
    scan = _scan_section(_source())

    # Everything re-scanned is refreshed, so stale entries drop off.
    assert (
        "const rescanned = new Set(candidates.map((f) => f.webkitRelativePath || f.name));"
        in scan
    )
    assert (
        "excelFiles = [...excelFiles.filter((x) => !rescanned.has(x.path)), ...matches];"
        in scan
    )
    assert 'scanExcelFiles(files, directory ? "folder" : "selection")' in scan


def test_workbooks_are_parsed_in_the_browser():
    scan = _scan_section(_source())

    # The folder is on the user's machine while the kernel is usually remote,
    # so the workbooks must never be shipped to Python.
    assert "dispatch(" not in scan
    assert "zipReadParts" in scan
    assert "0x06054b50" in scan  # end of central directory
    assert "0x02014b50" in scan  # central directory entry
    assert "0x04034b50" in scan  # local file header
    assert "0x07064b50" in scan  # zip64 locator
    assert 'DecompressionStream("deflate-raw")' in scan
    assert "xl\\/connections\\.xml" in scan


def test_only_connections_to_the_current_model_are_matched():
    scan = _scan_section(_source())

    assert 'connField(conn, "Data Source")' in scan
    assert 'connField(conn, "Initial Catalog")' in scan
    assert "powerbi:\\/\\/|pbiazure:\\/\\/|api\\.powerbi\\.com" in scan
    assert 'matchedOn: "Semantic model ID"' in scan
    assert 'matchedOn: "Model name"' in scan
    # Excel lock files are not workbooks.
    assert '!f.name.startsWith("~$")' in scan


def test_excel_files_are_rendered_as_diagram_nodes():
    source = _source()

    assert "function buildExcelNode(x)" in source
    assert "function buildExcelDetail(panel, x)" in source
    assert "excelFiles.forEach((x) => canvas.appendChild(buildExcelNode(x)));" in source
    # Workbooks reuse the report health classes, so an unanalyzed one is neutral.
    assert (
        '"slls-lv-node excel " + hs + (selectedId === x.id ? " selected" : "")'
        in source
    )
    assert "--slls-excel" not in source
    # Excel nodes orbit the model alongside the downstream reports.
    assert "const spokes = () => [" in source
    assert '...excelFiles.map((x) => ({ id: x.id, kind: "excel" })),' in source
    assert 'stat("Excel files", excelFiles.length, "")' in source
    # Workbooks are represented by the spreadsheet glyph, not a folder.
    assert '.replace("__ICON_EXCEL__", _UI_ICONS["excel"])' in source
    assert '`<div class="slls-lv-node-ws">${ICON.excel}' in source
    # A connected workbook is drawn with a solid edge, like a report.
    excel_edge = source[
        source.index("excelFiles.forEach((x) => {") : source.index(
            "canvas.appendChild(svg);"
        )
    ]
    assert "stroke-dasharray" not in excel_edge


def test_compound_file_workbooks_are_detected_by_signature():
    scan = _scan_section(_source())

    # A labelled or legacy workbook is an OLE2 file that still carries an
    # .xlsx name, so the extension alone cannot be trusted.
    assert "const OLE2_SIG = [0xd0, 0xcf, 0x11, 0xe0, 0xa1, 0xb1, 0x1a, 0xe1];" in scan
    assert "async function sniffFormat(file)" in scan
    assert 'return "opc"' in scan
    assert 'return "ole2"' in scan
    assert "xlsx|xlsm|xlsb|xltx|xltm|xls" in scan
    # OLE2 keeps its strings as UTF-16LE, so both decodings are searched.
    assert 'new TextDecoder("windows-1252").decode(buf)' in scan
    assert 'new TextDecoder("utf-16le").decode(buf)' in scan
    assert '"Workbook stream (Unicode)": text.wide,' in scan


def test_protected_workbooks_are_counted_and_listed_on_hover():
    source = _source()
    scan = _scan_section(source)

    assert 'text.wide.includes("EncryptedPackage")' in scan
    # Encrypted packages yield no connections, so they never become nodes.
    assert "unverified.push({" in scan
    assert "makeExcelNode(f, [], " not in scan
    assert "x.verified" not in source
    assert ".slls-lv-node.excel.unverified" not in source
    # They are counted in the summary bar and named in a hover card.
    assert "if (excelUnverified.length > 0) html += unverifiedStat();" in source
    assert "function unverifiedStat()" in source
    assert '<span class="slls-lv-stat-value">${excelUnverified.length}</span>' in source
    assert 'class="slls-lv-hovercard-item">${esc(u.path)}' in source
    assert (
        ".slls-lv-stat-hover:hover .slls-lv-hovercard, "
        ".slls-lv-stat-hover:focus-visible .slls-lv-hovercard { display: block; }"
        in source
    )
    assert "protected workbook${nUnverified === 1" in scan
    assert "unverified and not shown." in scan


def test_scan_notification_hides_itself():
    source = _source()
    scan = _scan_section(source)

    assert "const STATUS_HIDE_MS = 6000;" in source
    assert "function setLocalStatus(message, kind, autoHideMs)" in source
    assert "localStatusTimer = setTimeout(() => {" in source
    assert "function clearLocalStatus()" in source
    # Only the scan results expire; errors stay put.
    assert (
        'setLocalStatus(parts.join(" "), found > 0 ? "success" : "info", STATUS_HIDE_MS)'
        in scan
    )
    assert (
        'setLocalStatus(`This notebook host does not allow browsing local ${what}.`, "error")'
        in scan
    )


def test_analyze_also_checks_excel_workbooks():
    source = _source()
    scan = _scan_section(source)

    # The pivot cache's <cacheHierarchy> list mirrors the whole cube, so only
    # the fields actually placed in the workbook count as dependencies.
    assert "pivotCache\\/pivotCacheDefinition" in scan
    assert 'const re = /<cacheField[^>]*?\\sname="([^"]*)"/g;' in scan
    assert 'const re = /\\ssourceName="([^"]*)"/g;' in scan
    assert "function parseMdxName(raw)" in scan
    assert 'if (segs[0] === "Measures")' in scan
    assert 'segs.length === 2 || segs[2] === "All" || segs[1] === segs[2]' in scan
    assert 'String(raw || "").split(".&")[0]' in scan
    assert "function excelRefIsValid(ref, sets)" in scan
    # Validated in the browser against the model objects the back end publishes.
    assert "async function analyzeExcelFiles()" in scan
    assert "const objects = modelObjects();" in scan
    assert "analyzeExcelFiles();" in source
    assert "if (analyzed()) analyzeExcelFiles();" in scan
    # Legacy .xls workbooks have no OPC parts to inspect.
    assert 'x.kind !== "opc"' in scan
    assert '"Only .xlsx-format workbooks can be analyzed."' in scan
    assert "brokenExcel.forEach((x) => addRow(x, ICON.excel));" in source


def test_excel_broken_objects_offer_the_same_fix_picker():
    source = _source()
    scan = _scan_section(source)

    # Excel rows reuse the report fix row, so staging works identically.
    assert (
        "(x.invalidObjects || []).forEach((o) => bodyEl.appendChild(buildBrokenRow(x, o)));"
        in source
    )
    assert 'const isExcelId = (id) => String(id).startsWith("xl-");' in source
    assert "const excel = all.filter((f) => isExcelId(f.reportId));" in source
    assert "if (excel.length > 0) applyExcelFixes(excel);" in source
    assert (
        'if (reports.length > 0) dispatch({ action: "save_fixes", fixes: reports });'
        in source
    )
    # A back-end save must not clear the browser-side Excel fixes.
    assert "if (!isExcelId(f.reportId)) stagedFixes.delete(k);" in source


def test_excel_fixes_rewrite_the_package_without_touching_the_original():
    scan = _scan_section(_source())

    # Longest form first so [T].[C].[C] is rewritten before its [T].[C] prefix.
    assert "function mdxRenames(f)" in scan
    assert "`${oldC}.${q(f.brokenName)}`, to: `${newC}.${q(f.targetName)}`" in scan
    assert "`${oldC}.[All]`, to: `${newC}.[All]`" in scan
    assert "function rewriteWorkbookPart(partName, text, renames)" in scan
    assert "out = out.split(r.from).join(r.to);" in scan
    # Rebuilt in place: entry order and positional indices must survive.
    assert "async function zipRewrite(file, replacements)" in scan
    assert "function crc32(bytes)" in scan
    assert "0x04034b50" in scan and "0x02014b50" in scan and "0x06054b50" in scan
    assert "const flags = e.flags & 0x0800;" in scan
    # The picked file is read-only, so the result is saved as a copy.
    assert "async function saveWorkbookCopy(blob, suggestedName)" in scan
    assert "window.showSaveFilePicker" in scan
    assert '" (fixed).xlsx"' in scan
    assert "the original workbook is unchanged." in scan


def test_scanned_files_are_dropped_when_the_model_changes():
    source = _source()

    connected_handler = source[
        source.index('model.on("change:connected"') : source.index(
            'model.on("change:workspaces"'
        )
    ]
    assert connected_handler.count("excelFiles = [];") == 2


def test_dataset_id_is_synced_to_the_front_end():
    source = _source()

    assert 'dataset_id = traitlets.Unicode("").tag(sync=True)' in source
    assert "dataset_id=ds_id," in source
    assert "widget.dataset_id = ds_id" in source
    assert 'model.get("dataset_id")' in source
