import re
import json
import difflib
from collections import defaultdict


def color_text(text, color_code):
    return f"\033[{color_code}m{text}\033[0m"


def stringify(payload):
    try:
        if isinstance(payload, list):
            return (
                "[\n" + ",\n".join(f"  {json.dumps(item)}" for item in payload) + "\n]"
            )
        return json.dumps(payload, indent=2, sort_keys=True)
    except Exception:
        return str(payload)


def extract_top_level_group(path):
    # For something like: resourcePackages[1].items[1].name → resourcePackages[1].items[1]
    segments = re.split(r"\.(?![^[]*\])", path)  # split on dots not in brackets
    return ".".join(segments[:-1]) if len(segments) > 1 else segments[0]


def get_by_path(obj, path):
    """Navigate into nested dict/list based on a dot/bracket path like: a.b[1].c"""
    tokens = re.findall(r"\w+|\[\d+\]", path)
    for token in tokens:
        if token.startswith("["):
            index = int(token[1:-1])
            obj = obj[index]
        else:
            obj = obj.get(token)
    return obj


def deep_diff(d1, d2, path=""):
    diffs = []
    if isinstance(d1, dict) and isinstance(d2, dict):
        keys = set(d1) | set(d2)
        for key in sorted(keys):
            new_path = f"{path}.{key}" if path else key
            if key not in d1:
                diffs.append(("+", new_path, None, d2[key]))
            elif key not in d2:
                diffs.append(("-", new_path, d1[key], None))
            else:
                diffs.extend(deep_diff(d1[key], d2[key], new_path))
    elif isinstance(d1, list) and isinstance(d2, list):
        min_len = min(len(d1), len(d2))
        list_changed = False
        for i in range(min_len):
            if d1[i] != d2[i]:
                list_changed = True
                break
        if list_changed or len(d1) != len(d2):
            diffs.append(("~", path, d1, d2))
    elif d1 != d2:
        diffs.append(("~", path, d1, d2))
    return diffs


def diff_parts(d1, d2):

    def build_path_map(parts):
        return {part["path"]: part["payload"] for part in parts}

    try:
        paths1 = build_path_map(d1)
    except Exception:
        paths1 = d1
    try:
        paths2 = build_path_map(d2)
    except Exception:
        paths2 = d2
    all_paths = set(paths1) | set(paths2)

    for part_path in sorted(all_paths):
        p1 = paths1.get(part_path)
        p2 = paths2.get(part_path)

        if p1 is None:
            print(color_text(f"+ {part_path}", "32"))  # Green
            continue
        elif p2 is None:
            print(color_text(f"- {part_path}", "31"))  # Red
            continue
        elif p1 == p2:
            continue

        if p1 is None or p2 is None:
            print(
                color_text(f"+ {part_path}", "32")
                if p2 and not p1
                else color_text(f"- {part_path}", "31")
            )
            continue

        # Header for the changed part
        print(color_text(f"~ {part_path}", "33"))

        # Collect diffs
        diffs = deep_diff(p1, p2)
        # If the diff is only a change of a whole list (like appending to a list), group it under its key
        merged_list_diffs = []
        for change_type, full_path, old_val, new_val in diffs:
            if (
                change_type == "~"
                and isinstance(old_val, list)
                and isinstance(new_val, list)
            ):
                merged_list_diffs.append((change_type, full_path, old_val, new_val))

        # Replace individual item diffs with unified list diff
        if merged_list_diffs:
            diffs = merged_list_diffs

        # Group diffs by common parent path (e.g. items[1])
        grouped = defaultdict(list)
        for change_type, full_path, old_val, new_val in diffs:
            group_path = extract_top_level_group(full_path)
            grouped[group_path].append((change_type, full_path, old_val, new_val))

        # Print each group once with unified diff for the full substructure
        for group_path in sorted(grouped):
            print("  " + color_text(f"~ {group_path}", "33"))

            try:
                old_group = get_by_path(p1, group_path)
                new_group = get_by_path(p2, group_path)
            except Exception:
                old_group = new_group = None

            # Skip showing diffs for empty/null groups
            if isinstance(old_group, dict) and isinstance(new_group, dict):
                old_keys = set(old_group.keys())
                new_keys = set(new_group.keys())

                for key in sorted(old_keys - new_keys):
                    print(
                        "  "
                        + color_text(f"- {key}: {json.dumps(old_group[key])}", "31")
                    )
                for key in sorted(new_keys - old_keys):
                    print(
                        "  "
                        + color_text(f"+ {key}: {json.dumps(new_group[key])}", "32")
                    )
                for key in sorted(old_keys & new_keys):
                    if old_group[key] != new_group[key]:
                        print("  " + color_text(f"~ {key}:", "33"))
                        old_val_str = stringify(old_group[key]).splitlines()
                        new_val_str = stringify(new_group[key]).splitlines()
                        for line in difflib.unified_diff(
                            old_val_str,
                            new_val_str,
                            fromfile="old",
                            tofile="new",
                            lineterm="",
                        ):
                            if line.startswith("@@"):
                                print("    " + color_text(line, "36"))
                            elif line.startswith("-") and not line.startswith("---"):
                                print("    " + color_text(line, "31"))
                            elif line.startswith("+") and not line.startswith("+++"):
                                print("    " + color_text(line, "32"))
            elif old_group is None and new_group is not None:
                if isinstance(new_group, dict):
                    # print all added keys
                    for key, val in new_group.items():
                        print("  " + color_text(f"+ {key}: {json.dumps(val)}", "32"))
                elif isinstance(new_group, list):
                    old_str = []
                    new_str = stringify(new_group).splitlines()
                    for line in difflib.unified_diff(
                        old_str, new_str, fromfile="old", tofile="new", lineterm=""
                    ):
                        if line.startswith("@@"):
                            print("  " + color_text(line, "36"))
                        elif line.startswith("-") and not line.startswith("---"):
                            print("  " + color_text(line, "31"))
                        elif line.startswith("+") and not line.startswith("+++"):
                            print("  " + color_text(line, "32"))
                else:
                    print("  " + color_text(f"+ {json.dumps(new_group)}", "32"))

            elif new_group is None and old_group is not None:
                if isinstance(old_group, dict):
                    # print all removed keys
                    for key, val in old_group.items():
                        print("  " + color_text(f"- {key}: {json.dumps(val)}", "31"))
                elif isinstance(old_group, list):
                    old_str = stringify(old_group).splitlines()
                    new_str = []
                    for line in difflib.unified_diff(
                        old_str, new_str, fromfile="old", tofile="new", lineterm=""
                    ):
                        if line.startswith("@@"):
                            print("  " + color_text(line, "36"))
                        elif line.startswith("-") and not line.startswith("---"):
                            print("  " + color_text(line, "31"))
                        elif line.startswith("+") and not line.startswith("+++"):
                            print("  " + color_text(line, "32"))
                else:
                    print("  " + color_text(f"- {json.dumps(old_group)}", "31"))
            else:
                old_str = stringify(old_group).splitlines()
                new_str = stringify(new_group).splitlines()

                for line in difflib.unified_diff(
                    old_str, new_str, fromfile="old", tofile="new", lineterm=""
                ):
                    if line.startswith("@@"):
                        print("  " + color_text(line, "36"))
                    elif line.startswith("-") and not line.startswith("---"):
                        print("  " + color_text(line, "31"))
                    elif line.startswith("+") and not line.startswith("+++"):
                        print("  " + color_text(line, "32"))
