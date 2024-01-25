from random import randint
from typing import Any, Dict, Optional

from utils import preferences as p


def get_se_info(x: str, se_dict: dict) -> Optional[Dict[str, Any]]:
    """Get SE info from se_info collection and add SE/Region details to se_dict."""
    se_info_result = p.se_info.find_one({"se": x})
    if se_info_result is not None:
        se_region = se_info_result["region"]
        se_record = [[se_region], [x]]
        region_numb_result = p.cwa_regions.find_one({"Region": se_region})
        if region_numb_result is not None:
            region_numb = region_numb_result["Index"]
        if region_numb in se_dict:
            # Append se_dict with se_region and se
            se_dict[region_numb][1].append(x)
        else:
            # Add se_dict with se_region and se
            se_dict[region_numb] = se_record
    return se_info_result


def add_unknown_se(x: str, full_SEs: list, se_dict: dict):
    # find x in full_SEs and get the name
    unknown_se = [y for y in full_SEs if y[1] == x]

    # Get highest se_idx from se_info collection and return it
    hi_idx: Optional[Dict[str, Any]] = p.se_info.find_one(sort=[("se_idx", -1)])
    if hi_idx is not None:
        next_se_idx = int(hi_idx["se_idx"]) + 1
    else:
        next_se_idx = randint(100000, 999999)

    # Add SE to se_info collection
    print(f"Adding SE {x} to se_info collection.")
    try:
        p.se_info.insert_one(
            {
                "se_idx": next_se_idx,
                "se": x,
                "se_name": unknown_se[0][0],
                "op": "VIP",
                "region": "VIP",
            }
        )
        print(f"SE {x} added to se_info collection.")
    except Exception as e:
        print(f"Error adding SE {x} to se_info collection.")
        print(e)
        exit(1)

    # Add x to cwa_matches collection if not already there
    try:
        if p.cwa_matches.find_one({"SE": x}) is None:
            p.cwa_matches.insert_one({"SE": x, "assignments": {}})
            print(f"SE {x} added to cwa_matches collection.")
        else:
            print(f"SE {x} already in cwa_matches collection.")
    except Exception as e:
        print(f"Error adding SE {x} to cwa_matches collection.")
        print(e)
        exit(1)

    # Add SE to se_dict
    se_info_result = get_se_info(x, se_dict)

    return se_info_result
