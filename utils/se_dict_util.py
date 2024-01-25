from threading import current_thread
from time import perf_counter, sleep
from typing import Any, Dict, List

from pymongo.errors import ConnectionFailure

from utils import preferences as p
from utils import se_info_util

se_dict: Dict[int, list] = {}


def make_se_dict(x: str, se_dict: dict) -> Any:
    # Look up the SE in the SE info collection and return the SE info.
    # get_se_info does the lookup and builds an entry in the se_dict
    se_info_result = se_info_util.get_se_info(x, se_dict)
    if se_info_result is not None:
        return se_info_result
    else:
        return None


def se_count_dict(SEs: List[str]) -> Dict[str, int]:
    # Create a dict of se:match_count
    se_assignment_count: Dict[str, int] = {}
    start_se_assignment_dict = perf_counter()
    for x in SEs:
        if current_thread().name == "MainThread":
            for _ in range(5):
                try:
                    y = p.cwa_matches.find_one({"SE": x})
                    break
                except ConnectionFailure as e:
                    print(
                        f" *** Connect error getting SE {x} from cwa_matches collection."
                    )
                    print(f" *** Sleeping for {pow(2, _)} seconds and trying again.")
                    sleep(pow(2, _))
                    print(e)
            print(
                " *** Failed attempt to connect to cwa_matches collection. Mongo is down."
            )
        else:
            y = p.cwa_matches.find_one({"SE": x})
        if y is not None:
            # count the number of assignments for x
            count_assignments = len(y["assignments"])
            # add the se and count to the dict
            se_assignment_count[x] = count_assignments
    end_se_assignment_dict = perf_counter()
    # clear temp variables: x, y, count_assignments
    del count_assignments, x, y
    print(
        f" Time to create se_assignment_count: {end_se_assignment_dict - start_se_assignment_dict:.6f} seconds."
    )
    return se_assignment_count
