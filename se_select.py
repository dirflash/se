import inspect
import random
import sys
from datetime import datetime
from functools import wraps
from statistics import median_high
from time import perf_counter
from typing import Any, Dict, Optional

import numpy as np
from pymongo.errors import ConnectionFailure

from utils import preferences as p

regions = []
selected_pairings = []
running_count = {}
kobayashi_counter = 0

"""
# Setup Mongo connection
Mongo_Connect_URI: MongoClient = MongoClient(
    "mongodb+srv://fuse-test:VCxzQKyPFaohvoK1@cluster0.jzvod.mongodb.net"
    "/fuse-test?retryWrites=true&w=majority",
    tlsCAFile=certifi.where(),
    serverSelectionTimeoutMS=500,
)
"""

cwa_matches = p.cwa_matches  # Mongo_Connect_URI["fuse-test"]["cwa_matches"]
cwa_regions = p.cwa_regions  # Mongo_Connect_URI["fuse-test"]["cwa_regions"]
se_info = p.se_info  # Mongo_Connect_URI["fuse-test"]["cwa_SEs"]


def timeit(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = perf_counter()
        result = func(*args, **kwargs)
        end = perf_counter()
        if end - start > 1:
            print(f"{func.__name__} took {end - start:.6f} seconds to complete")
        return result

    return wrapper


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


@timeit
def create_se_dict() -> Dict[int, list]:
    # Create a dict with Region index as key and a list of SE's as value
    se_dict: Dict[int, list] = {}
    for se, region in SEs_region.items():
        try:
            region_index = region_dict[region]
        except KeyError:
            print(f"Unable to find {region} for {se}. Add to 0.")
            region_index = 0
        # append the se to the list of ses in the region
        se_dict.setdefault(region_index, []).append(se)
    # order the dict by key
    se_dict = dict(sorted(se_dict.items()))
    return se_dict


def SEs_list() -> list:
    # Create a list of all SEs to schedule from MongoDB with a record of schedule = True
    SEs = []
    try:
        start = perf_counter()
        for se in se_info.find({"se": {"$exists": True}, "schedule": True}):
            SEs.append(se["se"])
        end = perf_counter()
        se_list_time = end - start
        if se_list_time > 1:
            print(f"SEs list created in {se_list_time:.6f} seconds. (line {lineno()}))")
    except (RuntimeError, ConnectionFailure, TimeoutError) as mongo_connection_error:
        print(f"Error: {mongo_connection_error}.")
    return SEs


def jamarsh_reset() -> None:
    # Set jamarsh to schedule = False
    se_info.update_one(
        {"se": "jamarsh"},
        {"$set": {"schedule": False}},
    )
    jamarsh_available = se_info.find_one({"se": "jamarsh"})
    if jamarsh_available is not None:
        if jamarsh_available["schedule"] is False:
            print("Jim Marsh is reset to not available.")
    return None


setup_time_start = perf_counter()

jamarsh_reset()

# Create a list of all SEs to be scheduled
# SEs = SEs_list()

# Add the second column of each line from se_select.csv to SEs list
SEs = []
with open("se_select_01.csv", "r") as f:
    for line in f:
        SEs.append(line.split(",")[1].strip())

# remove the header from the list
SEs.pop(0)

# remove leading and trailing spaces from each item in SEs
SEs = [x.strip() for x in SEs]

count = len(SEs)

if count % 2 == 0:
    print("There are an even number of SEs.")
else:
    print("There are an odd number of SEs.")
    print("Jim Marsh will be added to available SEs.")
    # update schedule to True for jamarsh
    se_info.update_one(
        {"se": "jamarsh"},
        {"$set": {"schedule": True}},
    )
    jamarsh_available = se_info.find_one({"se": "jamarsh"})
    if jamarsh_available is not None:
        if jamarsh_available["schedule"] is True:
            print("Jim Marsh is available.")
            SEs = SEs_list()

# reset count of SEs
count = len(SEs)

# total count of SEs
total_se_count = count

# Create a dict of all SEs and their Region
SEs_region = {}
start = perf_counter()
for se in SEs:
    try:
        signs: Optional[Dict[str, Any]] = se_info.find_one({"se": se})
        if signs is not None:
            SEs_region[se] = signs["region"]
        else:
            print(f"Unable to find {se} in cwa_SEs collection.")
    except TypeError:
        print(f"Unable to find {se} in cwa_SEs collection. (line {lineno()})")
end = perf_counter()
region_se_time = end - start
if region_se_time > 1:
    print(
        f"Create a region/SE dict created in {region_se_time:.6f} seconds. (line {lineno()})"
    )

# Create a dict of Region names to Region index
region_dict: Dict[str, int] = {}
try:
    start = perf_counter()
    for x in cwa_regions.find({"Region": {"$exists": True}}):
        region_dict[x["Region"]] = x["Index"]
    end = perf_counter()
    name_index_time = end - start
    if name_index_time > 1:
        print(
            f"Region names and index dict created in {name_index_time:.6f} seconds. "
            f"(line {lineno()})"
        )
except (RuntimeError, ConnectionFailure, TimeoutError) as mongo_connection_error:
    print(f"Error: {mongo_connection_error}. (line {lineno()})")

# Create a dict of regions with list of SE's as values
se_dict = create_se_dict()
total_se_dict = se_dict.copy()

# create a list of Regions generated from the keys of se_dict
for key in se_dict.keys():
    regions.append(key)  # add the key to the list

"""Create a list of SEs that exceed the 80th percentile"""

# For each SEs, determine the number of matches in assignments
SE_assignment_count = {}
start = perf_counter()
for se in SEs:
    try:
        signs = p.cwa_matches.find_one({"SE": se})
    except (RuntimeError, ConnectionFailure, TimeoutError) as mongo_connection_error:
        print(f"Error: {mongo_connection_error}. (Line {lineno()})")
        sys.exit()
    if signs is not None:
        signs = signs["assignments"]
        signs_count = 0
        if isinstance(signs, dict):
            for values in signs.values():
                if values != "":
                    signs_count += 1
        SE_assignment_count[se] = signs_count
end = perf_counter()
se_match_time = end - start
if se_match_time > 1:
    print(f"SE matches dict created in {se_match_time:.6f} seconds. (line {lineno()})")

# Calculate the 80th percentile
if len(SE_assignment_count) > 0:
    threshold_percentile = np.percentile(list(SE_assignment_count.values()), 80)

    # Create a list of SEs that exceed the 80th percentile
    top_ses = []
    for se in SE_assignment_count:
        if SE_assignment_count[se] > threshold_percentile:
            top_ses.append(se)
    top_ses_kobayashi = top_ses.copy()
else:
    print("No SEs found in SE_assignment_count dict.")
    sys.exit()

"""End of 80th percentile calculation"""

setup_time_end = perf_counter()
if setup_time_end - setup_time_start > 1:
    print(f"\nSetup time: {setup_time_end - setup_time_start:.6f} seconds.")

# Fuse Date
current_time = "2/14/2024"

while count > 0:
    if kobayashi_counter == 5:
        print("Kobayashi Maru counter exceeded threshold.")
        exit()

    region_count = False
    kobayashi = False
    rebalance = False
    se_med = []

    # check if there are any regions with no SEs
    for id, num in enumerate(se_dict):
        # is se_dict[id] in regions?
        if id in regions and len(se_dict[num]) == 0:
            print(f"\nNo more SEs in region {num} (line {lineno()})")
            # remove the region from the list of regions
            try:
                regions.remove(id)
                print(f" Removed region {id} from list.")
            except ValueError:
                print(f"Region {id} not in regions list.")
                print(f"Regions list: {regions}")
                continue
        running_count.update({num: len(se_dict[num])})

    print(f"\n{count} SEs remaining. {len(regions)} regions remaining.")
    if len(top_ses) > 0:
        print(f" {len(top_ses)} top SEs left to assign:\n{top_ses}")

    # sort running_count by value
    running_count_sorted = sorted(
        running_count.items(), key=lambda item: item[1], reverse=True
    )
    running_count_sorted = [x for x in running_count_sorted if x[1] != 0]
    print(f"Running count (Region, SEs): {running_count_sorted}")

    # Check if there are only 2 SEs left in 1 region.
    if len(regions) == 1 and count >= 2:
        print(f"The {count} remaining SEs are in the same region.")
        # selected_pairings.append(["*DUP*", "*FOLLOWS*"])
        kobayashi = True
        kobayashi_counter += 1
    # Not good. Reset everything and try again.

    if kobayashi is False:
        """SE1 Selection Steps"""
        # Check if there are only 4 SEs left in 3 regions
        if len(regions) == 3 and count == 4:
            print(f"Only four SEs left in three regions. (line {lineno()})")
            region_count = True
            rebalance_region = random.randint(1, 2)
            print(
                f"Rebalance selection (Region, SEs): {running_count_sorted[rebalance_region]}"
            )
            rebalance = True
            placeholder_se = running_count_sorted[rebalance_region]
            # remove placeholder_se from running_count_sorted
            running_count_sorted.remove(placeholder_se)
            regions.remove(placeholder_se[0])
            count = len(SEs)

        """
        Calculate the median score of SEs to Region and pad regions list
        with regions over median plus 2.
        """
        if count > 10:
            # Create a list of scheduled values from running_count_sorted to calculate median
            for _ in running_count_sorted:
                # append values to se_med
                se_med.append(_[1])
            # calculate the median
            se_median = median_high(se_med)
            print(f"Median: {se_median}")

            # number of regions above median plus 2
            over_median = []
            two_over_median = []
            # add items and values from running_count_sorted greater than 2 to over_median
            two_over_median = [x for x in running_count_sorted if x[1] > 2]
            # get a list of items with values greater than the median in running_count_sorted
            over_median = [x[0] for x in two_over_median if x[1] > se_median]
            over_median_count = len(over_median)
            if over_median_count > 0:
                print(
                    f" Number of regions above the median: {over_median_count}. Regions: {over_median}"
                )

                # Add regions over median +2 to regions list for twice the chance to get selected
                regions.extend(over_median)
                print(f" Regions: {regions}")
        else:
            over_median_count = 0

        """End of median calculation and padding of regions list"""

        # If there are SEs in the top_ses list, select them for assignment first
        if len(top_ses) > 0:
            # pick a random se from top_ses
            se1 = random.choice(top_ses)
            # determine se1's region
            se1_data = se_info.find_one({"se": se1})
            if se1_data is not None:
                se1_region = se1_data["region"]
                print(f"Selected top SE {se1} from region {se1_region}")
                # Remove se1 from top_ses
                top_ses.remove(se1)
                print(f" Removed {se1} from top_ses list {se1_region}.")
                # Remove se1 from se_dict
                se1_region_data = cwa_regions.find_one({"Region": se1_region})
                if se1_region_data is not None:
                    se1_region_num = se1_region_data["Index"]
                    se_dict[se1_region_num].remove(se1)
                    print(f" Removed {se1} from se_dict region {se1_region}.")
                elif se1 in se_dict[0]:
                    se_dict[0].remove(se1)
                    print(f" Removed {se1} from se_dict region {se_dict[0]}.")
                else:
                    print(
                        f"Error: {se1} region data not found in cwa_regions collection."
                        f" Line {lineno()}"
                    )
                    sys.exit()
                pick_region = se1_region_num
            else:
                print(f"Error: {se1} not found in cwa_SEs collection.")
                print(f" Line {lineno()}")
                sys.exit()

        else:
            # pick a random integer from the regions list
            pick_region = random.choice(regions)
            print(f"Picked region {pick_region} with {len(se_dict[pick_region])} SEs.")

            # number of SEs in the region
            num_se = len(se_dict[pick_region])
            if num_se == 0:
                print(f"No more SEs in region {pick_region} (line {lineno()})")
                # remove the region from the list of regions
                regions.remove(pick_region)
                # pick a new region
                pick_region = random.choice(regions)
                print(f"Region {pick_region} has {len(se_dict[pick_region])} SEs")
                while len(se_dict[pick_region]) == 0:
                    print(f"No more SEs region {pick_region} (line {lineno()})")
                    # remove the region from the list of regions
                    regions.remove(pick_region)
                    print(regions)
                    # pick a new region
                    pick_region = random.choice(regions)
                    print(f"Region {pick_region} has {len(se_dict[pick_region])} SEs")

            # pick a random se from the region
            se1 = random.choice(se_dict[pick_region])
            print(f" Selected {se1} from region {pick_region}. (line {lineno()})")

            # remove se1 from se_dict
            se_dict[pick_region].remove(se1)

        """ SE2 Selection Steps"""
        print(f"\nSE2 Selection Steps (line {lineno()})")

        # clear out empty regions from regions list
        # TODO: Convert this to a function
        for id, num in enumerate(se_dict):
            # is se_dict[id] in regions?
            if id in regions and len(se_dict[num]) == 0 and num != pick_region:
                print(f"\nNo more SEs in region {num} (line {lineno()})")
                # remove the region from the list of regions
                try:
                    regions.remove(id)
                    print(f" Removed region {id} from list.")
                except ValueError:
                    print(f"Region {id} not in regions list.")
                    print(f"Regions list: {regions}")
                    continue
            running_count.update({num: len(se_dict[num])})

        count = len(SEs)

        print(f"\n{count} SEs remaining. {len(regions)} regions remaining.")

        # select random index from se_dict that is not pick_region
        if len(se_dict) == 1 or len(se_dict) == 0:
            print(f"se_dict: {se_dict}")
        pick_region2 = random.choice(regions)
        while pick_region2 == pick_region:
            if len(regions) == 1:
                print(
                    f"Only one region {regions} left with {len(se_dict[pick_region])} SEs."
                )
                break
            pick_region2 = random.choice(regions)
        num_se2 = len(se_dict[pick_region2])
        print(f"Picked region {pick_region2} with {num_se2} SEs.")
        while num_se2 == 0:
            print(f"No more SEs in region {pick_region2} (line {lineno()})")
            # remove the region from the list of regions
            print(f" Removing region {pick_region2}.")
            regions.remove(pick_region2)
            if len(regions) == 0:
                print("No more available regions. Cannot pick SE2.")
                kobayashi = True
                kobayashi_counter += 1
                break
            else:
                # pick a new region
                pick_region2 = random.choice(regions)
                print(
                    f"Picked region {pick_region2} with {len(se_dict[pick_region2])} SEs."
                )
                num_se2 = len(se_dict[pick_region2])

        # pick a random se from pick_region2
        if kobayashi is False:
            try:
                se2 = random.choice(se_dict[pick_region2])
                print(f" Selected {se2} from region {pick_region2}")

                # lookup list of previous assignments for se1
                try:
                    se1_assignments = p.cwa_matches.find_one({"SE": se1})
                except (
                    RuntimeError,
                    ConnectionFailure,
                    TimeoutError,
                ) as mongo_connection_error:
                    print(f"Error: {mongo_connection_error}.")
                    print(f" Line {lineno()}")
                    sys.exit()
                except Exception as e:
                    print(f"Error: {e}.")
                    print(f" Line {lineno()}")
                    sys.exit()

                if se1_assignments is not None:
                    # count number of keys in 'assignments' without a blank value and create a list
                    prev_assignments = 0
                    se1_previous_assignments = []
                    for _ in se1_assignments["assignments"].values():
                        if _ != "":
                            se1_previous_assignments.append(_)
                            prev_assignments += 1

                    # Does se1_previous_assignments contain all the entries in SEs?
                    matchability = True
                    ses_matched = []
                    for se in SEs:
                        if se == se1 or SEs_region[se] == SEs_region[se1]:
                            ses_matched.append(se)
                        else:
                            if se in se1_previous_assignments:
                                ses_matched.append(se)
                    if len(ses_matched) == len(SEs):
                        print(f"{se1} has been paired with all remaining SEs.")
                        matchability = False

                    if matchability is False:
                        print(f"Not possible to match {se1} with available SEs.")

                    # Logic to manage duplicated pairing.
                    if se2 in se1_previous_assignments:
                        matched_date = None
                        # Lookup se2 in Mongo to determine when they were paired.
                        match_dates = p.cwa_matches.find_one({"SE": se2})
                        if match_dates is not None:
                            for key, value in match_dates.get(
                                "assignments", {}
                            ).items():
                                if value == se1:
                                    matched_date = key
                        if matched_date is not None:
                            # Convert matched_date into type date
                            match_date = datetime.strptime(matched_date, "%m/%d/%Y")
                            # Calculate days ago match occurred
                            match_delta = match_date - datetime.now()
                            print(
                                f" {se1} and {se2} were previously paired {match_delta.days} days ago."
                            )
                        # Pick an unpaired SE from se_list, if available.
                        if len(SEs) > 2 and matchability is True:
                            print(f" Picking again from {len(SEs)} unmatched SEs.")
                            # determine se2's region
                            se2_data = se_info.find_one({"se": se2})
                            if se2_data is not None:
                                se2_region = se2_data["region"]
                                print(f"Selected {se2} from region {se2_region}")
                                # Select new se2
                                se2_pick_counter = 1
                                while (
                                    se2 in se1_previous_assignments
                                    or se1 == se2
                                    or SEs_region[se1] == SEs_region[se2]
                                ):  # noqa: E501
                                    # Need to do something here to prevent infinite loop
                                    # because all SEs were paired previously. Select cwroblew as se1
                                    print(f"{se2} was paired previously.")
                                    se2 = random.choice(SEs)
                                    # determine se2's region
                                    se2_data = se_info.find_one({"se": se2})
                                    if se2_data is not None:
                                        se2_region = se2_data["region"]
                                    print(f" Selected {se2} from region {se2_region}")
                                    se2_pick_counter += 1
                                print(
                                    f" It took {se2_pick_counter} attempts to find {se2}."
                                )
                            print(
                                f"{se1} has {prev_assignments} previous assignments. "
                                f"But not paired with {se2} in region {se2_region}."
                            )
                            # Remove se2 from se_dict
                            se2_region_index = region_dict[se2_region]
                            se_dict[se2_region_index].remove(se2)
                            print(f" Removed {se2} from se_dict region {se2_region}.")
                        elif match_delta.days > 364:
                            # Allow the match and proceed
                            print("Allowing the match and proceeding.")
                        else:
                            # No nonrepeating pairs available.
                            print(f"No nonrepeating pairs available. (line {lineno()})")
                            print(f"Available SEs: {len(SEs)}")
                            kobayashi = True
                            kobayashi_counter += 1
                    else:
                        print(f"{se1} and {se2} have NOT been paired previously.")
                        # Remove se2 from se_dict
                        se_dict[pick_region2].remove(se2)
                        print(f" Removed {se2} from se_dict region {pick_region2}.")

                # Remove se2 from top_ses list
                if se2 in top_ses:
                    print(f"{se2} is in top_ses list.")
                    # Remove se2 from top_ses
                    top_ses.remove(se2)
                    print(f" Removed {se2} from top_ses list.")

                # remove se1 & se2 from se_list
                if se1 in SEs:
                    SEs.remove(se1)
                    print(f" Removed {se1} from se_list.")
                if se2 in SEs:
                    SEs.remove(se2)
                    print(f" Removed {se2} from se_list.")
                print(f" {len(SEs)} SEs remaining.")

                # if length of regions is 2 and the count of any region is greater than 2, reset.
                if len(regions) == 2 and region_count is False:
                    # if region 0 minus region 1 does not equal 0, can't match remaining SEs
                    if running_count_sorted[0][1] - running_count_sorted[1][1] != 0:
                        print(
                            f"No way to successfully match remaining SEs. (line {lineno()})"
                        )
                        print(
                            f" Region {regions[0]} has {running_count_sorted[0][1]} SEs."
                        )
                        print(
                            f" Region {regions[1]} has {running_count_sorted[1][1]} SEs."
                        )
                        kobayashi = True
                        kobayashi_counter += 1
                    else:
                        while pick_region == pick_region2:
                            print(f"{se1} and {se2} are in the same region.")
                            print(" Pick again.")
                            se2 = random.choice(se_dict[pick_region2])
                            print(f" Selected {se2} from region {pick_region2}")
                            if len(regions) == 1 and count == 2:
                                print("Only two SEs left in same region.")
                                # reset and try again
                                kobayashi = True
                                kobayashi_counter += 1
                                break
            except IndexError:
                print(f"se_dict: {se_dict}. Region list: {regions}")

    if kobayashi is False:
        # remove se2 from se_dict
        if se2 in se_dict[pick_region2]:
            se_dict[pick_region2].remove(se2)

        # create a list of the selected SEs
        se_list = [se1, se2]
        print(f"Paired {se_list}")

        # add se_list to selected_pairings list
        selected_pairings.append(se_list)

        # remove last over_median_count regions from regions
        for x in range(over_median_count):
            regions.pop()

        if rebalance:
            # add placeholder_se back to regions
            regions.append(placeholder_se[0])

        # decrement count
        count -= 2
    else:
        print("Kobayashi Maru. Resetting.")
        selected_pairings = []
        regions = []
        # Reset top_ses list
        top_ses = top_ses_kobayashi.copy()
        # Reset se_dict
        se_dict = create_se_dict()
        total_se_dict = se_dict.copy()
        # create a list made up of the keys of the dictionary
        for key in se_dict.keys():
            regions.append(key)  # add the key to the list

        # Create a list of all SEs
        SEs = SEs_list()

        # reset the count of SEs
        count = len(SEs)

        print(f"Kobayashi counter: {kobayashi_counter}")
        print("Reset complete.")

print("\nAll SEs have been paired.")

print('\nUpdating "cwa_matches" collection in MongoDB.')
for se_pair in selected_pairings:
    # update assignments dict for se1 in the Mongo cwa_matches collection
    print(f" Updating: {se_pair[0]}")
    date_key = f"assignments.{current_time}"
    update_se1 = p.cwa_matches.update_one(
        {"SE": se_pair[0]},
        {"$set": {date_key: se_pair[1]}},
    )
    print(f" Updating: {se_pair[1]}")
    update_se2 = p.cwa_matches.update_one(
        {"SE": se_pair[1]},
        {"$set": {date_key: se_pair[0]}},
    )
print("Update complete.")
