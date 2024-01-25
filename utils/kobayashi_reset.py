from utils import csv_process, fuse_host, top_ses_util
from utils.se_dict_util import se_count_dict


def kobayashi(kobayashi_counter):
    # Reset and start over
    if kobayashi_counter == 5:
        print("Kobayashi Maru scenario encountered 5 times. Exiting.")
        exit(1)
    kobayashi_counter += 1
    print(
        f"\nKobayashi Maru scenario number {kobayashi_counter}. Reset and start over."
    )
    # Convert full_SEs to SEs
    SEs, full_SEs = csv_process.csv_process()
    # Add FUSE host if odd number of SEs
    SEs = fuse_host.fuse_host(SEs)
    # Create se_dict
    # se_dict = create_se_dict()
    # Create se_assignment_count
    se_assignment_count = se_count_dict(SEs)
    # Calculate the 80th percentile
    percentile = top_ses_util.top_percentile(se_assignment_count)
    # Create top_ses list
    top_ses = top_ses_util.top_ses(se_assignment_count, percentile)
    print(f"Reset number {kobayashi_counter} complete.\n")
    return SEs, full_SEs, se_assignment_count, percentile, top_ses  # , se_dict
