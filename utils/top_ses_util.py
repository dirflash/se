import numpy as np


def top_percentile(se_assignment_count):
    # Get the 80th percentile of the se_assignment_count
    percentile = np.percentile(list(se_assignment_count.values()), 80)
    print(f"80th percentile: {percentile}")
    return percentile


def top_ses(se_assignment_count, percentile):
    # Get the top 20% of SEs
    top_ses_set = set()
    top_ses_set = {x for x in se_assignment_count if se_assignment_count[x] > percentile}
    print(f"Top SES: {top_ses_set}")
    return top_ses_set
