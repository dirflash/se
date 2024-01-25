from utils import preferences as p


def fuse_host(SEs: set):
    # Add FUSE host if odd number of SEs
    if len(SEs) % 2 != 0:
        print("Odd number of SEs. Adding FUSE host to SEs list.")
        SEs.add(p.host)
    else:
        print("Even number of SEs.")
    return SEs
