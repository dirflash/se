def csv_process():
    """
    Create and return a list of SEs with just the CCO ID from se_select.csv
    Create and return a full list of SEs and their full names from se_select.csv
    """
    # Add the second column of each line from se_select.csv to SEs list
    SEs = set()
    full_SEs = []
    try:
        with open("20240119_se_select.csv", "r") as f:
            for line in f:
                # Split the line at the comma and strip the newline character
                split_line = line.strip().split(",")
                # append the full line to full_SEs
                full_SEs.append(split_line)
                # append the second column to SEs (CCO ID)
                SEs.add(split_line[1])
    except FileNotFoundError:
        print("File not found. Please check the file name and path.")
        exit(1)

    print("SEs set created from csv file")
    del line

    # What are the headers?
    header2 = full_SEs[0][1]

    # remove the header from the list
    full_SEs.pop(0)
    SEs.remove(header2)
    print(f" Removed '{header2}' from SEs set")

    return SEs, full_SEs


if __name__ == "__main__":
    SEs, full_SEs = csv_process()
    print(SEs)
    print("csv_process.py executed directly")
