#! /usr/bin/env python3

import os


def get_user_input():

    while True:
        xml_val = str(input(f"\
            How many xmls / proxies are needed in this batch? "))
        try:
            if int(xml_val) > 10000:
                print(f"{xml_val} is not a valid entry for the starting index, try again.")
                continue
            else:
                xml_total = xml_val
                break

        except ValueError:
            print(f"{xml_val} is not a valid entry for the starting index, try again.")
            continue

    return xml_total



    # while True:
    #     index_1 = str(input(f"\
    #         Index Number to start creating XMLs \
    #         (enter 0 to start at the the first row): "))
    #     try:
    #         index_start = int(index_1)
    #         break
    #     except ValueError:
    #         print(f"{index_1} is not a valid entry for the starting index, try again.")
    #         continue

    # while True:
    #     index_2 = str(input(f"\
    #         Index Number to stop creating XMLs\
    #         (enter 0 to stop at the the final row): "))
    #     try:
    #         index_stop = int(index_2)
    #         break
    #     except ValueError:
    #         print(f"{index_2} is not a valid entry for the stopping index, try again.")
    #         continue

    # return index_start, index_stop
