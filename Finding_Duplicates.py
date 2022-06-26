import time
import json

import concurrent.futures
from collections import namedtuple, OrderedDict

from fuzzywuzzy import fuzz
from prettytable import PrettyTable


# Profile Model (for given fields in Task)
class Profile:
    def __init__(
        self,
        pid,
        first_name,
        last_name,
        date_of_birth,
        class_year,
        email_field,
        inp_meta,
    ):
        self.pid = pid
        self.first_name = first_name
        self.last_name = last_name
        self.date_of_birth = date_of_birth
        self.class_year = class_year
        self.email_field = email_field
        self.meta = inp_meta  # Handle extra unknown fields


# Class to manage all operations
class Profiles:
    profiles_dict = (
        OrderedDict()
    )  # Creating a OrderedDict to keep all the profiles_dict

    def __init__(self):
        pass

    def add_profile(self, profile_input):
        # Initializing the profile_input with Profile model
        profile_obj = Profile(
            profile_input["meta"]["pid"],
            profile_input["first_name"],
            profile_input["last_name"],
            profile_input["date_of_birth"],
            profile_input["class_year"],
            profile_input["email_field"],
            profile_input["meta"],
        )

        # Storing each input object as value with key as (profile + pid)
        self.profiles_dict["profile" + str(profile_input["meta"]["pid"])] = profile_obj

    # Compare the two profiles with the given fields
    def profiles_comparison(self, profile_compare_a, profile_compare_b, fields_list):

        score = 0  # Score value initializes for every pair of i,j
        matching_attributes = list()  # Empty list of matching attributes
        nonmatching_attributes = list()  # Empty list of nonmatching attributes
        ignored_attributes = (
            attributes.copy()
        )  # Intially list containing all attributes inside ignored attributes

        weight_text_a = ""  # To creat combine name of first, last and email for i th
        weight_text_b = ""  # To creat combine name of first, last and email for j th

        if "first_name" in fields_list:
            weight_text_a += profile_compare_a.first_name
            weight_text_b += profile_compare_b.first_name
            matching_attributes.append("first_name")
            ignored_attributes.remove("first_name")
        if "last_name" in fields_list:
            weight_text_a += profile_compare_a.last_name
            weight_text_b += profile_compare_b.last_name
            matching_attributes.append("last_name")
            ignored_attributes.remove("last_name")
        if "email_field" in fields_list:
            weight_text_a += profile_compare_a.email_field
            weight_text_b += profile_compare_b.email_field
            matching_attributes.append("email_field")
            ignored_attributes.remove("email_field")

        # If the fuzz match value is greater than 80 , then increment the score
        if fuzz.partial_ratio(weight_text_a, weight_text_b) >= 80:
            score += 1
        else:
            nonmatching_attributes = (
                matching_attributes.copy()
            )  # matching attributes contains fields which couldn`t match
            matching_attributes = (
                list()
            )  # Initializing matching attribute to empty list

        # ----- BELOW CODE FOR COMPARING K FIELDS , where K <= M   ----- #
        for field in fields_list:
            if field not in ["first_name", "last_name", "email_field"]:
                if field in profile_compare_a.meta and field in profile_compare_b.meta:
                    ignored_attributes.append(field)  # Initially to be added
                    profile_a_value = profile_compare_a.meta[field]
                    profile_b_value = profile_compare_b.meta[field]
                elif field in attributes:
                    profile_a_value = getattr(profile_compare_a, field)
                    profile_b_value = getattr(profile_compare_b, field)
                else:
                    continue

                # If both profiles_dict have values other than None
                if profile_a_value != "None" and profile_b_value != "None":
                    ignored_attributes.remove(field)
                    if profile_a_value == profile_b_value:
                        score = score + 1
                        matching_attributes.append(field)
                    else:
                        score = score - 1  # Decrement the score
                        nonmatching_attributes.append(field)

        # If the final score is greater than or equal to 1, then print
        if score >= 1:

            # --- UNCOMMENT BELOW PRINT COMMAND TO PRINT OUTPUT IN DESIRED WAY ---#
            # print("profile",profile_compare_a.pid,", profile",profile_compare_b.pid, ", total match score :",score,
            #                 ", matching_attributes:",(', ').join(matching_attributes) if len(matching_attributes)>0 else "None",
            #                 ", non_matching_attributes:",(', ').join(nonmatching_attributes)if len(nonmatching_attributes)>0 else "None",
            #                 ", ignored_attributes:",(', ').join(ignored_attributes)if len(ignored_attributes)>0 else "None")

            # Adding profiles_dict and their attributes in table
            profile_duplicate_table.add_row(
                [
                    "profile-" + str(profile_compare_a.pid),
                    "profile-" + str(profile_compare_b.pid),
                    (", ").join(matching_attributes)
                    if len(matching_attributes) > 0
                    else "None",
                    (", ").join(nonmatching_attributes)
                    if len(nonmatching_attributes) > 0
                    else "None",
                    (", ").join(ignored_attributes)
                    if len(ignored_attributes) > 0
                    else "None",
                ]
            )

    # Defining the find_duplicates function
    def find_duplicates(self, Profiles_list, fields_list):
        for initialindex in range(len(Profiles_list)):
            for laterindex in range(initialindex + 1, len(Profiles_list)):

                # if both the profiles_dict are present in profiles_dict dictionary
                if (Profiles_list[initialindex] in self.profiles_dict) and (
                    Profiles_list[laterindex] in self.profiles_dict
                ):

                    # using MultiThreading
                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        executor.submit(
                            self.profiles_comparison,
                            self.profiles_dict[Profiles_list[initialindex]],
                            self.profiles_dict[Profiles_list[laterindex]],
                            fields_list,
                        )


#  Decoding json format input into dictionary key value pairs
def customProfileDecoder(ProfileJson):
    return namedtuple("X", ProfileJson.keys())(*ProfileJson.values())


# Converting and storing input
def input_process(profiles):

    # Initializing input values from input.txt
    file1 = open("input.txt", "r")
    Input_Lines = file1.readlines()

    input_list = list()

    for profile_input_line in Input_Lines:
        input_list.append(profile_input_line.strip())

    for profile_item in input_list:
        try:
            # Calling the customProfileDecoder function with json input value
            values = json.loads(profile_item, object_hook=customProfileDecoder)
        except json.decoder.JSONDecodeError as e:
            print("Incorrect/Bad format in input.txt")
            return

        input_dict = {}
        extra_dict = {}

        # Iterating in namedTuple
        for key, value in values._asdict().items():
            if key in attributes:
                input_dict[key] = value
            else:
                # Store the extra unknown fields passed in the input in dict
                extra_dict[key] = value

        input_dict["meta"] = extra_dict
        profiles.add_profile(input_dict)

    return


if __name__ == "__main__":
    start = time.perf_counter()

    # List of all M fields --HardCoded
    attributes = [
        "first_name",
        "last_name",
        "date_of_birth",
        "class_year",
        "email_field",
    ]

    # Table for storing output with --HardCoded header names of fields
    profile_duplicate_table = PrettyTable(
        [
            "1st Profile",
            "2nd Profile",
            "Matching Attributes",
            "Non-Matching Attributes",
            "Ignored Attributes",
        ]
    )

    # Create intance of Profiles()
    profiles = Profiles()

    # Calling function to process input with instance of Profiles() as parameter
    input_process(profiles)

    # Calling the method find_duplicates with --HardCoded "profile_names" and "fields_names"
    profiles.find_duplicates(
        ["profile2", "profile3", "profile1", "profile4", "profile5", "profile6"],
        ["first_name", "last_name", "date_of_birth", "date_of_marriage", "father_name"],
    )

    # Print the table
    print(profile_duplicate_table)

    print(f"Duration: {time.perf_counter() - start}")
