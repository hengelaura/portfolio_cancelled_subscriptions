# This is the main file for the cancelled subscriptions portfolio project

import sqlite3
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# we are going to follow the rough outline of the jupyter notebook that we already made
# connect to database, and inspect table structure
# use read_sql_query to collect each table
# check and convert the types (look for number of columns, names, types, so they match what we expect)
# may need to clean incoming data
# reorganize data
# export data


# function to clean students table
def clean_student_table(df):
    # convert types
    clean_df = df
    clean_df = clean_df.astype({'job_id': float, 'num_course_taken': float, 'current_career_path_id': float,
                                'time_spent_hrs': float})
    clean_df['dob'] = pd.to_datetime(clean_df['dob'])

    # add columns for age and age range
    now = pd.to_datetime('now')
    clean_df['age'] = round((now - clean_df['dob']).dt.days / 365)
    clean_df['age_range'] = round(clean_df['age'] / 10) * 10

    # expand contact info so it is usable
    clean_df['contact_info'] = clean_df['contact_info'].apply(lambda x: ast.literal_eval(x))
    explode_contact = pd.json_normalize(clean_df['contact_info'])
    clean_df = pd.concat([clean_df.drop('contact_info', axis=1), explode_contact], axis=1)
    expand_address = clean_df['mailing_address'].str.split(',', expand=True)
    clean_df['Street'] = expand_address[0]
    clean_df['City'] = expand_address[1]
    clean_df['State'] = expand_address[2]
    clean_df['Zipcode'] = expand_address[3]
    clean_df.drop('mailing_address', axis=1, inplace=True)

    # handle missing data
    clean_df.dropna(subset=['num_course_taken', 'job_id'], inplace=True)
    clean_df['current_career_path_id'] = clean_df['current_career_path_id'].fillna(0)
    clean_df['time_spent_hrs'] = clean_df['time_spent_hrs'].fillna(0)
    return clean_df


def clean_jobs(df):
    clean_df = df.drop_duplicates(subset=['job category', 'avg_salary'])
    return clean_df


def clean_courses(df):
    clean_df = df.drop_duplicates(subset=['career_path_name', 'hours_to_complete'])
    clean_df.loc[(len(clean_df.index))] = [0, 'no selection', 0]
    return clean_df


def merge_tables(student, course, job):
    final_table = pd.merge(student, course.rename(columns={'career_path_id': 'current_career_path_id'}))
    final_table = pd.merge(final_table, job)
    return final_table
