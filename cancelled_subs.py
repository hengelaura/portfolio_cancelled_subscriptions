# This is the main file for the cancelled subscriptions portfolio project

import sqlite3
import unittest
import ast
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


# I know think, with a testing framework, it makes sense to make all these function be methods
# of a single class so that I can set up and tear down a test environment
class Subscribers:
    # in the init function, I'd like to make a connection to the database, read the tables from the database into
    # dataframes
    def __init__(self, path_to_db):
        self.con = sqlite3.connect(path_to_db)
        curs = self.con.cursor()
        table_names = curs.execute('''select name from sqlite_master where type='table';''').fetchall()
        dfs = []
        for t in table_names:
            query = 'select * from {};'.format(t[0])
            dfs.append(pd.read_sql_query('''{}'''.format(query), self.con))
        self.students = dfs[0]
        self.courses = dfs[1]
        self.jobs = dfs[2]

    def clean_student_table(self):
        # convert types
        clean_df = self.students
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
        self.students = clean_df

    def clean_jobs(self):
        clean_df = self.jobs
        clean_df = clean_df.drop_duplicates(subset=['job category', 'avg_salary'])
        self.jobs = clean_df

    def clean_courses(self):
        clean_df = self.courses
        clean_df = clean_df.drop_duplicates(subset=['career_path_name', 'hours_to_complete'])
        clean_df.loc[(len(clean_df.index))] = [0, 'no selection', 0]
        self.courses = clean_df

    def merge_tables(self):
        final_table = pd.merge(self.students, self.courses.rename(columns={'career_path_id': 'current_career_path_id'}))
        final_table = pd.merge(final_table, self.jobs)
        return final_table

    def close_connection(self):
        self.con.close()


# now it is time to incorporate the unit tests to make sure I'm not ruining anything.
# aka: time to make some unit tests!
def test_null_count(df):
    # make sure no rows exist with null values still
    null_count = df.isnull().count()

    try:
        assert null_count == 0, 'There are no null values in the table'
    except AssertionError as e:
        print(e)  # come back and make me a logging item


def test_duplicates(df):
    # make sure that no rows are duplicates of each other
    length = len(df)
    no_dupes_length = len(df.drop_duplicates(axis=1))

    try:
        assert length == no_dupes_length, 'There are no duplicate values remaining'
    except AssertionError as e:
        print(e)  # come back for me!


# should have some way to make sure that the incoming tables have the number of columns we expect and the right names?
def test_incoming_table_number(tables):
    # check the length of tables (should be 3 incoming tables)
    try:
        assert tables == 3, 'The correct number of tables are present'
    except AssertionError as e:
        print(e)  # come back for me also


def test_columns(current_db, incoming_db):
    # Need the number of columns to be equal
    # need the types of columns to be equal
    current_col_number = len(current_db.columns)
    incoming_col_number = len(incoming_db.columns)

    try:
        assert current_col_number == incoming_col_number, 'Tables are same width, can continue'
    except AssertionError as e:
        print(e)

    try:
        assert current_db.dtypes == incoming_db.dtypes, 'All columns have the same type, can continue'
    except AssertionError as e:
        print(e)



