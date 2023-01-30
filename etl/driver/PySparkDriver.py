from etl.test.test import *

# Create Dataframe
CreateDataframe()

# Converting the Issue Date with the timestamp format
Timestamp().show()

# Convert timestamp to date type
DateTime().show()

# Remove the starting extra space in Brand column for LG and Voltas fields
EmptySpaces().show()

# Replace null values with empty values in Country column
ReplacingEmpty().show()

# Create 2nd Dataframe
CreateDataframe2()

# Change the camel case columns to snake case
ChangeCamelCase().show()

# Adding another column as start_time_ms and convert the values of StartTime to milliseconds
StartTime().show()

#Combine both the tables based on the Product Number and get all the fields in return. And get the country as EN
Join()