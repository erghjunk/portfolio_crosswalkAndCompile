"""
author: evan fedorko, evanjfedorko@gmail.com
date: 10/2021

This script was written to gather external data (13 values) for each of a set of records
(grant projets awarded from a specific program). Some of those values needed to be pulled
from a spreadsheet dated 3 years prior to the year the grant was awarded (to match the
data used by the grantee at the time of application), and some needed to be pulled from the
year of award. This script compiles them all.


pseudo code of process follows

import pandas as pan

read in target dataset as df to review
target field list = []
other input data = []

def f_crosswalk:
    create variables to lookup based on year
    for target_field in target_field_list:
        read and write

def f_getSourceDF:
    read in sourcetable
    return(sourcedf)

def f_coal employment:
    stuff
    return

def tempOutput:
    write
    return

def finalOutput:
    write
    return

# master program
for each record:
    read year
    if sourcedf exists:
        call f to crosswalk that record passing sourcedf, year, etc
    else:
        call f to create sourcedf
        call f to crosswalk that record passing sourcedf, year, whatever else is needed
    call f to process coal employment
    write temp output
call f to write final output
"""
import pandas as pan
import os

cwd = os.getcwd()

targetFile = cwd + r"\grantList.xlsx"

targetSheet = "Sheet1"

csvFile = cwd + r"\pyWorkOutput.csv"

targetDF = pan.read_excel(
    targetFile, sheet_name=targetSheet, header=0, index_col="Object_ID")

asp = cwd + r"\data"

arcSources = {
    2015: asp + r"\FY2015.xls",
    2016: asp + r"\FY2016.xls",
    2017: asp + r"\FY2017.xls",
    2018: asp + r"\FY2018.xls",
    2019: asp + r"\FY2019.xls",
    2020: asp + r"\FY2020.xls",
    2021: asp + r"\FY2021.xls"
}

arcSheet = "US_Raw"

fieldsYearPlus3 = {
    "AvgUnempRate_3yr": "UNRATETHREEYEAR",
    "AvgUnemp_PctOfUS_3yr": "UNRATETHREEYEARUS",
    "PerCapMktInc": "PCMI",
    "PovertyRt_5yr": "POVRATE5YR",
    "PCMI_pctOfUs": "PCMIUS",
    "PovRt_PercOfUS": "POVRATE5YRUS",
    "EMPTHREEYEAR": "EMPTHREEYEAR",
    "UNEMPTHREEYEAR": "UNEMPTHREEYEAR",
    "BEAPOP": "BEAPOP",
    "PoveryPop_5YrMean": "POVPOP5YR",
}

fieldsYear = {
    "Status": "ECONSTATUS",
    "IndexValueRank": "INDEXRANK",
    "Quartile": "INDEXQUART",
}


def fieldListBuilder(dict1, dict2, year):
    yearPlus3Str = str(year - 3)
    yearStr = str(year)
    fieldList = "fieldList" + yearStr
    fieldList = ["FIPS", "ARC"]
    for key, value in dict1.items():
        fieldList.append(value + yearPlus3Str)
    for key, value in dict2.items():
        fieldList.append(value + yearStr)
    return fieldList


def dataPrep():
    for key, value in arcSources.items():  # key is the year, value is the file path
        fieldList = fieldListBuilder(fieldsYearPlus3, fieldsYear, key)
        # create a string for dynamically named variable
        outName = "arc" + str(key) + "_df"
        # use dynamically named variable and make accessible in global namespace
        globals()[outName] = pan.read_excel(value, sheet_name=arcSheet,
                                            header=0, usecols=fieldList, index_col="FIPS")


def testing():

    # use .at for a single value, .loc for multiple returns
    # .iat and .iloc are the same thing but integer based instead of label based

    for item in targetDF.index:
        workingYear = targetDF.Year[item]
        workingFIPS = targetDF.fips2[item]
        subtractedYear = workingYear - 3
        # converts (?) a string to refer to a global variable name
        lookupDF = globals()["arc" + str(workingYear) + "_df"]
        value = "BEAPOP"
        lookupAtt = value + str(subtractedYear)
        print("value from lookup: " + str(lookupDF.at[workingFIPS, lookupAtt]))
        newValue = lookupDF.at[workingFIPS, lookupAtt]
        print("current value of target: " + str(targetDF.BEAPOP[item]))
        targetDF.at[item, "BEAPOP"] = newValue
        print("new value of target: " + str(targetDF.BEAPOP[item]))


def getData(input_df):
    for item in input_df.index:  # index_col for this DF is set to Object_ID
        workingYear = input_df.Year[item]
        workingFIPS = input_df.fips2[item]
        # key is the target field, value is the source without the year; year to append is working year minus 3
        for key, value in fieldsYearPlus3.items():
            subtractedYear = workingYear - 3
            # converts string to variable in global namespace
            lookupDF = globals()["arc" + str(workingYear) + "_df"]
            lookupAtt = value + str(subtractedYear)
            # reads as a row/column location; use .at to access single value in a df
            newValue = lookupDF.at[workingFIPS, lookupAtt]
            input_df.at[item, key] = newValue
        for key, value in fieldsYear.items():  # key is the target field, value is the source without the year; year to append is working year
            # converts string to variable in global namespace
            lookupDF = globals()["arc" + str(workingYear) + "_df"]
            lookupAtt = value + str(workingYear)
            # row/column "address"; use .at to access single value in a df
            newValue = lookupDF.at[workingFIPS, lookupAtt]
            input_df.at[item, key] = newValue
        arcField = "ARC"
        writeField = "ARC_Flag"
        newValue = lookupDF.at[workingFIPS, arcField]
        input_df.at[item, writeField] = newValue


def writeToCsv(targetDF):
    targetDF.to_csv(csvFile)


def finish():
    writeToCsv(targetDF)


# master program
dataPrep()
# testing()
getData(targetDF)
finish()
