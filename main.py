# %%
import os
import pandas as pd
import sys
from functions import *
# %%
class FilesPerYear:
    def __init__(self):
        self.examUserStatsDataFiles = pd.DataFrame()
        self.examAttemptDetails = pd.DataFrame()
        self.examEventLogs = pd.DataFrame()
        self.accessStatistics = pd.DataFrame()
        self.practiceUserStatsDataFiles = pd.DataFrame()
        self.practiceAttemptDetails = pd.DataFrame()
        self.practiceEventLogs = pd.DataFrame()

        self.examUserStatsFileName = ""
        self.examAttemptDetailsFileName = ""
        self.examEventLogsFileName = ""
        self.accessStatisticsFileName = ""
        self.practiceUserStatsDataFileName = ""
        self.practiceAttemptDetailsFileName = ""
        self.practiceEventLogsFileName = ""

class DataSource:
    def __init__(self, inputFolderDirectory):
        self.dict = {}
        
        for file in os.listdir(inputFolderDirectory):
            if file == '.DS_Store':
                continue
            
            filePath = os.path.join(inputFolderDirectory, file)
            split = file.split('_')
            years = split[1]
            year = years[4:8]       #split and store the year from the file name

            files_per_year = FilesPerYear()
            if year in self.dict:
                files_per_year = self.dict[year]                
            #segregate files of same type, add their respective year column in the csv files and handle missing data
            if file.endswith('exam_Users_Statistics.csv'):    
                dataframe = pd.read_csv(filePath)   
                dataframe['year'] = year
                files_per_year.examUserStatsDataFiles = pd.concat([files_per_year.examUserStatsDataFiles, dataframe]) 
                files_per_year.examUserStatsFileName = file
            elif file.endswith('exam_Attempt_Details.csv'):
                self.handleMissingColumns(filePath)
                dataframe = pd.read_csv(filePath)
                dataframe['year'] = year
                files_per_year.examAttemptDetails = pd.concat([files_per_year.examAttemptDetails, dataframe]) 
                files_per_year.examAttemptDetailsFileName = file
            elif file.endswith('exam_Quiz_Event_Logs.csv'):
                dataframe = pd.read_csv(filePath)
                dataframe['year'] = year
                files_per_year.examEventLogs = pd.concat([files_per_year.examEventLogs, dataframe]) 
                files_per_year.examEventLogsFileName = file
            elif file.endswith('Access_Statistics.csv'):
                dataframe = pd.read_csv(filePath)
                dataframe['year'] = year
                files_per_year.accessStatistics = pd.concat([files_per_year.accessStatistics, dataframe]) 
                files_per_year.accessStatisticsFileName = file
            elif file.endswith('practice_Users_Statistics.csv'):
                dataframe = pd.read_csv(filePath)
                dataframe['year'] = year
                files_per_year.practiceUserStatsDataFiles = pd.concat([files_per_year.practiceUserStatsDataFiles, dataframe]) 
                files_per_year.practiceUserStatsDataFileName = file
            elif file.endswith('practice_Attempt_Details.csv'):
                # TODO: Check if missing columns before handling
                self.handleMissingColumns(filePath)
                dataframe = pd.read_csv(filePath)
                dataframe['year'] = year
                files_per_year.practiceAttemptDetails = pd.concat([files_per_year.practiceAttemptDetails, dataframe]) 
                files_per_year.practiceAttemptDetailsFileName = file
            elif file.endswith('practice_Quiz_Event_Logs.csv'):
                dataframe = pd.read_csv(filePath)
                dataframe['year'] = year
                files_per_year.practiceEventLogs = pd.concat([files_per_year.practiceEventLogs, dataframe]) 
                files_per_year.practiceEventLogsFileName = file
            
            self.dict[year] = files_per_year

application_path = ""
if getattr(sys, 'frozen', False):
#    If the application is run as a bundle, the PyInstaller bootloader
#    extends the sys module by a flag frozen=True and sets the app 
#    path into variable _MEIPASS'.
    application_path = sys.executable
else:
    application_path = os.path.dirname(os.path.abspath(__file__))

application_path = os.path.dirname(application_path)
# application_path = os.getcwd()
inputFolder = "Data"
inputFolderDirectory = os.path.join(application_path, inputFolder)
print(application_path)
print("************** Setup Files and Folders ************** \n\n")
dataSource = DataSource(inputFolderDirectory)

outputFolder = "Output"
outputFolderDirectory = os.path.join(application_path, outputFolder)
if not os.path.exists(outputFolderDirectory):
    os.makedirs(outputFolderDirectory)

examAttemptDetailsCombined = {'df': []} 
examUserStatsCombined = {'df': []} 
examEventLogsCombined = {'df': []} 
accessStatsCombined = {'df': []} 
practiceAttemptDetailsCombined = {'df': []} 
practiceUserStatsCombined = {'df': []} 
practiceEventLogsCombined = {'df': []} 

sorted_keys = sorted(dataSource.dict.keys())

print("************** Starting handling after Parsing Input ************** \n\n")
for key in sorted_keys:

    print("************** Starting handling for Year " + key + " ************** \n\n")

    files_per_year = dataSource.dict[key]
    print("************** Starting Performing Operations ************** \n\n")
    performDataframeOperations(files_per_year)
    print("************** Write To Output Files ************** \n\n")
    writeToIndividualCSV(files_per_year, outputFolderDirectory)

    if 'file_name' not in examAttemptDetailsCombined:
        examAttemptDetailsCombined['file_name'] = files_per_year.examAttemptDetailsFileName
    if 'file_name' not in examUserStatsCombined:
        examUserStatsCombined['file_name'] = files_per_year.examUserStatsFileName

    if 'file_name' not in examEventLogsCombined:
        examEventLogsCombined['file_name'] = files_per_year.examEventLogsFileName

    if 'file_name' not in accessStatsCombined:
        accessStatsCombined['file_name'] = files_per_year.accessStatisticsFileName

    if 'file_name' not in practiceAttemptDetailsCombined:
        practiceAttemptDetailsCombined['file_name'] = files_per_year.practiceAttemptDetailsFileName

    if 'file_name' not in practiceUserStatsCombined:
        practiceUserStatsCombined['file_name'] = files_per_year.practiceUserStatsDataFileName

    if 'file_name' not in practiceEventLogsCombined:
        practiceEventLogsCombined['file_name'] = files_per_year.practiceEventLogsFileName

    examAttemptDetailsCombined['df'].append(files_per_year.examAttemptDetails)
    examUserStatsCombined['df'].append(files_per_year.examUserStatsDataFiles)
    examEventLogsCombined['df'].append(files_per_year.examEventLogs)
    accessStatsCombined['df'].append(files_per_year.accessStatistics)
    practiceAttemptDetailsCombined['df'].append(files_per_year.practiceAttemptDetails)
    practiceUserStatsCombined['df'].append(files_per_year.practiceUserStatsDataFiles)
    practiceEventLogsCombined['df'].append(files_per_year.practiceEventLogs)

print("************** Create Combined Output Files ************** \n\n")
combinedFolder = "Combined"
combinedFolderDirectory = os.path.join(outputFolderDirectory, combinedFolder)
if not os.path.exists(combinedFolderDirectory):
    os.makedirs(combinedFolderDirectory)

combineAndWrite(examAttemptDetailsCombined, combinedFolderDirectory)
combineAndWrite(examUserStatsCombined, combinedFolderDirectory)
combineAndWrite(examEventLogsCombined, combinedFolderDirectory)
combineAndWrite(accessStatsCombined, combinedFolderDirectory)
combineAndWrite(practiceAttemptDetailsCombined, combinedFolderDirectory)
combineAndWrite(practiceUserStatsCombined, combinedFolderDirectory)
combineAndWrite(practiceEventLogsCombined, combinedFolderDirectory)