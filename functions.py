# %%
import os
import pandas as pd
import math
import uuid
import re
import sys
import datetime
def handleMissingColumns(self, filePath):
    lineToWrite = ''
    totalLines = []
    with open(filePath, 'r') as file:
        totalLines = file.readlines()
        for line in totalLines:
            columnNames = line.split(',')
            if len(columnNames) == 18:
            #do some random magic and make 20. Add 2 more column names
                columnNames[17] = columnNames[17].rstrip()
                columnNames.extend(['total_marks_scored', 'total_marks_out_of\n'])
                lineToWrite = (',').join(columnNames)
            elif len(columnNames) != 20:
                raise Exception('Please ensure Attempt details file for both practice and exam have the correct headers as per user manual')
            break
    if len(lineToWrite) != 0 and len(totalLines) != 0:
        totalLines[0] = lineToWrite
    with open(filePath, 'w') as file:
        file.writelines(totalLines)

def getTopAndBottomScorers(df, percentage):
    total = len(df)
    numbers = math.ceil(percentage/100 * total)
    df = df.sort_values(by=['Score'], ascending = False)
    #get top and bottom users as per the specified percentage parameter
    topUserIds = df.head(numbers)['Username'].values.tolist()
    bottomUserIds = df.tail(numbers)['Username'].values.tolist()

    return (topUserIds, bottomUserIds)

def createHighScoreLowScorePerQuestionDict(df, topUsers, bottomUsers):
    topScoreDict = {}
    lowScoreDict = {}
    unique_question_ids = df['question_id'].unique()
    summary_df = df.copy()
    summary_df = summary_df.loc[summary_df['Q Title'] == "Summary:"]
    #determine the top and lowest scorers as per the topUsers and bottomUsers parameters passed
    for id in unique_question_ids:
        topScore = 0
        lowScore = 0
        temp = summary_df.loc[summary_df['question_id'] == id]
        for index, row in temp.iterrows():
            if row['Username'] in topUsers:
                topScore += row['total_marks_scored']
            if row['Username'] in bottomUsers:
                lowScore += row['total_marks_scored']
        topScoreDict[id] = topScore
        lowScoreDict[id] = lowScore
        
    return topScoreDict, lowScoreDict

def createDiscriminationIndex(df, user_stats_files):
    top27Users, bottom27Users = getTopAndBottomScorers(user_stats_files, 27)
    top33Users, bottom33Users = getTopAndBottomScorers(user_stats_files, 33)
    top50Users, bottom50Users = getTopAndBottomScorers(user_stats_files, 50)

    top27ScoreDict, low27ScoreDict = createHighScoreLowScorePerQuestionDict(df, top27Users, bottom27Users)
    top33ScoreDict, low33ScoreDict = createHighScoreLowScorePerQuestionDict(df, top33Users, bottom33Users)
    top50ScoreDict, low50ScoreDict = createHighScoreLowScorePerQuestionDict(df, top50Users, bottom50Users)
    
    addIndividualDiscriminationIndex(df, len(top27Users) + len(bottom27Users), top27ScoreDict, low27ScoreDict, 'discrimination_index')
    addIndividualDiscriminationIndex(df, len(top33Users) + len(bottom33Users), top33ScoreDict, low33ScoreDict, 'discrimination_index_33.33')
    addIndividualDiscriminationIndex(df, len(top50Users) + len(bottom50Users), top50ScoreDict, low50ScoreDict, 'discrimination_index_50')

def addIndividualDiscriminationIndex(df, totalUsers, topScoreDict, lowScoreDict, column_name):
    #Calculate, generate and store the values of discrimination index
    df[column_name] = None
    summary_df = df.copy()
    summary_df = summary_df.loc[summary_df['Q Title'] == "Summary:"]
    for index, row in summary_df.iterrows():
        question_id = row['question_id']
        hs = topScoreDict[question_id]
        ls = lowScoreDict[question_id]
        qs = row['total_marks_out_of']
        dif = (hs - ls)/(totalUsers * qs) * 2
        df.loc[df['question_id'] == question_id, column_name] = dif

def createDifficultyIndex(df, user_stats_files):
    top27Users, bottom27Users = getTopAndBottomScorers(user_stats_files, 27)
    top33Users, bottom33Users = getTopAndBottomScorers(user_stats_files, 33)
    top50Users, bottom50Users = getTopAndBottomScorers(user_stats_files, 50)
        
    top27ScoreDict, low27ScoreDict = createHighScoreLowScorePerQuestionDict(df, top27Users, bottom27Users)
    top33ScoreDict, low33ScoreDict = createHighScoreLowScorePerQuestionDict(df, top33Users, bottom33Users)
    top50ScoreDict, low50ScoreDict = createHighScoreLowScorePerQuestionDict(df, top50Users, bottom50Users)

    addIndividualDifficultyIndex(df, len(top27Users) + len(bottom27Users), top27ScoreDict, low27ScoreDict, 'difficulty_index')
    addIndividualDifficultyIndex(df, len(top33Users) + len(bottom33Users), top33ScoreDict, low33ScoreDict, 'difficulty_index_33.33')
    addIndividualDifficultyIndex(df, len(top50Users) + len(bottom50Users), top50ScoreDict, low50ScoreDict, 'difficulty_index_50')

def addIndividualDifficultyIndex(df, totalUsers, topScoreDict, lowScoreDict, column_name):
    #Calculate, generate and store the values of difficulty index
    df[column_name] = None
    summary_df = df.copy()
    summary_df = summary_df.loc[summary_df['Q Title'] == "Summary:"]
    for index, row in summary_df.iterrows():
        question_id = row['question_id']
        hs = topScoreDict[question_id]
        ls = lowScoreDict[question_id]
        qs = row['total_marks_out_of']
        dif = (hs + ls)/(totalUsers * qs)

        df.loc[df['question_id'] == question_id, column_name] = dif

def addScoreToSection(df):
    #add scores in Section
    filtered_df = df[~df['Section #'].isna()]
    previous_index = 0
    for index, row in filtered_df.iterrows():
        scored = row['total_marks_scored']
        max_score = row['total_marks_out_of']
        if not pd.isnull(scored) and not pd.isnull(max_score):
            df.at[previous_index, 'total_marks_scored'] = scored
            df.at[previous_index, 'total_marks_out_of'] = max_score
        previous_index = index

def addQNumberAndQType(df):
    previous_index = 0
    for index, row in df.iterrows():
        q_number = row['Q #']
        q_type = row['Q Type']
        if not pd.isnull(q_number) and not pd.isnull(q_type):
            df.at[previous_index, 'Q #'] = q_number
            df.at[previous_index, 'Q Type'] = q_type
        previous_index = index

def performEventLogsOperations(df, user_stats):
    #perform functions for EventLogs files
    #determine question numbers and calculate the time spent on them
    df['question_number'] = None 
    df['time_spent'] = None

    for index, row in df.iterrows():
        event = row[' Event']
    
        if event == 'Quiz Entry':
            previous_time = row[' Time']
    
        if re.fullmatch(r'Page \d+ Saved', event):
            number = event.split()[1]
            df.at[index, 'question_number'] = number
            format = '%I:%M %p'
            startDateTime = datetime.datetime.strptime(previous_time, format)
            endDateTime = datetime.datetime.strptime(row[' Time'], format)
            diff = endDateTime - startDateTime 
            df.at[index, 'time_spent'] = diff
            previous_time = row[' Time'] 

    addUserNames(df, user_stats)

def performUserStatsOperations(df, access_statistics):
    #perform operations for UserStats files
    #add a column for full name
    df['full_name'] = df['FirstName'] + ' ' + df['LastName']
    #add a column for ModuleName
    module_name = access_statistics['ModuleName'].iat[0]
    df['module_name'] = module_name

    sorted_df = df.sort_values('Score', ascending = False)
    sorted_df.insert(0, 'rank', range(1, 1 + len(df)))
    #add a column for rank and percentile
    df['rank'] = sorted_df['rank']
    df['percentile'] = df['Score'].rank(method='max', pct=True) * 100

def addUserNames(df, user_stats):
    #add a Username column
    for index, row in user_stats.iterrows():
        full_name = row['full_name']
        username = row['Username']
        if pd.isnull(username):
            df.loc[df['User'] == full_name, 'Username'] = 0
        else:
            df.loc[df['User'] == full_name, 'Username'] = username
    df['Username'] = df['Username'].fillna(0)
    df['Username'] = df['Username'].astype(int)

def createUniqueQuestionIds(df):
    #generate unique question ids
    uniqueQuestionIdDict = {}
    filtered = df.dropna(subset=['Section #'])
    for index, row in filtered.iterrows():
        if math.isnan(row['Section #']) or row['Q Title'] == 'Summary:':
            continue
        else:
            uniqueQuestionIdDict[row['Q Title']] = uuid.uuid4().hex
    for index, row in df.iterrows():
        title = row['Q Title']
        if title == 'Summary:':
            df.at[index, 'question_id'] = df.iloc[index - 1]['question_id']
            continue
        else:
            dropped = re.sub(r' \w*\(\d*\)', '', title)
            df.at[index, 'question_id'] = uniqueQuestionIdDict[dropped]

def writeToIndividualCSV(files_per_year, folderDirectory):
    #write files in the output directory
    examAttemptDetailsPath = os.path.join(folderDirectory, files_per_year.examAttemptDetailsFileName)
    files_per_year.examAttemptDetails.to_csv(examAttemptDetailsPath, index=False)

    examUserStatsPath = os.path.join(folderDirectory, files_per_year.examUserStatsFileName)
    files_per_year.examUserStatsDataFiles.to_csv(examUserStatsPath, index=False)

    examEventLogsPath = os.path.join(folderDirectory, files_per_year.examEventLogsFileName)
    files_per_year.examEventLogs.to_csv(examEventLogsPath, index=False)

    accessStatisticsPath = os.path.join(folderDirectory, files_per_year.accessStatisticsFileName)
    files_per_year.accessStatistics.to_csv(accessStatisticsPath, index=False)

    practiceAttemptDetailsPath = os.path.join(folderDirectory, files_per_year.practiceAttemptDetailsFileName)
    files_per_year.practiceAttemptDetails.to_csv(practiceAttemptDetailsPath, index=False)

    practiceUserStatsDataFilesPath = os.path.join(folderDirectory, files_per_year.practiceUserStatsDataFileName)
    files_per_year.practiceUserStatsDataFiles.to_csv(practiceUserStatsDataFilesPath, index=False)

    practiceEventLogsPath = os.path.join(folderDirectory, files_per_year.practiceEventLogsFileName)
    files_per_year.practiceEventLogs.to_csv(practiceEventLogsPath, index=False)

def combineAndWrite(dict, output_directory):
    #combine same name files
    dfs = dict['df']
    file_name = dict['file_name']
    result = pd.DataFrame()
    for df in dfs:
        result = pd.concat([result, df], ignore_index = True)
        
    file_path = os.path.join(output_directory, file_name)
    result.to_csv(file_path, index=False)

def performAttempDetailsOperations(df, user_stats_files, isRealExam):
    #perform functions for attempt details files
    df['full_name'] = df['FirstName'] + ' ' + df['LastName']
    addQNumberAndQType(df)
    addScoreToSection(df)
    if isRealExam:
        df.drop(columns=['Bonus?', 'Difficulty', 'Attempt #'], inplace=True)
        createUniqueQuestionIds(df)
        createDifficultyIndex(df, user_stats_files)
        createDiscriminationIndex(df, user_stats_files)
    else:
        df.drop(columns=['Bonus?', 'Difficulty'], inplace=True)
    df[['question_category','question_format', 'question_title']] = df['Q Title'].str.split(':', expand=True)

def performDataframeOperations(files_per_year):
    #attempt details
    print("************** Start manipulating Exam Attempt Details ************** \n\n")
    performAttempDetailsOperations(files_per_year.examAttemptDetails, files_per_year.examUserStatsDataFiles, True)
    print("************** Start manipulating Practice Attempt Details ************** \n\n")
    performAttempDetailsOperations(files_per_year.practiceAttemptDetails, files_per_year.practiceUserStatsDataFiles, False)

    #user stats
    print("************** Start manipulating Exam User Stats ************** \n\n")
    performUserStatsOperations(files_per_year.examUserStatsDataFiles, files_per_year.accessStatistics)
    print("************** Start manipulating Practice User Stats ************** \n\n")
    performUserStatsOperations(files_per_year.practiceUserStatsDataFiles, files_per_year.accessStatistics)    

    #event logs
    print("************** Start manipulating Exam Event Logs ************** \n\n")
    performEventLogsOperations(files_per_year.examEventLogs, files_per_year.examUserStatsDataFiles)
    print("************** Start manipulating Practice Event Logs ************** \n\n")
    performEventLogsOperations(files_per_year.practiceEventLogs, files_per_year.practiceUserStatsDataFiles)


