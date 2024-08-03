# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 13:52:17 2024
@author: ashrafr7
"""

import os
import pandas as pd
import math as mt
import datetime as dt
import time as tm
import numpy as np
from itertools import groupby
from datetime import datetime
import openpyxl
import sys


# Find the start index
class ExitNestedFunctions(Exception):
    pass                
                                                                                                                          
######################################################################################################################
### CONVERT EXACT FILE ###
def read_file(input_file,filename,start_date,end_date):
    
    start_day, start_month, start_year = start_date.split('/') #Extract the start day and month
    end_day, end_month, end_year = end_date.split('/') #Extract the end day and month

    file_import_full = pd.read_excel(input_file,header=None)
    timestamp_index = (file_import_full[file_import_full[0] == 'Timestamp'].index[0])+1
    file_import = file_import_full.loc[timestamp_index:].reset_index(drop=True)


    ## baseline period ##
    if int(start_month) == 11 or int(start_month) == 12 or int(start_month) == 1 or int(start_month) == 2 or int(start_month) == 3: 
        try:
            start_index = file_import[file_import[0].apply(lambda x: x.day == int(start_day) and x.month == int(start_month))].index[0]
        except IndexError:
            # If no match is found, default to the first row
            start_index = 0
    
        try:
            end_index = file_import[file_import[0].apply(lambda x: x.day == int(end_day) and x.month == int(end_month))].index[-1]
        except IndexError:
            april_or_higher_indices = file_import[file_import[0].apply(lambda x: x.month >= 4 and x.year == 2024)].index
            if len(april_or_higher_indices) > 0:
                end_index = april_or_higher_indices[0] - 1
            else:
                # If April or higher values are not available, use the last row of the data
                end_index = file_import.index[-1]    
    
    
    ## embedding period ##
    elif int(start_month) == 4 or int(start_month) == 5:
        start_indices = file_import[file_import.iloc[:, 0].apply(lambda x: x.day == int(start_day) and x.month == int(start_month))].index
        
        if len(start_indices) > 0:
            start_index = start_indices[0]
        else:
            april_or_higher_indices = file_import[file_import[0].apply(lambda x: x.month >= 4 and x.year == 2024)].index
            if len(april_or_higher_indices) > 0:
                start_index = april_or_higher_indices[0]
            else:
                raise ExitNestedFunctions("Data for the time period specified is not available for this sensor")    
                
        # Find the end index (last occurrence)
        end_indices = file_import[file_import.iloc[:, 0].apply(lambda x: x.day == int(end_day) and x.month == int(end_month))].index
        
        if len(end_indices) > 0:
            end_index = end_indices[-1]
        else:
            # Find the last row before the month changes to June or higher
            june_or_higher_indices = file_import[file_import[0].apply(lambda x: x.month >= 6 and x.year == 2024)].index
            if len(june_or_higher_indices) > 0:
                end_index = june_or_higher_indices[0] - 1
            else:
                # If June or higher values are not available, use the last row of the data
                end_index = file_import.index[-1]
    
    
    ## solar e-cooker period ##
    else:  
        start_indices = file_import[file_import.iloc[:, 0].apply(lambda x: x.day == int(start_day) and x.month == int(start_month))].index
        
        if len(start_indices) > 0:
            start_index = start_indices[0]
        else:
            june_or_higher_indices = file_import[file_import[0].apply(lambda x: x.month >= 6 and x.year == 2024)].index
            if len(june_or_higher_indices) > 0:
                start_index = june_or_higher_indices[0]
            else:
                raise ExitNestedFunctions("Data for the time period specified is not available for this sensor")    
                
        # Find the end index (last occurrence)
        end_indices = file_import[file_import.iloc[:, 0].apply(lambda x: x.day == int(end_day) and x.month == int(end_month))].index
        
        if len(end_indices) > 0:
            end_index = end_indices[-1]
        else:
            # Find the last row before the month changes to June or higher
            sept_or_higher_indices = file_import[file_import[0].apply(lambda x: x.month >= 10 and x.year == 2024)].index
            if len(sept_or_higher_indices) > 0:
                end_index = sept_or_higher_indices[0] - 1
            else:
                # If June or higher values are not available, use the last row of the data
                end_index = file_import.index[-1]
        
    
    # end_indices = file_import[file_import.iloc[:, 0].apply(lambda x: x.day == end_day and x.month == end_month)].index
    # if len(end_indices) > 0:
    #     end_index = end_indices[-1]
    # else:
    #     # Find the last row before the month changes to April or higher
    #     april_or_higher_indices = file_import[file_import[0].apply(lambda x: x.month >= 4 and x.year == 24)].index
    #     if len(april_or_higher_indices) > 0:
    #         end_index = april_or_higher_indices[0] - 1
    #     else:
    #         # If April or higher values are not available, use the last row of the data
    #         end_index = file_import.index[-1]
    
    file_import = file_import.loc[start_index:end_index].reset_index(drop=True)

    columnnames = ['timestamp','value','unit','filename','label']
    data_file = pd.DataFrame(columns=columnnames)
        
    #data_file['timestamp'] = file_import.iloc[:, 0].apply(lambda x: tm.mktime(dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timetuple()))  #3732,3792,5018,5053,5073,5143
    #data_file['timestamp'] = file_import.iloc[:, 0].apply(lambda x: tm.mktime(dt.datetime.strptime(x, "%d/%m/%Y %H:%M").timetuple()))  #3693
    data_file['timestamp'] = file_import.iloc[:, 0].apply(lambda x: x.timestamp())
    data_file['value'] = file_import.iloc[:, 2].values  # column number in which data is locted in the excel/csv files
    data_file['unit'] = 'C'
    data_file['filename'] = filename.split('.')[0]
    data_file['label'] = 0
    
    ## Fill for missing timestamps in the middle ##
    for i in range(1,len(data_file)):
        if data_file['timestamp'].iloc[i]-data_file['timestamp'].iloc[i-1] != (15*60):    ## 15  minute frequency ##
            num_intervals = int((data_file['timestamp'].iloc[i] - data_file['timestamp'].iloc[i-1]) / (15*60))    ## 15  minute frequency ##
            for j in range(1,num_intervals):
                intermediate_timestamp = data_file['timestamp'].iloc[i-1] + j * (15*60)     ## 15  minute frequency ##           
                new_row = data_file.loc[i-1].copy()
                new_row['timestamp'] = intermediate_timestamp
                data_file = pd.concat([data_file.iloc[:(i-1) + j], pd.DataFrame([new_row]), data_file.iloc[(i-1) + j:]]).reset_index(drop=True)
                data_file['value'].iloc[(i-1)+j] = None
                data_file['unit'].iloc[(i-1)+j] = None
                data_file['filename'].iloc[(i-1)+j] = None
                data_file['label'].iloc[(i-1)+j] = None                
            i += num_intervals
                
    ## condition ## 
    ## if error values are encountered for two more weeks then cut back to the last value ##
    ## continue checking as sensor might get replaced and data is reliable again ##
    # Number of intervals in two weeks (14 days * 24 hours/day * 4 intervals/hour)
    # intervals_in_two_weeks = 14 * 24 * 4    
    
    # i = 0
    # n = len(data_file)

    # while i < n:
    #     if i + intervals_in_two_weeks <= n:
    #         # Check if the condition is met for the specified interval
    #         if all((data_file['value'][i:i+intervals_in_two_weeks] == 0) | 
    #                (data_file['value'][i:i+intervals_in_two_weeks] == 100) | 
    #                (data_file['value'][i:i+intervals_in_two_weeks] >= 32000)):
    #             data_file['value'][i:i+intervals_in_two_weeks] = None
    #             i += intervals_in_two_weeks  # Skip the next two weeks
    #         else:
    #             i += 1
    #     else:
    #         break    
                             
    return data_file

######################################################################################################################
### SPLIT DATA INTO SECTIONS BASED ON NaN VALUES
def data_sections(df):
    sections = []
    current_section = []
    for idx, row in df.iterrows():
        if pd.isna(row['value']):
            if current_section:
                sections.append(pd.DataFrame(current_section))
                current_section = []
        else:
            current_section.append(row.to_dict())
    if current_section:
        sections.append(pd.DataFrame(current_section))
    return sections

######################################################################################################################
### FUEL CODE ###
def air_quality(filename,filepath):
    
    selected_start_date = None
    selected_end_date = None
    input_file = os.path.join(filepath, filename)
    
    # write the date in the following format #   ## baseline period ##
    start_date = '27/11/23' #will start on this day 00.00
    end_date = '12/03/24' #will end on this day 23.59
    
    # # write the date in the following format #   ## embedding period ##
    # start_date = '01/04/24' #will start on this day 00.00
    # end_date = '31/05/24' #will end on this day 23.59
    
    start_day, start_month, start_year = start_date.split('/') # Extract the start day and month
    end_day, end_month, end_year = end_date.split('/') # Extract the end day and month
    
    # if int(start_month) >= 4 and int(start_year)==24:
    #     try:
    #         data_file,selected_start_date,selected_end_date = read_exact_embedding(input_file,filename,start_date,end_date)  # reads the data, fills for missing timestamps, remove error values
    #     except ExitNestedFunctions as e:
    #         print(e)
    #         raise # Re-raise the exception to be caught in the main script
    
    # else:
    #     data_file,selected_start_date,selected_end_date = read_exact_baseline(input_file,filename,start_date,end_date)  # reads the data, fills for missing timestamps, remove error values

    data_file = read_file(input_file,filename,start_date,end_date)  # reads the data, fills for missing timestamps, remove error values
    sections = data_sections(data_file) # divide the code into different based on any NaN values (returns datafile with the different sections)
    
    x = 0
    for idx, section in enumerate(sections):
    
        ### Code to remove error values from the end/start or entire length of the data ###    
        # Function to check if a value meets the conditions to be set to None
        def meets_conditions(value):
            return (value == 0) or (value == 100) or (value >= 32000)

        # Iterate from the end to the start of the DataFrame
        for i in range(len(section) - 1, -1, -1):
            value = section.loc[i, 'value']
            if meets_conditions(value):
                section.loc[i, 'value'] = None
            else:
                break  # Stop checking when the conditions are no longer valid
        
         # If the loop completes without breaking, check from the start to the end of the DataFrame
        if section['value'].notnull().all():
            for i in range(len(section)):
                value = section.loc[i, 'value']
                if meets_conditions(value):
                    section.loc[i, 'value'] = None
                else:
                    break  # Stop checking when the conditions are no longer valid
                    
        if section['value'].isnull().all():
            x += 1
        else: x = 0
            
    if x == len(sections): # all the section have None values
        # Check if all values in all the data is None
        selected_start_date="All error values for this sensor"
        selected_end_date=None
        total_days=None
        avg_air_quality_per_day=None
        min_hourly_air_quality=None
        max_hourly_air_quality=None
        #consumption_per_event=None

        #print("No reliable data for this sensor")
        print('Start Date:', selected_start_date) # chosen start date
        print('End Date:', selected_end_date) # chosen end date
        print('Total days:', total_days) #total number of days for all sections
        print('Avg. Air Quality/day:', avg_air_quality_per_day) 
        print('Min. Hourly Air Quality:', min_hourly_air_quality) 
        print('Max. Hourly Air Quality:', max_hourly_air_quality) 
        #print('Avg. Consumption/event:', avg_consumption_per_event)
        
        data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Air Quality/day:': [avg_air_quality_per_day],\
                'Min. Hourly Air Quality:': [min_hourly_air_quality],'Max. Hourly Air Quality:': [max_hourly_air_quality]}
    #data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Consumption/day:': [avg_consumption_per_day],'Avg. Consumption/event:': [avg_consumption_per_event]}

    else:
        number_of_days = []
        number_of_hours = []
        vector_avghour_section_air_quality = []
        vector_avghour_air_quality = []
        #min_air_quality_per_day = []
        #max_air_quality_per_day = []

        for idx, section in enumerate(sections):
            
            if section['value'].isnull().all():
                continue
                
            section = section.dropna(subset=['value'])
            start_date = datetime.fromtimestamp(section.loc[section['value'].notna(), 'timestamp'].iloc[0]) # (1) - start of the data for section
            end_date = datetime.fromtimestamp(section.loc[section['value'].notna(), 'timestamp'].iloc[-1]) # (2) - end of the data for section
            number_of_days.append(((end_date - start_date).total_seconds())/(24*3600))
            number_of_hours.append(((end_date - start_date).total_seconds())/(3600))
            
            if selected_start_date is None:
                selected_start_date = datetime.fromtimestamp(section['timestamp'].iloc[0])
            # Always set end_date to the end of the current section
            selected_end_date = datetime.fromtimestamp(section['timestamp'].iloc[-1])
            
            sections[idx]['value'] = section['value'] * pump_calibration_value
            
            total_hour_section_air_quality = section['value'].iloc[0]
            counter = 1
            for i in range(len(section)-1):
                if datetime.fromtimestamp(section['timestamp'].iloc[i]).hour == datetime.fromtimestamp(section['timestamp'].iloc[i+1]).hour:
                    total_hour_section_air_quality = total_hour_section_air_quality + section['value'].iloc[i+1]
                    counter += 1 
                else:
                    avg_hour_section_air_quality = total_hour_section_air_quality/counter
                    vector_avghour_section_air_quality.append(avg_hour_section_air_quality)
                    total_hour_section_air_quality = section['value'].iloc[i+1]
                    counter = 1            
            
            vector_avghour_air_quality.extend(vector_avghour_section_air_quality) # append the avg. from all the different sections
          
        total_days = sum(number_of_days) #in all the individual sections
        total_hours = sum(number_of_hours) #in all the individual sections        
        
        avg_air_quality_per_day = sum(vector_avghour_air_quality)/total_hours
        min_hourly_air_quality = min(vector_avghour_air_quality)
        max_hourly_air_quality = max(vector_avghour_air_quality)
        
        #print("No reliable data for this sensor")
        print('Start Date:', selected_start_date) # chosen start date
        print('End Date:', selected_end_date) # chosen end date
        print('Total days:', total_days) #total number of days for all sections
        print('Avg. Air Quality/day:', avg_air_quality_per_day) 
        print('Min. Hourly Air Quality:', min_hourly_air_quality) 
        print('Max. Hourly Air Quality:', max_hourly_air_quality) 
        #print('Avg. Consumption/event:', avg_consumption_per_event)
        
        data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Air Quality/day:': [avg_air_quality_per_day],\
                'Min. Hourly Air Quality:': [min_hourly_air_quality],'Max. Hourly Air Quality:': [max_hourly_air_quality]}
        #data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Consumption/day:': [avg_consumption_per_day],'Avg. Consumption/event:': [avg_consumption_per_event]}
    
    df = pd.DataFrame(data)
    return df

################################ LIST OF ALL THE SENSORS ################################
#### HAPEx ####
filename66 = "HAPEx 4569_1569_1014.xlsm"
filename67 = "HAPEx 4570_9086_1020.xlsm"
filename68 = "HAPEx 4571_3270_1006.xlsm"
filename69 = "HAPEx 4572_4469_9082_1010.xlsm"
filename70 = "HAPEx 4573_1003.xlsm"
filename71 = "HAPEx 4575_9090_1005.xlsm"
filename72 = "HAPEx 4576_9088_1019.xlsm"
filename73 = "HAPEx 4578_9092_1017.xlsm"
filename74 = "HAPEx 4587_9089_1018.xlsm"
filename75 = "HAPEx 4588_4438_1004.xlsm"
filename76 = "HAPEx 4590_1008.xlsm"
filename77 = "HAPEx 4591_1576_9084_1001.xlsm"
filename78 = "HAPEx 4593_3307_1013.xlsm"
filename79 = "HAPEx 4594_1016.xlsm"
filename80 = "HAPEx 4595_3311_1007.xlsm"
filename81 = "HAPEx 4610_1009.xlsm"
filename82 = "HAPEx 4612_3302_1012.xlsm"
filename83 = "HAPEx 4613_3327_4595_1011.xlsm"
filename84 = "HAPEx 4616_1002.xlsm"
filename85 = "HAPEx 4617_3289_4578_1015.xlsm"

pump_calibration_value = 2 # update value based on calibration of sensors with the pumps (2 is an approximation)

################################ MAIN SCRIPT ################################
hapex_filenames_list = [filename66,filename67,filename68,filename69,filename70,filename71,filename72,filename73,\
                  filename74,filename75,filename76,filename77,filename78,filename79,filename80,filename81,filename82,filename83,\
                  filename84,filename85]   #hapex files

#hapex_filenames_list = [filename66]   #hapex files

filepath = r"C:\Users\ashrafr7\OneDrive - Coventry University\Research Fellow Role (Feb - July 2024)\Data Analysis\Cookstove Sensor Data\Baseline Data Analysis\Full Baseline Data - sensors stitched"
os.chdir(filepath) #set working directory

workbook_path = 'Air Quality_HAPEX.xlsx'
wb = openpyxl.load_workbook(workbook_path)
sheet = wb['Sheet1']

# Convert start and end cells to row and column indices to be able to clear data from those rows
start_row = int('B2'[1:])
start_col = openpyxl.utils.column_index_from_string('B2'[0])
end_row = int('J21'[1:])
end_col = openpyxl.utils.column_index_from_string('J21'[0])

# Clear data in the specified range
for row in range(start_row, end_row + 1):
    for col in range(start_col, end_col + 1):
        sheet.cell(row=row, column=col).value = None

# Save the workbook
wb.save(workbook_path)

for i in range(len(hapex_filenames_list)):
    try:
        result = air_quality(hapex_filenames_list[i],filepath) 
    except ExitNestedFunctions:
        print("Data for the time period specified is not available for this sensor")
        sheet[f"{'B'}{i+2}"] = "Data for the time period specified is not available for this sensor" #start date
        continue  # Continue to the next iteration if start_date is not found
    
    sheet[f"{'B'}{i+2}"] = result.iloc[0,0] #start date
    sheet[f"{'C'}{i+2}"] = result.iloc[0,1] #end date
    sheet[f"{'D'}{i+2}"] = result.iloc[0,2] #total days
    sheet[f"{'E'}{i+2}"] = result.iloc[0,3] #avg. air quality conc. per day
    sheet[f"{'F'}{i+2}"] = result.iloc[0,4] #min. air quality conc. per day
    sheet[f"{'G'}{i+2}"] = result.iloc[0,5] #max. air quality conc. per day
    #sheet[f"{'H'}{i+2}"] = result.iloc[0,4] #avg. air quality conc. per event
      
wb.save(workbook_path)












