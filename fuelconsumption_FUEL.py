# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 15:48:11 2024
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
    data_file['value'] = file_import.iloc[:, 3].values
    data_file['unit'] = 'C'
    data_file['filename'] = filename.split('.')[0]
    data_file['label'] = 0
    
    ## Fill for missing timestamps in the middle ##
    ## Fill for missing timestamps in the middle ##
    for i in range(1,len(data_file)):
        if data_file['timestamp'].iloc[i]-data_file['timestamp'].iloc[i-1] != 60:   ## 1  minute frequency ##
            num_intervals = int((data_file['timestamp'].iloc[i] - data_file['timestamp'].iloc[i-1]) / 60)   ## 1  minute frequency ##
            for j in range(1,num_intervals):
                intermediate_timestamp = data_file['timestamp'].iloc[i-1] + j * 60      ## 1  minute frequency ##          
                new_row = data_file.loc[i-1].copy()
                new_row['timestamp'] = intermediate_timestamp
                data_file = pd.concat([data_file.iloc[:(i-1) + j], pd.DataFrame([new_row]), data_file.iloc[(i-1) + j:]]).reset_index(drop=True)
                data_file['value'].iloc[(i-1)+j] = None
                data_file['unit'].iloc[(i-1)+j] = None
                data_file['filename'].iloc[(i-1)+j] = None
                data_file['label'].iloc[(i-1)+j] = None                
            i += num_intervals
    
                      
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
def fuel_consumption(filename,filepath):
    
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
    
        ## if a value of -5 is detected then all values after that are set to None so they can be removed ##
        mask = (section['value'] < -5).cumsum() # Step 1: Create a boolean mask where the condition is met
        section.loc[mask > 0, 'value'] = None # Step 2: Set values to None where mask is greater than 0
                    
        if section['value'].isnull().all():
            x += 1
        else: x = 0

    # Check if all values in all the sections is None
    if x == len(sections): # all the section have None values
        # Check if all values in all the data is None    
        selected_start_date="All error values for this sensor"
        selected_end_date=None
        total_days=None
        avg_consumption_per_day=None
        min_consumption_per_day=None
        max_consumption_per_day=None
        #consumption_per_event=None

        #print("No reliable data for this sensor")
        print('Start Date:', selected_start_date) # chosen start date
        print('End Date:', selected_end_date) # chosen end date
        print('Total days:', total_days) #total number of days for all sections
        print('Avg. Consumption/day:', avg_consumption_per_day) 
        print('Min. Consumption/day:', min_consumption_per_day) 
        print('Max. Consumption/day:', max_consumption_per_day)       
        #print('Avg. Consumption/event:', avg_consumption_per_event)
        
        data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Consumption/day:': [avg_consumption_per_day],\
                'Min. Consumption/day:': [min_consumption_per_day],'Max. Consumption/day:': [max_consumption_per_day]}
        #data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Consumption/day:': [avg_consumption_per_day],'Avg. Consumption/event:': [avg_consumption_per_event]}

    else:
        number_of_days = []
        vector_avg_section_consumption_per_day = []
        vector_min_section_consumption_per_day = []
        vector_max_section_consumption_per_day = []
        #total_days
        
        for idx, section in enumerate(sections):
        
            if section['value'].isnull().all():
                continue    
        
            section = section.dropna(subset=['value']) # Remove rows with NaN values
            section['difference'] = section['value'].diff() # Calculate the difference between consecutive values in the 'value' column
            section = section.dropna() # Remove the first row as it will have NaN after diff
            
            section['shifted_value'] = section['value'].shift(-1) # Shift the 'value' column up by one position
            section['difference'] = section['value'] - section['shifted_value'] # Calculate the difference as [i] - [i+1]
            section = section.dropna(subset=['shifted_value']) # Remove the last row as it will have NaN after the shift
            section = section.drop(columns=['shifted_value']) # Drop the auxiliary 'shifted_value' column
            
            section['difference'] = section['difference'].apply(lambda x: 0 if -0.025 <= x <= 0.025 else x) # converting values <0.025 and <-0.025 to zero so those small changes are not considered (25g)
            positive_sum = section['difference'][section['difference'] > 0].sum() # Sum all the positive values in the 'difference' column
            
            start_date = datetime.fromtimestamp(section.loc[section['value'].notna(), 'timestamp'].iloc[0]) # (1) - start of the data for section
            end_date = datetime.fromtimestamp(section.loc[section['value'].notna(), 'timestamp'].iloc[-1]) # (2) - end of the data for section
            number_of_days.append(((end_date - start_date).total_seconds())/(24*3600))
            
            if selected_start_date is None:
                selected_start_date = datetime.fromtimestamp(section['timestamp'].iloc[0])
            # Always set end_date to the end of the current section
            selected_end_date = datetime.fromtimestamp(section['timestamp'].iloc[-1])
            
            avg_section_consumption_per_day = positive_sum/(((end_date - start_date).total_seconds())/(24*3600))
            vector_avg_section_consumption_per_day.append(avg_section_consumption_per_day)
            
            if section.loc[section['difference'] > 0].index.tolist() == []:
                vector_min_section_consumption_per_day.append(0)
                vector_max_section_consumption_per_day.append(0)
            
            else:
                vector_min_section_consumption_per_day.append(section['difference'][section['difference'] > 0].min())
                vector_max_section_consumption_per_day.append(section['difference'][section['difference'] > 0].max())
        
        total_days = sum(number_of_days) #in all the individual sections
        avg_consumption_per_day = sum(vector_avg_section_consumption_per_day)/len(vector_avg_section_consumption_per_day)
        
        min_consumption_per_day = min(vector_min_section_consumption_per_day)
        max_consumption_per_day = max(vector_max_section_consumption_per_day)
        
        #print("No reliable data for this sensor")
        print('Start Date:', selected_start_date) # chosen start date
        print('End Date:', selected_end_date) # chosen end date
        print('Total days:', total_days) #total number of days for all sections
        print('Avg. Consumption/day:', avg_consumption_per_day) 
        print('Min. Consumption/day:', min_consumption_per_day) 
        print('Max. Consumption/day:', max_consumption_per_day) 
        #print('Avg. Consumption/event:', avg_consumption_per_event)
        
        data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Consumption/day:': [avg_consumption_per_day],\
                'Min. Consumption/day:': [min_consumption_per_day],'Max. Consumption/day:': [max_consumption_per_day]}
        #data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg. Consumption/day:': [avg_consumption_per_day],'Avg. Consumption/event:': [avg_consumption_per_event]}
    
    df = pd.DataFrame(data)
    return df

################################ LIST OF ALL THE SENSORS ################################
#### FUEL ####
filename40 = "FUELv2 4561_1019.xlsm"
filename41 = "FUELv2 4564_1001.xlsm"
filename42 = "FUELv2 4567_1020.xlsm"
filename43 = "FUELv2 4581_1018.xlsm"
filename44 = "FUELv2 4584_1007.xlsm"
filename45 = "FUELv2 4585_1014.xlsm"
filename46 = "FUELv2 4586_1013.xlsm"
filename47 = "FUELv2 4600_1016.xlsm"
filename48 = "FUELv2 4604_1015.xlsm"
filename49 = "FUELv2 4606_1004.xlsm"
filename50 = "FUELv2 4620_1020.xlsm"
filename51 = "FUELv2 4621_1013.xlsm"
filename52 = "FUELv2 4623_1016.xlsm"
filename53 = "FUELv2 4640_1009.xlsm"
filename54 = "FUELv2 4641_1008.xlsm"
filename55 = "FUELv2 4642_1012.xlsm"
filename56 = "FUELv2 4643_1003.xlsm"
filename57 = "FUELv2 4645_1017.xlsm"
filename58 = "FUELv2 4646_4566_1005.xlsm"
filename59 = "FUELv2 4662_1008.xlsm"
filename60 = "FUELv2 4665_1005.xlsm"
filename61 = "FUELv2 4666_1010.xlsm"
filename62 = "FUELv2 4680_1006.xlsm"
filename63 = "FUELv2 4682_1011.xlsm"
filename64 = "FUELv2 4685_1002.xlsm"

################################ MAIN SCRIPT ################################
fuel_filenames_list = [filename40,filename41,filename42,filename43,filename44,filename45,filename46,filename47,filename48,filename49,filename50,\
                  filename51,filename52,filename53,filename54,filename55,filename56,filename57,filename58,filename59,filename60,\
                  filename61,filename62,filename63,filename64]   #fuel files
    
#fuel_filenames_list = [filename64]   #fuel files

filepath = r"C:\Users\ashrafr7\OneDrive - Coventry University\Research Fellow Role (Feb - July 2024)\Data Analysis\Cookstove Sensor Data\Baseline Data Analysis\Full Baseline Data - sensors stitched"
os.chdir(filepath) #set working directory
    
workbook_path = 'Fuel Consumption_FUEL.xlsx'
wb = openpyxl.load_workbook(workbook_path)
sheet = wb['Sheet1']

# Convert start and end cells to row and column indices to be able to clear data from those rows
start_row = int('B2'[1:])
start_col = openpyxl.utils.column_index_from_string('B2'[0])
end_row = int('J26'[1:])
end_col = openpyxl.utils.column_index_from_string('J26'[0])

# Clear data in the specified range
for row in range(start_row, end_row + 1):
    for col in range(start_col, end_col + 1):
        sheet.cell(row=row, column=col).value = None

# Save the workbook
wb.save(workbook_path)
            
for i in range(len(fuel_filenames_list)):
    try:
        result = fuel_consumption(fuel_filenames_list[i],filepath)
    except ExitNestedFunctions:
        print("Data for the time period specified is not available for this sensor")
        sheet[f"{'B'}{i+2}"] = "Data for the time period specified is not available for this sensor" #start date
        continue  # Continue to the next iteration if start_date is not found
     
    sheet[f"{'B'}{i+2}"] = result.iloc[0,0] #start date
    sheet[f"{'C'}{i+2}"] = result.iloc[0,1] #end date
    sheet[f"{'D'}{i+2}"] = result.iloc[0,2] #total days
    sheet[f"{'E'}{i+2}"] = result.iloc[0,3] #avg. fuel consumption per day
    sheet[f"{'F'}{i+2}"] = result.iloc[0,4] #min. fuel consumption per day
    sheet[f"{'G'}{i+2}"] = result.iloc[0,5] #max. fuel consumption per day
    #sheet[f"{'H'}{i+2}"] = result.iloc[0,4] #avg. fuel consumption per event
      
wb.save(workbook_path)
























