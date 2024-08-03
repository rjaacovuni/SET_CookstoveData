# -*- coding: utf-8 -*-
"""
Created on Tue Jul 30 22:55:59 2024
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
def read_exact(input_file,filename,start_date,end_date):
    
    start_day, start_month, start_year = start_date.split('/') #Extract the start day and month
    end_day, end_month, end_year = end_date.split('/') #Extract the end day and month
    
    # row_one = pd.read_excel(input_file,nrows=1) #read cvs file
    # if mt.isnan(row_one.iloc[0,1]):
    #     header_rows = 17  #if the first row is empty
    # else:
    #     header_rows = 17  #if the first row has data
        
    #file_import = pd.read_csv(inputfile,header = True, skiprows = header_rows, dtype ={'Timestamp':'string','3 stones Usage (EXACT 3693)':'string','Timestamp':'string'})
    #file_import = pd.read_excel(input_file, skiprows = header_rows)
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
    
    
    file_import = file_import.loc[start_index:end_index].reset_index(drop=True)
    
    
    #names(file_import) <-  c('timestamp', 'unit', 'value')
    
    # determine frequency of measurements from first 20 rows
    #time_test = file_import.head(20) #store first 20 rows
    
    #calculate whether time is four digits or six digits (just minutes or seconds also)
    #time_length = len(time_test.iloc[:,0][0].split(':')[1]) #detemrmine number of elements after semicolon
    #am_pm = any(grepl("AM|PM", time_test$timestamp))  # grepl is used to match two patterns of strings...this line checks if AM|PM is contained in the string
    # if time_length == 2:
    #     time_format = "%d-%m-%Y %H:%M"  #hour and minutes only
    # else:
    #     time_format = "%d-%m-%Y %H:%M:%S"  #hour, minutes and seconds only
    #file_import.iloc[:,0][0].strftime("%d-%m-%Y %H:%M")
    
    
    ##################### BITS OF CODE NOT NEEDED #####################
    columnnames = ['timestamp','value','unit','filename','label']
    data_file = pd.DataFrame(columns=columnnames)
        
    #data_file['timestamp'] = file_import.iloc[:, 0].apply(lambda x: tm.mktime(dt.datetime.strptime(x, "%Y-%m-%d %H:%M:%S").timetuple()))  #3732,3792,5018,5053,5073,5143
    #data_file['timestamp'] = file_import.iloc[:, 0].apply(lambda x: tm.mktime(dt.datetime.strptime(x, "%d/%m/%Y %H:%M").timetuple()))  #3693
    data_file['timestamp'] = file_import.iloc[:, 0].apply(lambda x: x.timestamp())
    data_file['value'] = file_import.iloc[:, 2].values
    data_file['unit'] = 'C'
    data_file['filename'] = filename.split('.')[0]
    data_file['label'] = 0
    
    ## Fill for missing timestamps in the middle ##
    for i in range(1,len(data_file)):
        if data_file['timestamp'].iloc[i]-data_file['timestamp'].iloc[i-1] != 480:
            num_intervals = int((data_file['timestamp'].iloc[i] - data_file['timestamp'].iloc[i-1]) / 480)
            for j in range(1,num_intervals):
                intermediate_timestamp = data_file['timestamp'].iloc[i-1] + j * 480                
                new_row = data_file.loc[i-1].copy()
                new_row['timestamp'] = intermediate_timestamp
                data_file = pd.concat([data_file.iloc[:(i-1) + j], pd.DataFrame([new_row]), data_file.iloc[(i-1) + j:]]).reset_index(drop=True)
                data_file['value'].iloc[(i-1)+j] = None
                data_file['unit'].iloc[(i-1)+j] = None
                data_file['filename'].iloc[(i-1)+j] = None
                data_file['label'].iloc[(i-1)+j] = None                
            i += num_intervals
    
    ## Code to select the sections of good data ##
    #data_file['value'] = data_file['value'].replace(100, pd.NA)
    
    return data_file

######################################################################################################################
### EST. SAMPLE INTERVAL ###
def est_sample_interval(data_file):
    data_file['timestamp'] = pd.to_datetime(data_file['timestamp'],unit='s')
    sorted_timestamp = data_file['timestamp'].sort_values()
    difftimes = (sorted_timestamp - sorted_timestamp.shift(1)).dt.total_seconds().dropna()
    sample_interval = difftimes.median()
    return sample_interval

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
### FIREFINDER ALGORITHM ###
def firefinder_detector(section,sample_interval,primary_threshold,min_event_temp,min_event_sec,min_break_sec,max_run_length,lower_temp_range,upper_temp_range):
    #numeric_value = np.nan if min_event_temp is None else min_event_temp
    
    #data_table = data_file.to_numpy() #convert data frame to data table
    #data_table_copy = data_table.copy()
    
    #CALCULATE FEATURES
    ### EST. SAMPLE INTERVAL ###
    #sample_interval = est_sample_interval(data_file['timestamp'])
    sample_interval_mins = sample_interval/60
    
    #make a column of 1st derivative (degC/minute) - temperature change per minute
    section['difftemps'] = section['value'].diff() # Calculate differences
    section['difftemps'] = section['difftemps']/sample_interval_mins # Divide by sample_interval_mins
    section['difftemps'].fillna(0, inplace=True) # Fill NaN with 0 (for the first row)
      
    #make a column of delta timestamps
    section['difftimes'] = section['timestamp'].diff().dt.total_seconds() # Calculate differences between timestamps in seconds
    section['difftimes'].fillna(0, inplace=True) # Fill NaN with 0 (for the first row)
    
    #look at whether or not most of the data coming up in the next 
    #hour is negative slope or 100 data points, whichever is lower
    def rolling_quantile(section, window_size, quantile):
        rolling = section.rolling(window=window_size, min_periods=1)
        return rolling.quantile(quantile)
    
    window_size = min(max_run_length, min(round(60/sample_interval_mins),len(section))) # Calculate the window size
    
    # Calculate quantile_difftemps
    if len(section) > 1:
        quantile_difftemps = rolling_quantile(section['difftemps'], window_size, 0.8) #0.8 = 80th percentile
    else:
        quantile_difftemps = None
    
    section['quantile_difftemps'] = quantile_difftemps # Assign quantile_difftemps to the DataFrame
    
    #RUN THE DECISION TREE
    section['event_raw'] = False #just assume there is no cooking to start
    section.loc[section['value'] > primary_threshold, 'event_raw'] = True #define points that are likely to be cooking
    section.loc[section['quantile_difftemps'] < 0, 'event_raw'] = False #get rid of long runs of negative slopes
    section.loc[section['difftemps'] > 2, 'event_raw'] = True #assume cooking for highly positive slopes
    section.loc[section['difftemps'] < -1*section['value']/500, 'event_raw'] = False #get rid of highly negative slopes
    section.loc[section['difftimes'] > sample_interval, 'event_raw'] = False #remove places with gaps longer than the sample interval
    
    section['event_raw'] = smooth_events(section['event_raw'], sample_interval, min_event_sec, min_break_sec)
    
    if min_event_temp != None:
        section['event_num'] = number_events(section['event_raw']) #events are given numbers and non-events are given NA
        section['event_max'] = section.groupby('event_num')['value'].transform('max')
        section.loc[section['event_max'] < min_event_temp, 'event_raw'] = False
    
    section.loc[~((section['value'] < upper_temp_range) & (section['value'] > lower_temp_range)), 'event_raw'] = False
    
    return section


######################################################################################################################
### SMOOTH EVENTS ###
#' Reduce "blipiness" in event indicators by eliminating small cooking events and gaps
def smooth_events(event_raw, sample_interval, min_event_sec, min_break_sec):
    sample_interval = sample_interval[0] if isinstance(sample_interval, list) else sample_interval
    
    # Run-length encoding
    rl_obj = [(k, sum(1 for _ in g)) for k, g in groupby(event_raw)]
    # Convert to numpy array for easier manipulation
    values = [k for k, _ in rl_obj] #contains event_raw values True or False
    lengths = [length for _, length in rl_obj] #contains number of times each values occurs together
    
    # Remove short breaks between cooking events (a small break would still be considered in the cooking event)
    for i in range(len(values)):
        if values[i] == False and lengths[i] * sample_interval < min_break_sec:
            values[i] = True
    # Invert the run-length encoding
    event_raw = [k for k, l in zip(values, lengths) for _ in range(l)]
     
    # Run-length encoding again
    rl_obj2 = [(k, sum(1 for _ in g)) for k, g in groupby(event_raw)]
    # Convert to numpy array for easier manipulation
    values = [k for k, _ in rl_obj2] #contains event_raw values True or False
    lengths = [length for _, length in rl_obj2] #contains number of times each values occurs together
    
    # Remove short cooking events
    for i in range(len(values)):
        if values[i] == True and lengths[i] * sample_interval < min_event_sec:
            values[i] = False
    # Invert the second run-length encoding
    event_raw = [k for k, l in zip(values, lengths) for _ in range(l)]    
    
    return event_raw

######################################################################################################################
### NUMBER EVENTS ###
#' Generate unique identifiers for each event, based on indicator of being in an event.
#' Nonevent periods are labelled NA
def number_events(event_raw):
    runs = [(k, sum(1 for _ in g)) for k, g in groupby(event_raw)]
    # Convert to numpy array for easier manipulation
    values = [k for k, _ in runs] #contains event_raw values True or False
    lengths = [length for _, length in runs] #contains number of times each values occurs together
    
    for i in range(len(values)):
        if values[i] == None:
            values[i] = False
    
    cumulative_sum = 0
    new_values = []
    for i in range(len(values)):
        if values[i] == True:
            cumulative_sum += 1
            new_values.append(cumulative_sum)
        else:
            new_values.append(np.nan)
            
    values = new_values
    event_nums = [k for k, l in zip(values, lengths) for _ in range(l)]  
    
    return event_nums

######################################################################################################################
### LIST EVENTS ###
#' Generate a list of events
def list_events(section,sample_interval,event=None):
    
    start_date = section.loc[section['value'].notna(), 'timestamp'].iloc[0] # (1) - start of the data for section
    end_date = section.loc[section['value'].notna(), 'timestamp'].iloc[-1] # (2) - end of the data for section
    section_days = ((end_date - start_date).total_seconds())/(24*3600)
    
    if event is None:
        event = section['label']
    
    labeled_data = section.copy()
    #labeled_data['event_num'] = number_events(event) #didn't understand this code as they had set event to None
    events = labeled_data.dropna(subset=['event_num']).groupby('event_num').apply(lambda x: pd.Series({
        'start_time': x['timestamp'].min(),
        'stop_time': (x['timestamp'] + pd.to_timedelta(sample_interval, unit='s')).max(),
        'min_temp': x['value'].min(),
        'max_temp': x['value'].max(),
    }))
    
    events['duration_mins'] = (events['stop_time'] - events['start_time']).dt.total_seconds()/60 #calculates duration of each event in minutes
    return section_days,events

######################################################################################################################
### SUMMARISE EVENTS ###
#' Generate basic event summaries
def summarize_events(list_of_events):
    summaries = list_of_events.groupby('filename').agg(total_duration_mins=('duration_mins','sum'),
                                           nevents=('event_num','count'))
    return summaries


######################################################################################################################
### MAIN CODE ###
def cookingevents(filename,filepath):
    
    selected_start_date = None
    selected_end_date = None
    input_file = os.path.join(filepath, filename)
    
    primary_threshold = 50 #from literature (pg. 10 Wilson paper)
    min_event_temp = 50 #remove values with very low cooking temperatures (maximum value of event cannot be less than this)
    min_event_sec = 10*60 #minimum event duration
    min_break_sec = 60*60 #minimum break between cooking events
    max_run_length = 100 
    lower_temp_range = 0 #based on my data
    upper_temp_range = 140 #based on my data
    
    # write the date in the following format #   ## baseline period ##
    start_date = '27/11/23' #will start on this day 00.00
    end_date = '12/03/24' #will end on this day 23.59
    
    # # write the date in the following format #   ## embedding period ##
    # start_date = '01/04/24' #will start on this day 00.00
    # end_date = '31/05/24' #will end on this day 23.59
    
    start_day, start_month, start_year = start_date.split('/') #Extract the start day and month
    end_day, end_month, end_year = end_date.split('/') #Extract the end day and month
    
    data_file = read_exact(input_file,filename,start_date,end_date)  # reads the data, fills for missing timestamps, remove error values
    
    sample_interval = est_sample_interval(data_file)
    sections = data_sections(data_file) # divide the code into different based on any NaN values (returns datafile with the different sections)

    x = 0
    for idx, section in enumerate(sections): 

        ### Code to remove error values from the end/start or entire length of the data ###    
        # Function to check if a value meets the conditions to be set to None
        def meets_conditions(value):
            return (value == None)      
        
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
        
        # condition 2 - moving average
        window_size = 100
        section['Rolling_Mean'] = section['value'].rolling(window=window_size, min_periods=1).mean() # Calculate a rolling mean with a specified window size, ignoring NaN values
        section['Rolling_Mean_Diff'] = section['Rolling_Mean'].diff() # Calculate the difference between rolling means
        max_value_before_decline = section['value'].cummax() # Find the maximum value before any decline
        section['Below_Max'] = section['value'] < max_value_before_decline # Identify periods where value is consistently below the max_value_before_decline
        declining_start_index = None # Find the first consistent declining trend
        
        for i in range(len(section) - window_size):
            if section['Below_Max'].iloc[i]:
                # Check if the values remain below the previous maximum (a 10 drop is okay) after this point
                if (section['value'].iloc[i+1:] < (max_value_before_decline.iloc[i]-10)).all():
                    # Ensure we are not simply seeing constant values
                    recent_values = section['value'].iloc[i+1:i+window_size]
                    constant_values = recent_values.nunique() == 1

                    # Check if there is a decline (values are dropping) and not just constant values
                    if not constant_values:
                        # Ensure we keep values greater than 0 that directly follow after the max value
                        while i < len(section) and section['value'].iloc[i] > 0:
                            i += 1
                        declining_start_index = i
                        break
        
        # Remove rows after the start of the consistent declining trend
        if declining_start_index is not None:
            section = section.iloc[:declining_start_index].reset_index(drop=True)
        
        section.drop(columns=['Rolling_Mean', 'Rolling_Mean_Diff', 'Below_Max']) # Drop the helper columns used for the calculations    
        sections[idx] = section
    
        # condition 2 - check if the data frame contains all values less than or equal to 20 or NaN values
        #valid_or_nan_count = ((section['value'] <= 20) | section['value'].isna()).sum()
        if section['value'].isnull().all():
            x += 1     ##all values in the section are not useful
        else: x = 0 
   
    #Check if all values in all the sections is None
    if x == len(sections):
        selected_start_date="All error values for this sensor"
        selected_end_date=None
        total_days=None
        avg_event_per_day=None
        min_event_per_day=None
        max_event_per_day=None
        avg_duration_per_event=None
        min_duration_per_event=None
        max_duration_per_event=None
        avg_duration_per_day=None
        min_duration_per_day=None
        max_duration_per_day=None
        
        #print("No reliable data for this sensor")
        print('Start Date:', selected_start_date) #chosen start date
        print('End Date:', selected_end_date) #chosen end date
        print('Total days:', total_days) #total number of days for all sections
        print('Avg.event/day:', avg_event_per_day) 
        print('Min.event/day:', min_event_per_day)
        print('Max.event/day:', max_event_per_day)
        print('Avg.duration/event:', avg_duration_per_event)
        print('Min.duration/event:', min_duration_per_event)
        print('Max.duration/event:', max_duration_per_event)
        print('Avg.duration/day:', avg_duration_per_day)
        print('Min.duration/day:', min_duration_per_day)
        print('Max.duration/day:', max_duration_per_day)
        
        data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg.event/day:': [avg_event_per_day],'Min.event/day:': [min_event_per_day], \
                'Max.event/day:': [max_event_per_day],'Avg.duration/event:': [avg_duration_per_event],'Min.duration/event:': [min_duration_per_event], \
                'Max.duration/event:': [max_duration_per_event],'Avg.duration/day:': [avg_duration_per_day],'Min.duration/day:': [min_duration_per_day], \
                'Max.duration/day:': [max_duration_per_day]}
            
   
    else:        
        number_of_days = []
        section_events = []
        for idx, section in enumerate(sections):
        
            if section['value'].isnull().all():
                continue    
            
            section = section.dropna(subset=['value']) # Remove rows with NaN values
            section = firefinder_detector(section,sample_interval,primary_threshold,min_event_temp,min_event_sec,min_break_sec,max_run_length,lower_temp_range,upper_temp_range) #determine cooking instance for data filtering

            #no_events_detected = False
            if selected_start_date is None:
                selected_start_date = section['timestamp'].iloc[0]
            # Always set end_date to the end of the current section
            selected_end_date = section['timestamp'].iloc[-1]
            
            if section['event_raw'].eq(False).all():  # no events in the section #
                start_date = section.loc[section['value'].notna(), 'timestamp'].iloc[0] # (1) - start of the data for section
                end_date = section.loc[section['value'].notna(), 'timestamp'].iloc[-1] # (2) - end of the data for section
                number_of_days.append(((end_date - start_date).total_seconds())/(24*3600))
                #no_events_detected = True
            else:
                [section_days,events] = list_events(section,sample_interval)
                number_of_days.append(section_days)
                section_events.append(events)
                
        total_days = sum(number_of_days)     
         
         # Check if there are no events identified
        if section_events == []:
             avg_event_per_day=0
             min_event_per_day=0
             max_event_per_day=0
             avg_duration_per_event=0
             min_duration_per_event=0
             max_duration_per_event=0
             avg_duration_per_day=0
             min_duration_per_day=0
             max_duration_per_day=0
             
             #print("No events identified for this sensor")
             print('Start Date:', selected_start_date) # chosen start date
             print('End Date:', selected_end_date) # chosen end date
             print('Total days:', total_days) #total number of days for all sections
             print('Avg.event/day:', avg_event_per_day) 
             print('Min.event/day:', min_event_per_day)
             print('Max.event/day:', max_event_per_day)
             print('Avg.duration/event:', avg_duration_per_event)
             print('Min.duration/event:', min_duration_per_event)
             print('Max.duration/event:', max_duration_per_event)
             print('Avg.duration/day:', avg_duration_per_day)
             print('Min.duration/day:', min_duration_per_day)
             print('Max.duration/day:', max_duration_per_day)
             
             data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg.event/day:': [avg_event_per_day],'Min.event/day:': [min_event_per_day], \
                     'Max.event/day:': [max_event_per_day],'Avg.duration/event:': [avg_duration_per_event],'Min.duration/event:': [min_duration_per_event], \
                     'Max.duration/event:': [max_duration_per_event],'Avg.duration/day:': [avg_duration_per_day],'Min.duration/day:': [min_duration_per_day], \
                     'Max.duration/day:': [max_duration_per_day]}
                 
                 
        else:
            list_of_events = pd.concat(section_events, ignore_index=True)
            dates = list_of_events['start_time'].dt.day 
            
            events_per_day = []
            total_duration_per_day = []
            events = 1
            total_duration = list_of_events['duration_mins'].iloc[0]
                
            if len(list_of_events) == 1:
                events_per_day.append(events)
                total_duration_per_day.append(total_duration)
            
            else:
                # days at the start of the data when there are no events #
                # if list_of_events['start_time'].iloc[0].date() != start_date.date():
                #     no_event_days = (list_of_events['start_time'].iloc[0].date() - start_date.date()).days
                #     events_per_day = [0] * no_event_days + events_per_day[no_event_days:]  #adding the number of zeros of no event days to the start of the vector
                #     total_duration_per_day = [0] * no_event_days + total_duration_per_day[no_event_days:] #adding duration of zero for those days
            
                # events in the middle of data #
                for i in range(1,len(list_of_events)):
                    if dates.iloc[i] == dates.iloc[i-1]:  #getting the total event durations for the day - multiple events in a day
                        events += 1  #this means that there is more than one event in a day
                        total_duration = total_duration + list_of_events['duration_mins'].iloc[i] #adding event durations together for multiple events in a day
                    else:    
                        events_per_day.append(events)
                        total_duration_per_day.append(total_duration)
                        events = 1 #start of new day
                        total_duration = list_of_events['duration_mins'].iloc[i] #adding duration of the new day's event
                
                events_per_day.append(events)
                total_duration_per_day.append(total_duration)
                
            ### AVERAGE VALUES FOR SENSORS BASED ON ALL SECTIONS ###
            avg_event_per_day = sum(events_per_day)/total_days # (4)
            min_event_per_day = min(events_per_day) if total_days - len(events_per_day) == 0 else 0 # (5)
            max_event_per_day = max(events_per_day) # (6)
            #min_temperature = min(list_of_events['min_temp']) # (7)
            #max_temperature = max(list_of_events['max_temp']) # (8)
            #avg_temperature = (min_temperature + max_temperature)/2 # (9)
            avg_duration_per_event = (sum(list_of_events['duration_mins'])/len(list_of_events))/60 # (10) hrs/day
            min_duration_per_event = (min(list_of_events['duration_mins']))/60 # (11) hrs/day
            max_duration_per_event = (max(list_of_events['duration_mins']))/60 # (12) hrs/day
            avg_duration_per_day = (sum(total_duration_per_day)/total_days)/60 # (13) hrs/day
            min_duration_per_day = (min(total_duration_per_day))/60 if total_days - len(events_per_day) == 0 else 0 # (14) hrs/day
            max_duration_per_day = (max(total_duration_per_day))/60 # (15) hrs/day     
            
            #print(start_date)
            #print(end_date)
            print('Start Date:', selected_start_date) # chosen start date
            print('End Date:', selected_end_date) # chosen end date
            print('Total days:', total_days) #total number of days for all sections
            print('Avg.event/day:', avg_event_per_day) 
            print('Min.event/day:', min_event_per_day)
            print('Max.event/day:', max_event_per_day)
            print('Avg.duration/event:', avg_duration_per_event)
            print('Min.duration/event:', min_duration_per_event)
            print('Max.duration/event:', max_duration_per_event)
            print('Avg.duration/day:', avg_duration_per_day)
            print('Min.duration/day:', min_duration_per_day)
            print('Max.duration/day:', max_duration_per_day)
            print('List of Events', list_of_events)
            
            data = {'Start Date:': [selected_start_date],'End Date:': [selected_end_date],'Total days:': [total_days],'Avg.event/day:': [avg_event_per_day],'Min.event/day:': [min_event_per_day], \
                    'Max.event/day:': [max_event_per_day],'Avg.duration/event:': [avg_duration_per_event],'Min.duration/event:': [min_duration_per_event], \
                    'Max.duration/event:': [max_duration_per_event],'Avg.duration/day:': [avg_duration_per_day],'Min.duration/day:': [min_duration_per_day], \
                    'Max.duration/day:': [max_duration_per_day]}
                
    df = pd.DataFrame(data)
    return df
 
    
######################################################################################################################
### MAIN CODE ###

filepath = r"C:\Users\ashrafr7\OneDrive - Coventry University\Research Fellow Role (Feb - July 2024)\Data Analysis\Cookstove Sensor Data\Baseline Data Analysis\Full Baseline Data - sensors stitched"
os.chdir(filepath) #set working directory

filename1 = "EXACT 3693_1007.xlsm"
filename2 = "EXACT 3709_5202_1001.xlsm"
filename3 = "EXACT 3732_1002.xlsm"
filename4 = "EXACT 3792_1019.xlsm"
filename5 = "EXACT 5018_1017.xlsm"
filename6 = "EXACT 5046_1010.xlsm"
filename7 = "EXACT 5053_1014.xlsm"
filename8 = "EXACT 5073_1015.xlsm"
filename9 = "EXACT 5143_1003.xlsm"

filenames_list = [filename1,filename2,filename3,filename4,filename5,filename6,filename7,filename8,filename9]

#filenames_list = [filename1]
    
workbook_path = 'Cooking_Events_EXACT.xlsx'
wb = openpyxl.load_workbook(workbook_path)
sheet = wb['Sheet1']

# Convert start and end cells to row and column indices
start_row = int('B2'[1:])
start_col = openpyxl.utils.column_index_from_string('B2'[0])
end_row = int('M10'[1:])
end_col = openpyxl.utils.column_index_from_string('M10'[0])

# Clear data in the specified range
for row in range(start_row, end_row + 1):
    for col in range(start_col, end_col + 1):
        sheet.cell(row=row, column=col).value = None

# Save the workbook
wb.save(workbook_path)

for i in range(len(filenames_list)):
    try:
        result = cookingevents(filenames_list[i],filepath)
    except ExitNestedFunctions:
        print("Data for the time period specified is not available for this sensor")
        sheet[f"{'B'}{i+2}"] = "Data for the time period specified is not available for this sensor" #start date
        continue  # Continue to the next iteration if start_date is not found
    
    sheet[f"{'B'}{i+2}"] = result.iloc[0,0] #start date
    sheet[f"{'C'}{i+2}"] = result.iloc[0,1] #end date
    sheet[f"{'D'}{i+2}"] = result.iloc[0,2] #total days
    sheet[f"{'E'}{i+2}"] = result.iloc[0,3] #avg. event per day
    sheet[f"{'F'}{i+2}"] = result.iloc[0,4] #min. event per day
    sheet[f"{'G'}{i+2}"] = result.iloc[0,5] #max. event per day
    sheet[f"{'H'}{i+2}"] = result.iloc[0,6] #avg. duration per event
    sheet[f"{'I'}{i+2}"] = result.iloc[0,7] #min. duration per event
    sheet[f"{'J'}{i+2}"] = result.iloc[0,8] #max. duration per event
    sheet[f"{'K'}{i+2}"] = result.iloc[0,9] #avg. duration per day
    sheet[f"{'L'}{i+2}"] = result.iloc[0,10] #min. duration per day
    sheet[f"{'M'}{i+2}"] = result.iloc[0,11] #max. duration per day
      
wb.save(workbook_path)
 




















