# SET_CookstoveData
This project analyses data from sensors installed in twenty households in Rwanda. The project was completed by Coventry University in colloboration with Rwanda Energy Group (REG) and MeshPower Ltd. Further details about the project can be found here: https://www.coventry.ac.uk/research/research-directories/current-projects/2022/set/

This repository provides the different code files used to analyse data from the sensors. Four type of sensors were installed:
- EXACT infrared sensor installed on the three-stone fire stoves to measure temperature
- EXACTv2 thermocouple sensor installed on the mudstoves, charcoal and improved biomass stoves to measure temperature
- FUEL sensors installed on the firewood and charcoal fuel baskets to measure fuel consumption
- HAPEx sensors installed in the kitchen to measure prticulate matter concentration

Data from different collections for each sensor was stitched together in one file. Data from the fuel consumption and air quality sensors are pre-filtered. Raw and preprocessed data files are available via this link [..........]. The date range to be analysed can be specified in the code by the user. The model iterates through all the sensors in a for loop and the results are copied into an Excel file. For each type of sensor, the sensor file name can be updated. The file path where the preprocessed data files are located is specified. The file names in the code are checked to see they match the Excel file names.

infrared_EXACT.py and thermocouple_EXACTv2.py
1. Parameters for the cooking event algorithm and the start and end date are specified in the 'cookingevents' function.
2. If the data contains any erronous values, those values are removed and data is broken into sections. The sections are then stitched together and analysed as a whole.
4. Parameters reported are the average, maximum and minimum values of events per day, duration of events per day and duration per event.

fuelconsumption_FUEL.py
1. Start and end date are specified in the 'fuel_consumption' function.
2. If data values drops below -5kg then the data is cut from that point onwards.
3. Fuel consumption is only considered if the change is weight between subsequent data points is 25g or more.
4. Parameters reported are the average, maximum and minimum fuel consumption per day.

airquality_HAPEx.py
1. Start and end date are specified in the 'airquality' function.
2. If the data contains any erronous values, those values are removed and data is broken into sections. The sections are then stitched together and analysed as a whole.
3. Parameters reported are the average, maximum and minimum hourly air quality.
