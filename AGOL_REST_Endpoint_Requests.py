import os
import requests
import json 
import sys
import arcpy
import datetime

#--------------------------------------------------------------------------------------------------------
def object_ids(url):
     
     # function returns a count of Object IDs, and all Object IDs 

    where = "1=1"
    urlstring = url + f"/query?where={where}&returnIdsOnly=true&f=json"
    # print(f"URL String from OID Count request: {urlstring}")
    response = requests.get(urlstring)

    # Check the status of the request
    if response.status_code == 200:

        data = response.json()
        # The object IDs should be in the 'objectIds' field. Create list from the json.
        object_ids = data.get('objectIds', [])
        
        num_objectIDs = len(object_ids)
        print (f"Number of Object IDs: {num_objectIDs}")
        return object_ids, num_objectIDs
    
    else:
        print(f"Failed to get data, status code: {response.status_code}")
#----------------------------------------------------------------------------------------

def all_requests(OID_count, OID_list, jpath, workspace, url, fc_name):
    
    # loop through Object ID count, returns a JSON of each 100 records or less
    # Creates an output feature class. Imports JSONs to temp feature classes
    # Appends temp feature classes to output feature class
    # deletes all JSONs and temp feature classes 
    
    print (f"Running function 'all_requests'...")
    total_count = OID_count
    all_OIDs =  OID_list

    # Check for spaces in feature class name
    if ' ' in fc_name:
    # Replace spaces with underscores
        fc_name = fc_name.replace(' ', '_')

    # Number of requests to make to API. 
    # Uses 1000 returns per request
    
    number_requests = int(total_count/100)
    print (f"Total Number of requests: {str(number_requests)}")
    
    # Gets the remainder 
    remainder = total_count % 100
    print (f"Request remainder: {str(remainder)}")


    # list to hold all JSON paths
    # all_json_features = []

    # responses = []
    
    # create file geodatabase
    workspace = workspace

    # Set the name of the file geodatabase
    geodatabase_name = "data.gdb"

    # full path of the file geodatabase
    geodatabase_path = os.path.join(workspace, geodatabase_name)

    # Check if the file geodatabase already exists
    if arcpy.Exists(geodatabase_path):
        # If it exists, delete it
        arcpy.Delete_management(geodatabase_path)
        print("Existing file geodatabase deleted.")

    # Construct the full path of the file geodatabase
    arcpy.CreateFileGDB_management(workspace, geodatabase_name)

    # Print the path of the created file geodatabase
    print("File geodatabase created at:", geodatabase_path)

    # Counter of total number of OIDs added
    c = 0

    # log file
    log_path = os.path.join(workspace, 'log.txt')
    if os.path.exists(log_path):
        os.remove(log_path)
    log = open(log_path, 'w') 

    # feature class count
    fc_count = 0

    # feature class for appends 
    append_fc = ""

    # delete JSONs
    # List all files in the folder
    file_list = os.listdir(workspace)

    # Filter .json files
    json_files = [file for file in file_list if file.endswith('.json')]

    # Delete each .json file
    for json_file in json_files:
        file_path = os.path.join(workspace, json_file)
        os.remove(file_path)
    

    for i in range(number_requests):
      
        # Counter of 100 record requests
        c1 = 1
        
        # String to hold OIDS, seperated by comma. No comma at end. Reset every 100 records.
        oid_string = ""

        # URL for query - add 'query' to the base URL on first request
        if c==0:
            url = url + '/query?'

        while c1 < 100:
            new_oid = str(all_OIDs[c])
            new_oid_str = f"{new_oid},"
            oid_string += new_oid_str
            # Update counts
            c+=1
            c1+=1
            
        if c1 == 100:
            new_oid = str(all_OIDs[c])
            # remove comma
            new_oid_str = f"{new_oid}"
            oid_string += new_oid_str
            c+=1
            
        # remove trailing comma if it exists
        if oid_string.endswith(","):
            oid_string = oid_string[:-1]

    
        params = {
        'where': '1=1',
        'objectIds': oid_string,
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'json'
        }
        
        response = requests.get(url, params=params)
        responseURL = f"Full URL:, {response.url}"

        # check for 200 response, then proceed to JSON dump
        
            
        if response.status_code == 200:
            
            r = response.status_code
            print(f"\nRequest interval: {str(i)}")
            print(f"Response was successfull, status code: {r}")
            log.write(f"Response was successfull, status code: {r}, Request #{str(i)}, at count {str(c)}:")
            log.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log.write(f"\nResponse URL\n {responseURL}\n")
            log.write(f"\nOIDs: {oid_string}\n")
            log.write(f"-----------------------------------------------------------")


            data = response.json()

            # responses.append(data)

            # Get the current date
            current_date = datetime.datetime.now()

            # Create a formatted string with the current date
            date_string = current_date.strftime('%Y-%m-%d')

            # Combine the formatted date with the file name

            f_name = f"{str(c)}_{date_string}_Requests.json"
            jpath = os.path.join(workspace, f_name)

            print(f"Path to output JSON: {jpath}")

            with open(jpath, "w") as f:
                json.dump(data, f)


            # If first feature class - Use this for all appended feature classes
            if fc_count == 0:
                
                # Set the output feature class name
                output_feature_class = fc_name

                # Construct the full path of the output feature class within the file geodatabase
                output_feature_class_path = geodatabase_path + "\\" + output_feature_class

                print (f"First feature class: {output_feature_class_path}")
                append_fc = output_feature_class_path

            # else, create a temp feature class using JSON name as feature class name
            else:

                # Set the output feature class name
                output_feature_class = f"json_{str(c)}_request"

                # Construct the full path of the output feature class within the file geodatabase
                output_feature_class_path = geodatabase_path + "\\" + output_feature_class

            # Convert JSON to feature class
            arcpy.JSONToFeatures_conversion(jpath, output_feature_class_path)

           
                
                
            # If not first feature class - append to first feature class
            if fc_count > 0:

                # append feature into first feature class
                arcpy.Append_management(
                inputs=output_feature_class_path,
                target=append_fc,
                schema_type="TEST"
                )
                    
                # delete feature class
                arcpy.Delete_management(output_feature_class_path)

            # delete JSON
            os.remove(jpath)
            # all_json_features.append(output_feature_class_path)

            fc_count += 1

        if response.status_code != 200:
            r = response.status_code
            print(f"\nRequest interval: {str(i)}")
            print(f"Failed to get data, status code: {r}")
            log.write(f"Failed to get data, status code: {r}, Request #{str(i)}, at count {str(c)}:")
            log.write(f"{r}+\n")
            log.write(f"\nResponse URL\n {responseURL}\n")
            log.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log.write(f"\nOIDs: {oid_string}\n")
            log.write(f"-----------------------------------------------------------")
            
       
    # Add remaining OIDs
    if int(remainder) > 0:
        # String to hold OIDs from remainder
        oid_string = ""
        r=0 
        for i in range(remainder):
            #print (f"I in range = {str(i)}")
            if i < remainder:

    
                new_oid = str(all_OIDs[c])
                new_oid_str = f"{new_oid},"
                oid_string += new_oid_str

                # update remainder count
                r+=1
                # update total count
                c+=1

            if i == remainder:
                new_oid = str(all_OIDs[c])
                # remove comma 
                new_oid_str = f"{new_oid}"
                oid_string += new_oid_str

                # update remainder count
                r+=1
                # update total count
                c+=1

  
        url = url + '/query?'
        
        params = {
        'where': '1=1',
        'objectIds': oid_string,
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'json'
        }
        
        response = requests.get(url, params=params)
        responseURL = f"Full URL:, {response.url}"

        print (f"\n\nTotal remaining OIDs: {str(remainder)}")
        
        
        # check for 200 response, then proceed to JSON dump
        # response = requests.get(URL_query)
                    
        if response.status_code == 200:
            
            r = response.status_code
           
            print(f"Response was successfull, status code: {r}")
            log.write(f"Response was successfull, status code: {r}, Request #{str(i)}, at count {str(c)}:")
            log.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log.write(f"\nResponse URL\n {responseURL}\n")
            log.write(f"\nOIDs: {oid_string}\n")
            log.write(f"-----------------------------------------------------------")
            
            data = response.json()

           
            f_name = f"{str(c)}_2023_05_25_Requests.json"

            jpath = os.path.join(workspace, f_name)

            # Get the current date
            current_date = datetime.datetime.now()

            # Create a formatted string with the current date
            date_string = current_date.strftime('%Y_%m_%d')
            f_name = f"{str(c)}_{date_string}_Requests.json"
            jpath = os.path.join(workspace, f_name)

            if arcpy.Exists(jpath):
                # If it exists, delete it
                arcpy.Delete_management(jpath)

            with open(jpath, "w") as f:
                json.dump(data, f)


            if number_requests == 0:
                
                output_feature_class = fc_name
            
            else:
                output_feature_class = f"json_{str(c)}_request"


            # Construct the full path of the output feature class within the file geodatabase
            output_feature_class_path = geodatabase_path + "\\" + output_feature_class

            print (jpath)
            print (output_feature_class_path)

            # Convert JSON to feature class
            arcpy.JSONToFeatures_conversion(jpath, output_feature_class_path)

            # if number of features less than 100, no append is needed. So if number of requests is 0, skip append. 
            if number_requests > 0:
                
                # append feature into first feature class
                arcpy.Append_management(
                inputs=output_feature_class_path,
                target=append_fc,
                schema_type="TEST"
                )
                        
                # delete feature class
                arcpy.Delete_management(output_feature_class_path)

            # delete JSON
            os.remove(jpath)

        else:
            r = response.status_code
            log.write(f"\nFailed to get data, at count {str(c)}:")
            log.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            log.write(f"\nResponse URL\n {responseURL}\n")
            log.write(f"\nOIDs: {oid_string}\n")
            log.write(f"-----------------------------------------------------------")
            

    log.close()

#------------------------------------------------------------------------------------------

# setup JSON
local_path = r"C:\Users\warmstrong\Documents\Data\InFORM\09062023 read only service download"
f_name = "Estimated_Treatment.json"
json_path = os.path.join(local_path, f_name)

# Check for JSON and delete if already exists
if os.path.exists(json_path):
    os.remove(json_path)

URL = "https://inform-dev-arcgis.mbsdevazure.com/server/rest/services/OpenData/InFORM_Fuels_Treatments_and_Activities/FeatureServer/3"

# Output name of feature class
out_feature = "actual activity"

# call 'object_ids' function
idList, idCount = object_ids(URL)

print(f"Total count of OIDs: {str(idCount)}\n\n")

# call 'all_requests' function
all_requests(idCount, idList, json_path, local_path, URL, out_feature)










