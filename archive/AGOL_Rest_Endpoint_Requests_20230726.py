import os
import requests
import json 
import sys
import arcpy

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

def all_requests(OID_count, OID_list, jpath, workspace, url):
    
    # loop through Object ID count, returns a JSON of each 100 records or less
    # Creates an output feature class. Imports JSONs to temp feature classes
    # Appends temp feature classes to output feature class
    # deletes all JSONs and temp feature classes 
    
    print (f"Running function 'all_requests'...")
    total_count = OID_count
    all_OIDs =  OID_list

    
    # Number of requests to make to API. 
    # Uses 1000 returns per request
    
    number_requests = int(total_count/100)
    print (f"Total Number of requests: {str(number_requests)}")
    
    # Gets the remainder 
    remainder = total_count % 100
    print (f"Request remainder: {str(remainder)}")


    # list to hold all JSON paths
    all_json_features = []

    responses = []
    
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

    for i in range(number_requests):
        print (f"Request number: {str(i)}")
        
        # print (f"Total number of OIDs added to requests: {str(c)}")
        
        # Counter of 100 record requests
        c1 = 1
        
        # String to hold OIDS, seperated by comma. No comma at end. Reset every 100 records.
        oid_string = ""

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
            
        print (f"\nInterval OID count (should be 100): {str(c1)}")
     
        url = url + '/query?'
      
        params = {
        'where': '1=1',
        'objectIds': oid_string,
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'json'
        }
        
        response = requests.get(url, params=params)

        # check for 200 response, then proceed to JSON dump
        #response = requests.get(URL_query)
            
        if response.status_code == 200:
            print (response)
            data = response.json()

            responses.append(data)


            # local_path = "C:\\Users\\warmstrong\\Documents\\Jupyter Notebooks"
            f_name = f"{str(c)}_2023_07_17_Requests.json"
            jpath = os.path.join(workspace, f_name)

            if arcpy.Exists(jpath):
                # If it exists, delete it
                arcpy.Delete_management(jpath)

            # print(f"Path to output JSON: {jpath}")

            with open(jpath, "w") as f:
                json.dump(data, f)


            # Set the output feature class name
            output_feature_class = f"json_{str(c)}_request"

            # Construct the full path of the output feature class within the file geodatabase
            output_feature_class_path = geodatabase_path + "\\" + output_feature_class

            # Convert JSON to feature class
            arcpy.JSONToFeatures_conversion(jpath, output_feature_class_path)

            all_json_features.append(output_feature_class_path)

        else:
            print(f"Failed to get data, status code: {response.status_code}")
            return
        # Exit to run just once
        # sys.exit()

    # Add remaining OIDs
    if int(remainder) > 0:
        # String to hold OIDs from remainder
        oid_string = ""
        r=0 
        for i in range(remainder):
            #print (f"I in range = {str(i)}")
            if i < remainder:

                #print (f"")
                #print (f"OID index = {str(all_OIDs[c])}")
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

  
        url = url
        
        params = {
        'where': '1=1',
        'objectIds': oid_string,
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'json'
        }
        
        response = requests.get(url, params=params)
        
        
        # check for 200 response, then proceed to JSON dump
        # response = requests.get(URL_query)
                    
        if response.status_code == 200:
            print (response)
            data = response.json()

            responses.append(data)

            # local_path = "C:\\Users\\warmstrong\\Documents\\Jupyter Notebooks"
            f_name = f"{str(c)}_2023_05_25_Requests.json"
            jpath = os.path.join(workspace, f_name)

            if arcpy.Exists(jpath):
                # If it exists, delete it
                arcpy.Delete_management(jpath)

            with open(jpath, "w") as f:
                json.dump(data, f)


            # Set the output feature class name
            output_feature_class = f"json_{str(c)}_request"

            # Construct the full path of the output feature class within the file geodatabase
            output_feature_class_path = geodatabase_path + "\\" + output_feature_class

            # Convert JSON to feature class
            arcpy.JSONToFeatures_conversion(jpath, output_feature_class_path)

            all_json_features.append(output_feature_class_path)

        else:
            print(f"Failed to get data, status code: {response.status_code}")
            return

                
        print(f"Path to output JSON: {jpath}")
        print (f"Total OIDs from remainder: {str(remainder)}\nNum of remainder OIDs added: {str(r)}")
        # print (f"Remainder OID String: {oid_string}")
        # print (all_json_features)
        
        # Use the first feature class in the list to be the target others are appended to
        j1 = all_json_features[0]

        c = 0
        for j in all_json_features:
            if c != 0:
                # append feature into first feature class
                arcpy.Append_management(
                inputs=j,
                target=j1,
                schema_type="TEST"
                )
            c+=1

#------------------------------------------------------------------------------------------

# setup JSON
local_path = r"C:\Users\warmstrong\Documents\Data\NFPORS\08102023 download"
f_name = "08102023 NFPORS.json"
json_path = os.path.join(local_path, f_name)

# Check for JSON and delete if already exists
if os.path.exists(json_path):
    os.remove(json_path)
    print(f'Deleted old JSON')


# base URL to feature service
# URL = "https://services3.arcgis.com/T4QMspbfLg3qTGWY/ArcGIS/rest/services/Jurisdictional_Unit_(Public)/FeatureServer/0"
URL = "https://usgs.nfpors.gov/arcgis/rest/services/treatmentPoly/MapServer/0"

# call 'object_ids' function
idList, idCount = object_ids(URL)

print(f"Total count of OIDs: {str(idCount)}\n\n")

# call 'all_requests' function
all_requests(idCount, idList, json_path, local_path, URL)










