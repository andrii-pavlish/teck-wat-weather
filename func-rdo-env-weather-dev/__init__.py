import time
import requests
import os
import datetime
import pygrib
import pandas as pd
import numpy as np
import tempfile
from datetime import timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import azure.functions as func


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    # Determine execution environment
    DEVELOPMENT = False
    #if "rdo-env-production" in dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get():
    #  DEVELOPMENT = False
    
    historical_period = 5 # days
    forecasts_per_day = 4 # 0, 6, 12, 18
    
    def d_print(phrase):
      print("{} {}".format(datetime.datetime.now(), phrase))
    
    
    ## North American Mesoscale forecast for Alaska
    nam_AK_forecast_url_gf = "https://nomads.ncep.noaa.gov/cgi-bin/filter_nam_alaskanest.pl"
    nam_AK_forecast_dir_format = "nam.{}{:02d}{:02d}" 
        # {1} is the year in YYYY format
        # {2} is the month in MM format
        # {3} is the day in DD format
    nam_AK_forecast_file_format = "nam.t{:02d}z.alaskanest.hiresf{:02d}.tm00.grib2"
        # {1} is time the forecast is made in hours 00, 06, 12, 18
        # {2} is the forecast hour (in the future) 00, 01, 02...60
    nam_AK_forecast_folder = "nam_alaska_forecasts"
    nam_AK_forecast_var_list = ['PRES',     # Pressure (Pa)
                                'TMP',      # Temperature (K)
                                'SPFH',     # Specific humidity (kg/kg)
                                'SSRUN',    # Storm surface runoff (kg/m^2)
                                'BGRUN',    # Baseflow-groundwater runoff (kg/m^2)
                                'SNOD',     # Snow depth (m)
                                'EVP',      # Evaporation (kg/m^2)
                                'PRATE',    # Precipitation rate (kg/m^2/s)
                                'DSWRF',    # Downward short-wave radiation (W/m^2)
                                'DLWRF',    # Downward long-wave radiation (W/m^2)
                                'SOILM',    # Soil moisture content (kg/m^2)
                                'TSOIL',    # Soil temperature (K)
                                'APCP',     # Total precipitation (kg/m^2)
                                'UGRD',     # U-component of wind (m/s)
                                'VGRD',     # V-component of wind (m/s)
                                'CNWAT',    # Plant canopy surface water (kg/m^2)
                                'NCPCP',    # Large-scale precipitation (non-convective) (kg/m^2)
                                'LHTFL']    # Latent heat net flux (W/m^2)
    
    ## Inputs
    red_dog_lat = 68.0756
    red_dog_lon = -162.8561
    
    red_dog_NAM_lat = 68.07837    # closest points in the NAM dataset
    red_dog_NAM_lon = -162.85785 
    
    start_hour = 0
    end_hour = 45
    
    # ADLS path to save files
    save_directory_folder = "Raw_Data/Weather_NAM/"
    model_output_file_name_fmt = "Model_Data/Model_Output_TDS/{}"
    if DEVELOPMENT:
      save_directory_folder = save_directory_folder.replace("Raw_Data", "Raw_Data_Dev")
      model_output_file_name_fmt = model_output_file_name_fmt.replace("Model_Data", "Model_Data_Dev")
    save_directory_fmt = save_directory_folder + "{}{:02d}{:02d}/{:02d}/" # YYYYMMDD/HH
    
    # Query parameters for file retrieval
    query_params = {
                'file': None,                        # file name
                'subregion': 'on',                   # retrieve data within bounds
                'leftlon': red_dog_lon - 0.29,       # left longitude bounds
                'rightlon': red_dog_lon + 0.21,      # right longitude bounds
                'toplat': red_dog_lat + 0.21,        # top latitude bounds
                'bottomlat': red_dog_lat - 0.30,     #  bottom latitude bounds
                'dir': None,                         # forecast directory
                'lev_surface': 'on',                 # surface level
                'lev_10_m_above_ground': 'on',       # 10 m above ground
                'lev_0-2_m_below_ground': 'on',      # 0-2 m below ground
                'lev_0-0.1_m_below_ground': 'on',    # 0-0.1 m below ground
                'lev_0.1-0.4_m_below_ground': 'on',  # 0.1-0.4 m below ground
                'lev_0-1_m_below_ground': 'on',      # 0-1 m below ground
                'lev_0.4-1_m_below_ground': 'on'     # 0.4-1 m below ground
            }
    
    # Current time in UTC
    current_time = datetime.datetime.utcnow()
    d_print(">>> Downloading NAM Alaska forecasts ({})".format(current_time))
    
    
    # Populate forecast directory
    forecast_dir = nam_AK_forecast_dir_format.format(current_time.year, current_time.month, current_time.day)
    d_print("... using directory: "+forecast_dir)
    
    # Populate query parameters (sans file name)
    query_params["dir"] = "/"+forecast_dir
    query_params = {**query_params, 
                    **{"var_"+v: "on" for v in nam_AK_forecast_var_list}}
    
    # Define download parameters and create directory if not existing
    forecast_prod_hour = current_time.hour - (current_time.hour % 6) # 0, 6, 12, 18
    d_print("... using forecast production hour: {}".format(forecast_prod_hour))
    save_directory = save_directory_fmt.format(current_time.year, current_time.month, 
                                current_time.day, forecast_prod_hour)
    
    
    temp_dir = tempfile.TemporaryDirectory()
    print(temp_dir.name)
    
    
    # Loop through forecast files and download
    start_download_time = time.time()
    for forecast_hour in range(start_hour, end_hour + 1):
        query_params["file"] = nam_AK_forecast_file_format.format(forecast_prod_hour, forecast_hour)
        d_print("... ... retrieving data from file: "+query_params["file"])
        forecast_response_temp_file_name = temp_dir.name + "/" + query_params["file"]
        print("forecast_response_temp_file_name", forecast_response_temp_file_name)
        blob_path_file_name = save_directory + query_params["file"]
    
        # Create the BlobServiceClient object which will be used to create a container client
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=strdoenvpdhdev;AccountKey=WPW5NHX2VJtSt/uAcB8f5l2zt5bjaYnTuv7YV6Mcmdl7X139bt8AAWYV28YoxY4sCQyPevsFBWdWkFg1aWfXWw==;EndpointSuffix=core.windows.net")
    
        # Container name
        container_name = "rdo-environmental"
    
        # Create the container client
        container_client = blob_service_client.get_container_client(container_name)
    
        #### UPLOAD TO AZURE ####
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path_file_name)
        
        retries = 0
    
        while True:
            try:
                response = requests.get(nam_AK_forecast_url_gf, params=query_params, timeout=120)
                response.raise_for_status() # Raise exception for bad status code
                open(forecast_response_temp_file_name, "wb").write(response.content)
                
                print("\nUploading to Azure Storage as blob:\n\t" + blob_path_file_name)
    
                # Upload the created file to Azure Blob
                with open(forecast_response_temp_file_name, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                
                break
            except:
                retries += 1
                time.sleep(retries * 5)
                if retries == 2:
                    raise Exception("Max number of retries exceeded")
                
    d_print("<<< Download complete in {:.2f} seconds".format(time.time() - start_download_time))
    
    
    d_print(">>> Starting post processing from GRIB2 to Parquet")
    
    data = {} 
    for forecast_hour in range(start_hour, end_hour + 1):
        file_path = temp_dir.name + "/" + nam_AK_forecast_file_format.format(forecast_prod_hour, forecast_hour)
        print("file_path", file_path)
        grb = pygrib.open(file_path)
        for measure in grb:
              # Split label string into constituents
              # Ex:
              # 1:Surface pressure:Pa (instant):polar_stereographic:surface:level 0:fcst time 30 hrs:from 202004061800
              description = str(measure).split(":")[1].replace(" ", "_")
              measurement_type = str(measure).split(":")[2].split("(")[1].strip(")")
              level = "_".join(str(measure).split(":")[5].split()[1:])
              forecast_date = str(measure).split(":")[7].split()[1]
    
              # Extract arrays for values and coordinates
              values, lats, lons = measure.data()
    
              # Build column name and forecast date
              column = "{}_{}_{}".format(description, level, measurement_type)
              forecast_time = datetime.datetime.strptime(forecast_date, "%Y%m%d%H%M")
              forecast_time += timedelta(hours=forecast_hour)
    
              # Iterate through values
              for idx, value in np.ndenumerate(values):
                  coords = (lats[idx], lons[idx])
                  if coords not in data:
                      data[coords] = {}
                  if forecast_time not in data[coords]:
                      data[coords][forecast_time] = {}
                  data[coords][forecast_time][column] = value
    
    # Construct dictionary of dataframes and concatenate
    data_df = {k: pd.DataFrame(data[k]).T for k,v in data.items()}
    
    comb_output_file_name_fmt = "nam.t{:02d}z.alaskanest.hirescombined.tm00.parquet"
    comb_output_file_name = temp_dir.name + comb_output_file_name_fmt.format(forecast_prod_hour)
    data_df_concat = pd.concat(data_df, axis=0)
    
    # temp location for parquet
    local_path_blob_concat_parquet = temp_dir.name + '/data_df_concat.parquet'
    
    # Save the historical file
    df_historical = pd.DataFrame()
    df_historical.to_parquet(local_path_blob_concat_parquet)
    
    #### UPLOAD TO AZURE ####
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=comb_output_file_name)
    
    # Upload the created file to Azure Blob
    with open(local_path_blob_concat_parquet, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    d_print("<<< Completed post processing")
    
    
    d_print(">>> Starting post processing from GRIB2 to Parquet")
    
    data = {} 
    for forecast_hour in range(start_hour, end_hour + 1):
        file_path = temp_dir.name + "/" + nam_AK_forecast_file_format.format(forecast_prod_hour, forecast_hour)
        grb = pygrib.open(file_path)
        print(grb)
    
    blob_list = container_client.list_blobs(name_starts_with="Raw_Data_Dev/Weather_NAM/")
    
    
    date_list = []
    
    for blob in blob_list:
        blob_folder_date = blob.name.split("/")[2]
        if blob_folder_date not in date_list:
            date_list.append(blob_folder_date)
    
    
    last_5_day_date_list = date_list[-1 * (historical_period + 1):]
    
    
    forecast_files = []
    
    # looping through dates in last_5_day_date_list
    for weather_date in last_5_day_date_list:
        hour_list = []
        blob_list = container_client.list_blobs(name_starts_with="Raw_Data_Dev/Weather_NAM/" + weather_date + '/')
        
        # making a hour_list from hours in last_5_day_date_list
        for blob in blob_list:
            blob_folder_hour = blob.name.split("/")[3]
            if blob_folder_hour not in hour_list:
                hour_list.append(blob_folder_hour)
    
        # making a list of forecast Parquet files         
        for hour in hour_list:
            blob_list_for_parquet = container_client.list_blobs(name_starts_with="Raw_Data_Dev/Weather_NAM/" + weather_date + '/' + hour)
            for blob in blob_list_for_parquet:
                if '.parquet' in blob.name:
                    forecast_files.append(blob.name)
            
            
    # Subset forecast files to last 5 days
    forecast_files = forecast_files[-1 * (historical_period * forecasts_per_day + 1):]    
    
    local_path_blob_parquet = temp_dir.name + '/forecast.parquet'
    
    # Merge first 6h of every forecast into shared dataframe, excluding the most recent file, which is merged in entirety
    prev_accumulated = pd.DataFrame()
    
    for forecast_file_path in forecast_files:
        
        blob = BlobClient(account_url="https://strdoenvpdhdev.blob.core.windows.net",
                      container_name=container_name,
                      blob_name= forecast_file_path,
                      credential="WPW5NHX2VJtSt/uAcB8f5l2zt5bjaYnTuv7YV6Mcmdl7X139bt8AAWYV28YoxY4sCQyPevsFBWdWkFg1aWfXWw==")
    
        with open(local_path_blob_parquet, "wb") as f:
            data = blob.download_blob()
            data.readinto(f)
        
        df = pd.read_parquet(local_path_blob_parquet)
        acc_col = [x for x in df.columns if x.endswith('_accum')]
        inst_col = [x.replace('_accum', '_instant') for x in acc_col]
    
        # transform accumulated columns to instant by storing the instant elements in a cloned dataframe
        df_accumulated = df[acc_col].reset_index()
    
        # mask for values to change (hours 0,2,3,5,6,8,9,11,12,14,15,17,18,20,21,23 without the first on in the file)
        change_mask = (((df_accumulated['level_2'].dt.hour-1) % 3) > 0) & (df_accumulated['level_2'] > df_accumulated['level_2'].min())
    
        # mask for values to subtract from values to change (hours 1,2,4,5,7,8,10,11,13,14,16,17,19,20,22,23)
        subtract_mask = ((df_accumulated['level_2'].dt.hour-1) % 3) < 2
    
        # subtract previous column values for masked flows (change_mask and subtract_mask)
        df_accumulated.loc[change_mask, acc_col] = df_accumulated.loc[change_mask, acc_col].values - df_accumulated.loc[subtract_mask, acc_col].values
        df_accumulated.set_index(['level_0', 'level_1', 'level_2'], inplace=True)
    
        # rename column names from *_accum to *_instant
        df_accumulated.rename(columns={x:y for x,y in zip(acc_col, inst_col)}, inplace=True)
    
        # write the cloned instant columns to the original dataframe
        for col in inst_col:
            df[col] = df_accumulated[col]
    
        # update the instant values according to the 6h forecast of the previous forecast and store 6h forecast for next iteration
        df.update(prev_accumulated)  
        prev_accumulated = df[inst_col][df.index.get_level_values(2) == df.index.get_level_values(2).min()+timedelta(hours=24/forecasts_per_day)]
    
        # truncate forecast at 6h for all except for the last forecast file
        if forecast_file_path != forecast_files[-1]:
            df = df[df.index.get_level_values(2) < df.index.get_level_values(2).min()+timedelta(hours=24/forecasts_per_day)]
    
        df_historical = df_historical.append(df)
    
    # sort the indices
    df_historical.sort_index(inplace=True)
    
    # temp location for parquet
    local_path_blob_historical_parquet = temp_dir.name + '/historical.parquet'
    
    # Save the historical file
    df_historical.to_parquet(local_path_blob_historical_parquet)
    
    #### UPLOAD TO AZURE ####
    # Create a blob client using the local file name as the name for the blob
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=model_output_file_name_fmt.format("nam.hirescombined.5days.parquet"))
    
    # Upload the created file to Azure Blob
    with open(local_path_blob_historical_parquet, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    
    d_print("<<< Merging complete")
    
    
    
    d_print(">>> Starting image processing from Parquet to PNG")
    
    # Reset index and select coordinates closest to RDO
    plot_df = data_df_concat.reset_index().round({"level_0": 5, "level_1": 5})
    # plot_df = plot_df[(plot_df.level_0 == red_dog_NAM_lat) & (plot_df.level_1 == red_dog_NAM_lon)]
    # plot_df.set_index("level_2", inplace=True) # Datetime is the index
    plot_df = plot_df.groupby("level_2").mean() 
    
    # Engineer features
    #    Temperature (K) -> (C) and (F)
    #    Snow depth (m)
    #    Precipitation rate (kg/m^2/s) -> (mm)
    #    Storm surface runoff (kg/m^2) -> instant
    plot_df["Temperature_0_instant_C"] = plot_df["Temperature_0_instant"] - 273.15
    plot_df["Temperature_0_instant_F"] = (plot_df["Temperature_0_instant"] - 273.15) * ( 9.0 / 5.0 ) + 32.0
    plot_df["Precipitation_rate_0_instant_mm"] = plot_df["Precipitation_rate_0_instant"] * 60.0 * 60.0 # 60 min/hr * 60 sec/min
    # SSRUN
    storm_surface_runoff_0_instant = []
    last_val = None
    for idx, row in plot_df.iterrows():
        ssrun = row["Storm_surface_runoff_0_accum"]
        if len(storm_surface_runoff_0_instant) == 0 or idx.hour % 3 == 1:
            storm_surface_runoff_0_instant.append(ssrun)
        else:
            storm_surface_runoff_0_instant.append(ssrun - last_val)
        last_val = ssrun
    plot_df["Storm_surface_runoff_0_instant"] = storm_surface_runoff_0_instant
    
    # Adjust timezone to AKST
    plot_df.index = plot_df.index.tz_localize(tz='UTC')
    plot_df.index = plot_df.index.tz_convert(tz="US/Alaska")
    plot_df.index = plot_df.index.tz_localize(None)
    
    # Create plots
    for column in ["Temperature_0_instant_C", "Snow_depth_0_instant", "Precipitation_rate_0_instant_mm", "Storm_surface_runoff_0_instant"]:
        plt.rc('font', size=14)
        plt.rc('xtick', color="#999999")
        plt.rc('ytick', color="#999999")
        plt.rc('axes', edgecolor='#999999')
        
        plt.figure(figsize=(5,2.5))
        plt.xticks(rotation=65)    
    
        ax = plt.gca()
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d %HH'))
        plt.plot_date(plot_df.index, plot_df[column], lw=2.0, ls="-", c="#336699")
    
        # temp location for image
        local_path_blob_image = temp_dir.name + '/image.png'
    
        # Save the image locally in the temp location specified above
        plt.savefig(local_path_blob_image, dpi=96, bbox_inches="tight", transparent=True)
    
        # Raw_Data location on Azure
        output_file_name = save_directory + "nam.t{:02d}z.".format(forecast_prod_hour)+column+".tm00.png"
    
        # Model_Output location on Azure
        model_output_file_name = model_output_file_name_fmt.format(column+".png")
    
        #### UPLOAD TO AZURE ####
        # Loop through the Blob locations above and store the image from temp location to Azure Blob respectively.
        # Create a blob client using the local file name as the name for the blob
        for blob_location in [output_file_name, model_output_file_name]:
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_location)
    
            # Upload the image from temp location to Azure Blob
            with open(local_path_blob_image, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
    
        plt.close()
    
    d_print("<<< Completed image processing")
