from pathlib import PurePath
import numpy as np
import pandas as pd

from load_data import *
import datetime
from statsmodels.tsa.statespace import sarimax



def run_sarimax_predictions(df,arima_order, sarima_order,col_name='consumption',  skip_first_n_weeks=4, optim_method="bfgs", history_size_weeks=4, intermediate_prefix=1):
        """
        Params:
            df: pd.DataFrame
                The DataFrame that contains the data that should be predicted
            arima_order: 
                3-Tuple that contains the (AR,I,MA) Order of the model
            SARIMA_order:
                4-Tuple that contains the Season + AR I MA Order
            col_name:
                The column of the dataframe that should be predicted

        """
    
        df.index.freq = "H"
        hours_in_week =  7*24
        num_skipped_entries =  hours_in_week * skip_first_n_weeks
        
        print(f"Using {optim_method} Optimizer")
              
        start_time = datetime.datetime.now()
        history =  df[col_name].loc[df.index<=df.index[num_skipped_entries]]
        print(f"{start_time}: Fitting first model.. ")
        model = sarimax.SARIMAX(history, order=arima_order, seasonal_order=sarima_order)
        fit = model.fit(method=optim_method)
        print(f"Needed {datetime.datetime.now() - start_time}")
        
        hours_to_predict = 24
        
        predictions = pd.DataFrame(index=df.index[num_skipped_entries:], columns=list(range(1, hours_to_predict+1)))

        for ts in df.index[num_skipped_entries:]:
            if ts.day_of_week == 0 and ts.hour == 0:
                
                history =  df[col_name].loc[(df.index<= ts) & (df.index >= (ts - pd.Timedelta(weeks = history_size_weeks)))]
                print("Shape History:", history.shape)
                start_time = datetime.datetime.now()
                print(f"{start_time}: Fitting model for {ts}..")
                model = sarimax.SARIMAX(history, order=arima_order, seasonal_order=sarima_order)
                fit = model.fit(method=optim_method)
                print(f"Needed {datetime.datetime.now() - start_time}")
                predictions.to_csv(PurePath('.', 'intermediate', f'intermediate_{intermediate_prefix}.csv'))
            else:
                start_time = datetime.datetime.now()
                fit = fit.append([df[col_name][ts]])
        
            predict =   fit.predict(start=(ts + pd.Timedelta(hours=1)) , end=(ts + pd.Timedelta(hours=hours_to_predict)))
           
            for h, p in enumerate(predict):
                predictions[h+1][ts] = p
    
            
        
        return predictions

arima_orders = [(1,0,1),  (1,0,1)] # [(3,0,0), (1,0,1),(0,1,2)]
sarima_orders =[(1,0,1,24), (2,0,0,24)] # [(2,0,0,24), (2,0,0,24),(0,1,1,24)]

history_size=4

df = pd.read_csv(PurePath('.','htw_berlin_gt', 'htw_berlin.csv'), parse_dates=True, index_col=["utc_timestamp"])

for arima_order, sarima_order in zip(arima_orders, sarima_orders):
    for prediction_target in ['balance', ]:
        for  prosumer_id in range(30,74):

            column_to_predict = 'prosumer%d_%s' % (prosumer_id, prediction_target)
       
            print(f"Beginning with prosumer {prosumer_id}")

            pred_bfsg = run_sarimax_predictions(df, arima_order, sarima_order, optim_method='bfgs', intermediate_prefix=prosumer_id, history_size_weeks=history_size, col_name=column_to_predict)
            pred_bfsg.to_csv(f"htw_berlin_result/predictions_{prosumer_id}_{prediction_target}_{arima_order}_{sarima_order}_history_{history_size}.csv")
                 
