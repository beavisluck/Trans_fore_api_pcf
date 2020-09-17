
#there's a possibility of an unused lib here
import numpy as np
import pandas as pd
import random


from sklearn.preprocessing import MinMaxScaler
import os
from flask_restful import Resource,Api
from flask import Flask,request,jsonify
import cx_Oracle
from sqlalchemy import create_engine
from datetime import timedelta
import math
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.metrics import mean_squared_error
from flask_cors import CORS
import requests
import json

random.seed(0)
np.random.seed(0)



app=Flask(__name__)
api=Api(app)
here = os.path.dirname(os.path.abspath(__file__))
cors = CORS(app, resources={r"/*": {"origins": "*"}})
cf_port = os.getenv("PORT")
cx_Oracle.init_oracle_client(lib_dir=r"/home/vcap/app/oracle/instantclient")
dsn_tns = cx_Oracle.makedsn('fill urr conn properties here', 'fill urr conn properties here', service_name='fill urr conn properties here') 


cstr = 'fill urr conn properties here'.format(
            sid=dsn_tns
        )

class forecast(Resource):
    def get(self):
        with cx_Oracle.connect(user='fill urr conn properties here', password='fill urr conn properties here', dsn=dsn_tns) as conn:
            curr=conn.cursor()
			#seasonal optimizer
            def param_opt(df):
                date=df.index[-1]- timedelta(days=pred_periods)
                train = df.loc[:date]
                test = df.loc[date+timedelta(days=1):]    
                max_param=60
                params=[]
                rmse=[]
                i=2
                while i<=max_param:
                
                    model = ExponentialSmoothing(train, seasonal='mul', seasonal_periods=i).fit()
                    pred = model.predict(start=test.index[0], end=test.index[-1])
                    
                    test["pred"]=pred
                    
                    testScore = math.sqrt(mean_squared_error(test["TRX_AMOUNT"], test["pred"]))
                
                    params.append(i)
                    rmse.append(testScore)
                    i=i+1
                
                optimal=pd.DataFrame(columns=["Param","RMSE"])
                optimal["Param"]=params
                optimal["RMSE"]=rmse
                optimal=optimal.sort_values(by=['RMSE'])
                s_per=optimal["Param"].head(1).values
                rmse=optimal["RMSE"].head(1).values
                return s_per,rmse
            #listing table and trx_type
            table=["forecast_main","forecast_branch"]
            trx_type=["DB","CR"]
            #start the loop
            #do for each_db table
            for tab in table:
                print()
                if tab=="forecast_branch":
                    q_brid="branch_account_id"
                else:
                    q_brid="main_account_id"
                curr=conn.cursor()
                sql="select count(*) from "+tab
                curr.execute(sql)
                rownum = curr.fetchone()
                rownum= int(list(rownum)[0])
                #if data exist
                if rownum>0:
                    #do per trx_type income and outcome
                    for flg in trx_type:
                        sql="select count(distinct("+q_brid+")) from "+tab
                        curr.execute(sql)
                        branch= curr.fetchone()
                        branch= int(list(branch)[0])
                        #each branch
                        for br_id in range(1,branch+1):
                            sql="select * from "+tab+" where "+q_brid+"="+str(br_id)+" and trx_type='"+flg+"'"
                            df=pd.read_sql(sql,con=conn)
                            df=df.drop(df.columns[0], axis=1)
                            df=df.drop(df.columns[2], axis=1)
                            df=df.drop(df.columns[2], axis=1)
                            df=df.drop(df.columns[2], axis=1)
                            df['TRX_DATE']=pd.to_datetime(df['TRX_DATE'])
                            df = df.set_index(pd.DatetimeIndex(df['TRX_DATE']))
                            df=df.drop(df.columns[1], axis=1)
                            df.sort_index(inplace=True)
                            
                            pred_periods=24
                            
                            s_per=param_opt(df)
                            model = ExponentialSmoothing(df, seasonal='mul', seasonal_periods=s_per[0]).fit()
                            pred = model.forecast(pred_periods)
                            df_forecast= pd.DataFrame(pred,columns=["forecast_amount"])
                            
                            df_forecast["branch_id"]=br_id
                            df_forecast["recommended"]=df_forecast["forecast_amount"]+s_per[1]
                                
                            df_forecast=df_forecast.reset_index()
                            df_forecast=df_forecast.rename(columns={"index":"date"})
                            
                            df_forecast=df_forecast[["branch_id","date","forecast_amount","recommended"]]
                            df_forecast["trx_type"]=flg
                            df_forecast=df_forecast.rename(columns={"branch_id":q_brid,"date":"trx_date"})
                            print(sql)
                            #for insert
                           
                            c_alchemy = create_engine(cstr)
                            df_forecast.to_sql(tab,c_alchemy, if_exists='append',index=False)
                
                            
                else:
                    print("no data")
            # use triple quotes if you want to spread your query across multiple lines
            
            return ("done")


            

api.add_resource(forecast,'/fore')


if __name__ == '__main__':
	if cf_port is None:
		app.run( host='0.0.0.0', port=5000, debug=True, threaded=True )
	else:
		app.run( host='0.0.0.0', port=int(cf_port), debug=True, threaded=True)