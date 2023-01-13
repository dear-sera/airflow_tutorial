import pandas as pd

from MLproject.preprocess import TitanicPreprocess
from MLproject.config import PathConfig
from MLproject.dataio import DataIOSteam
from MLproject.model import TitanicModeling


class TitanicMain(TitanicPreprocess, PathConfig, TitanicModeling, DataIOSteam):
    def __init__(self):
        TitanicPreprocess.__init__(self)
        PathConfig.__init__(self)
        TitanicModeling.__init__(self)
        DataIOSteam.__init__(self)

    #타이타닉 데이터 로드, 전처리 및 저장
    def prepro_data(self, f_name, **kwargs):
        # fname = train.csv
        data = self.get_data(self.titanic_path, f_name)  #데이터 로드
        data = self.run_preprocessing(data) #데이터 전처리
        data.to_csv(f"{self.titanic_path}/prepro_titanic.csv", index=False) #전처리 데이터 저장
        #전처리 거친 파일의 경로를 Airflow XCom에 저장
        kwargs['task_instance'].xcom_push(key='prepro_csv', value=f"{self.titanic_path}/prepro_titanic")
        return "end prepro"

    #모델링
    def run_modeling(self, n_estimator, flag, **kwargs):
        # n_estimator = 100
        #Airflow XCom에서 저장된 전처리 데이터 경로를 가져옴
        f_name = kwargs["task_instance"].xcom_pull(key='prepro_csv')
        data = self.get_data(self.titanic_path, f_name, flag) #전처리 데이터 로드
        X, y = self.get_X_y(data) #데이터 타겟, 피쳐값 추출
        #모델링 및 모델 정보 추출(모델 점수 및 파라미터)
        model_info = self.run_sklearn_modeling(X, y, n_estimator)
        #모델 정보를 Airflow XCom에 저장
        kwargs['task_instance'].xcom_push(key='result_msg', value=model_info)
        return "end modeling"