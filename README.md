# Log-Anomaly-Detection

IITP-2021-0-00256 클라우드 자원의 지능적 관리를 위한 이종 가상화(VM+Container) 통합 운용 기술 개발 과제를 통해 공개한 오케스트로의 로그 이상탐지 알고리즘

### Directory Explanation
* pre-processing : 데이터 로드 및 저장 & 전처리
> create_parquet.py : csv 파일을 parquet 파일로 처리하여 hdfs에 저장
> 
> preprocessing.py : elasticsearch에서 데이터 로드 후 로그 메시지 전처리

* model : 사용 모델 알고리즘
> save_to_es.py : elasticsearch에 설계된 스키마 형태로 데이터 저장
> 
> update_padding_data : 벡터화된 데이터 패딩을 진행
> 
### Who We Are
회사 홈페이지:
http://okestro.com/

### License
Apache 2.0 License
