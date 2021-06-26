# QUICK START

## PROJECT STRUCTURE

```bash

├── app
│   ├── api
│   │   │── view.py
│   │   │
│   │── main.py                    
│   │   │   
│   │   │ 
├── data_pipeline
│   │   │── calculate_aggregates.py
│   │   │── pipeline.py
│   │   │── process_raw_data.py
│   │   │── schedule.py
│   │   │
├── trading_system
│   │   │── raw_data.py
│   │   │
├── utils
│   │   │── config_parser.py
│   │   │── database_connection.py
│   │   │
└── README.md
└── Makefile
└── requirements.txt
└── config.ini
└── .env
```

## Database 

Please ensure you have database with the name `trading_data`

## Installation & Setup

Run the following commands to clone the project and then create virtual enviroment

```bash
git clone git@github.com:AwaishK/practice_test.git
cd practice_test
make dev-env
```

Run the following command to add environemtns variable to your virtual env

```bash
printf "\nexport \$(grep -v '^#' .env | xargs)" >> env/bin/activate
```

Please use below given template to create a config file.

config_dot_ini_template is dummy file, please ensure you same configuration but remember to change username and password

```bash
    config_dot_ini_template -> config.ini
```

Please use below given template to create a .env file

dot_env is dummy file, please use same env variables but remember to update the values

```bash
    dot_env -> .env
```

Please run below command to schedule the data pipeline

```bash
    python data_pipeline/schedule
```

Please run below command to run restful api

```bash
    python app/main.py
```

