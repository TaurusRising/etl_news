#Импортирование библиотек

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from datetime import datetime
import psycopg2
import os


#Установка и получение Variables

Variable.set ('lenta', 'https://lenta.ru/rss/')
Variable.set ('tass', 'https://tass.ru/rss/v2.xml')
Variable.set ('vedomosti', 'https://www.vedomosti.ru/rss/news')

data = {'lenta': Variable.get('https://lenta.ru/rss/'),
        'tass': Variable.get('https://tass.ru/rss/v2.xml'),
        'vedomosti': Variable.get('https://www.vedomosti.ru/rss/news')
}

#Установка Connections

try:
    conn_parameters = BaseHook.get_connection('connection_db')
except:
    conn = Connection(
        conn_id='connection_db',
        conn_type='postgres',
        host='db',
        login='postgres',
        password='password',
        schema='postgres',
        port=5432
    )
    session = settings.Session ()
    session.add(conn)
    session.commit()
    
    conn_parameters = BaseHook.get_connection('connection_db')

try:
    conn_parameters_fs = BaseHook.get_connection('filepath')
except:
    conn = Connection(
        conn_id='filepath',
        conn_type='fs',
        extra='{"path":"/opt/airflow"}'
    )
    session = settings.Session()
    session.add(conn)
    session.commit()

    conn_parameters_fs = BaseHook.get_connection('filepath')


#Параметры по умолчанию
default_args = {
    "owner":"airflow",
    "startdate": days_ago(0) 
}


#Обработка данных
def add_to_processed_data(cursor):
        cursor.execute(
        """
            INSERT INTO core_data(news_id, source_id, category_id, pub_date, link, title)
            SELECT rd.guid AS news_id, s.id AS source_id, cr.category_id, rd.pub_date, rd.link, rd.title
            FROM raw_data AS rd
            INNER JOIN source_categories AS sc ON sc.name = rd.category
            INNER JOIN sources AS s ON rd.source_name = s.name
            INNER JOIN categories_relationship AS cr ON cr.source_category_id = sc.id
            WHERE rd.guid NOT IN (SELECT DISTINCT news_id FROM core_data)
            GROUP BY rd.guid, s.id, cr.category_id, rd.pub_date, rd.link, rd.title
            ORDER BY by s.id, rd.pub_date DESC
        """)

#Обновление данных
cursor.execute(
        """
            INSERT INTO mart_data
            (
            category_id, 
            category_name, 
            source_name, 
            number_of_news_by_category, 
            number_of_news_by_category_and_source,
            number_of_news_by_category_last_day,
            number_of_news_by_category_and_source_last_day, 
            avg_number_of_news_by_category_per_day, 
            day_with_max_number_of_news_by_category,
            news_count_monday, 
            news_count_tuesday, 
            news_count_wednesday, 
            news_count_thursday, 
            news_count_friday, 
            news_count_saturday, 
            news_count_sunday
            )
            SELECT 
                cd.category_id, 
                c.ctg_name AS category_name, 
                s.src_name AS source_name,
                t1.number_of_news_by_category,
                COUNT (DISTINCT cd.news_id) AS number_of_news_by_category_and_source,
                t2.number_of_news_by_category_last_day,
                t3.number_of_news_by_category_and_source_last_day,
                t4.avg_number_of_news_by_category_per_day,
                t4.day_with_max_number_of_news_by_category,
                t5.news_count_monday,
                t5.news_count_tuesday,
                t5.news_count_wednesday,
                t5.news_count_thursday,
                t5.news_count_friday,
                t5.news_count_saturday,
                t5.news_count_subday
            FROM core_data AS cd
            INNER JOIN categories AS c ON cd.category_id = c.id
            INNER JOIN sources AS s ON cd.source_id = s.id
            LEFT JOIN
                (SELECT category_id, count(distinct news_id) AS number_of_news_by_category
                FROM core_data AS cd
                GROUP BY category_id
                ) t1 ON cd.category_id = t1.category_id
                LEFT JOIN
                    (SELECT category_id, COUNT(DISTICT news_id) AS number_of_news_by_category_last_day
                    FROM core_data
                    WHERE pub_date between (NOW() - interval '1 DAY') AND NOW()
                    GROUP BY category_id
                    ) t2 ON cd.category_id = t2.category_id
                LEFT JOIN
                    (SELECT category_id, source_id, COUNT(DISTICT news_id) AS number_of_news_by_category_and_source_last_day
                    FROM core_data
                    WHERE pub_date between (NOW() - interval '1 DAY') AND NOW()
                    GROUP BY category_id, source_id
                    ) t3 ON cd.category_id = t3.category_id AND cd.source_id = t3.source_id
                LEFT JOIN
                    (SELECT category_id, 
                            ROUND(AVG(news_count)) AS avg_number_of_news_by_category_per_day,
                            MAX
                                (CASE WHEN row_num = 1 
                                THEN pub_date 
                                ELSE NULL
                                END) AS day_with_max_number_of_news_by_category
                    FROM
                        (SELECT category_id, 
                                pub_date, 
                                news_count, 
                                ROW_NUMBER() OVER(PARTITION BY category_id ORDER BY news_count DESC) AS row_num
                        FROM
                            (SELECT category_id, date(pub_date) AS pub_date, COUNT(DISTINCT news_id) AS news_count
                            FROM core_data
                            GROUP BY category_id, date(pub_date)
                        ) g1
                    ) g2
                    GROUP BY category_id
                ) t4 ON cd.category_id = t4.category_id
                LEFT JOIN
                    (SELECT 
                        category_id,
                        SUM(CASE WHEN day_of_week = 1 THEN news_count ELSE 0 END) AS news_count_monday,
                        SUM(CASE WHEN day_of_week = 2 THEN news_count ELSE 0 END) AS news_count_tuesday,
                        SUM(CASE WHEN day_of_week = 3 THEN news_count ELSE 0 END) AS news_count_wednesday,
                        SUM(CASE WHEN day_of_week = 4 THEN news_count ELSE 0 END) AS news_count_thursday,
                        SUM(CASE WHEN day_of_week = 5 THEN news_count ELSE 0 END) AS news_count_friday,
                        SUM(CASE WHEN day_of_week = 6 THEN news_count ELSE 0 END) AS news_count_saturday,
                        SUM(CASE WHEN day_of_week = 7 THEN news_count ELSE 0 END) AS news_count_sunday
                    FROM
                        (SELECT 
                            category_id, 
                            EXTRACT(ISODOW FROM pub_date) AS day_of_week, 
                            COUNT(DISTINCT news_id) AS news_count
                        FROM core_data
                        GROUP BY category_id, EXTRACT(ISODOW FROM pub_date)
                    ) g1
                    GROUP BY category_id
                ) t5 ON cd.category_id = t5.category_id
            GROUP BY 
                cd.category_id, 
                c.ctg_name, 
                s.id, 
                s.src_name,
                t1.number_of_news_by_category, 
                t2.number_of_news_by_category_last_day, 
                t3.number_of_news_by_category_and_source_last_day,
                t4.avg_number_of_news_by_category_per_day, 
                t4.day_with_max_number_of_news_by_category,
                t5.news_count_monday, 
                t5.news_count_tuesday, 
                t5.news_count_wednesday, 
                t5.news_count_thursday, 
                t5.news_count_friday, 
                t5.news_count_saturday, 
                t5.news_count_sunday
            ORDER BY cd.category_id, s.id;
        """)

#Создание тасков в слоях raw, core и mart
with DAG (
    dag_id = 'etl', 
    tags = [news], 
    start_date = datetime (2023, 9, 15), 
    shedule_interval = '0 0 * * *', 
    catchup = False, 
    default_args = default_args) as dag:

    raw_task = PythonOperator(
    task_id = 'raw_task',
    python_callable = raw_task)

    core_task = PythonOperator(
    task_id = 'core_task',
    python_callable = core_task)
    
    mart_task = PythonOperator(
    task_id = 'mart_task',
    python_callable = mart_task)

#Порядок выполнения тасков
raw_task >> core_task >> mart_task