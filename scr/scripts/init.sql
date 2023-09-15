#Создание таблицы сырых данных
CREATE TABLE IF NOT EXISTS raw_data (
	id VARCHAR(100) NULL,
	source_name VARCHAR(100) NULL,
	link VARCHAR(100) NULL,
	title VARCHAR(100) NULL,
	category VARCHAR(100) NULL,
	pub_date DATETIME NULL
);

#Создание и заполнение таблицы категорий новостей
CREATE TABLE IF NOT EXISTS categories (
	id INTEGER NOT NULL,
	ctg_name varchar(100) NOT NULL,
	CONSTRAINT categories_pk PRIMARY KEY (id)
);

INSERT INTO categories(id, ctg_name) VALUES (1, 'Общество');
INSERT INTO categories(id, ctg_name) VALUES (2, 'Интернет и СМИ');
INSERT INTO categories(id, ctg_name) VALUES (3, 'Культура');
INSERT INTO categories(id, ctg_name) VALUES (4, 'Международная панорама');
INSERT INTO categories(id, ctg_name) VALUES (5, 'Моя страна');
INSERT INTO categories(id, ctg_name) VALUES (6, 'Наука и техника');
INSERT INTO categories(id, ctg_name) VALUES (7, 'Происшествия');
INSERT INTO categories(id, ctg_name) VALUES (8, 'Спорт');
INSERT INTO categories(id, ctg_name) VALUES (9, 'Экономика и бизнес');
INSERT INTO categories(id, ctg_name) VALUES (10, 'Политика');

#Создание и заполнение таблицы источников новостных данных
CREATE TABLE IF NOT EXISTS sources (
	id INTEGER NOT NULL,
	source_name VARCHAR(100) NOT NULL,
	url_rss VARCHAR(100) NOT NULL,
	CONSTRAINT sources_pk PRIMARY KEY (id)
);

INSERT INTO sources(id, name, url_rss) 
VALUES
	(1, 'lenta.ru', 'https://lenta.ru/rss'),
	(2, 'tass.ru', 'https://tass.ru/rss/v2.xml'),
	(3, 'vedomosti.ru', 'https://www.vedomosti.ru/rss/news');
	
#Создание и заполнение таблицы категорий источников новостей
CREATE TABLE IF NOT EXISTS source_categories (
	id INTEGER NOT NULL,
	source_id INTEGER NOT NULL,
	ctg_src_name VARCHAR(100) NOT NULL,
	CONSTRAINT source_categories_pk PRIMARY KEY (id)
);

INSERT INTO source_categories(id, source_id, ctg_src_name)
VALUES
	(1, 1, 'Забота о себе'),
	(2, 1, 'Из жизни'),
	(3, 1, 'Путешествия'),
	(4, 1, 'Среда обитания'),
	(5, 1, 'Интернет и СМИ'),
	(6, 1, 'Культура'),
	(7, 1, 'Мир'),
	(8, 1, 'Бывший СССР'),
	(9, 1, 'Моя страна'),
	(10, 1, 'Россия'),
	(11, 1, 'Наука и техника'),
	(12, 1, 'Силовые структуры'),
	(13, 1, 'Спорт'),
	(14, 1, 'Экономика');

INSERT INTO source_categories(id, source_id, ctg_src_name)
VALUES
	(15, 2, 'Общество'),
	(16, 2, 'Культура'),
	(17, 2, 'Международная панорама'),
	(18, 2, 'Армия и ОПК'),
	(19, 2, 'В стране'),
	(20, 2, 'Москва'),
	(21, 2, 'Московская область'),
	(22, 2, 'Северо-Запад'),
	(23, 2, 'Новости Урала'),
	(24, 2, 'Сибирь'),
	(25, 2, 'Национальные проекты'),
	(26, 2, 'Наука'),
	(27, 2, 'Космос'),
	(28, 2, 'Происшествия'),
	(29, 2, 'Спорт'),
	(30, 2, 'Экономика и бизнес'),
	(31, 2, 'Малый бизнес'),
	(32, 2, 'Политика');

INSERT INTO source_categories(id, source_id, ctg_src_name)
VALUES
	(33, 3, 'Общество'),
	(34, 3, 'Менеджмент'),
	(35, 3, 'Карьера'),
	(36, 3, 'Медиа'),
	(37, 3, 'Стиль жизни'),
	(38, 3, 'Технологии'),
	(39, 3, 'Авто'),
	(40, 3, 'Экономика'),
	(41, 3, 'Бизнес'),
	(42, 3, 'Финансы'),
	(43, 3, 'Инвестиции'),
	(44, 3, 'Личный счет'),
	(45, 3, 'Политика');

#Установление взаимосвязи категорий новостей и категорий из разных источников данных. Заполнение таблицы взаимосвязи данными
CREATE TABLE IF NOT EXISTS categories_relationship (
	category_id INTEGER NOT NULL,
	source_category_id INTEGER NULL,
	CONSTRAINT categories_relationship_pk PRIMARY KEY (category_id, source_category_id),
	CONSTRAINT categories_relationship_fk_categories FOREIGN KEY (category_id) REFERENCES categories(id),
	CONSTRAINT categories_relationship_fk_source_categories FOREIGN KEY (source_category_id) REFERENCES source_categories(id)
);

INSERT INTO categories_relationship(category_id, source_category_id)
VALUES
	(1, 1),
	(1, 2),
	(1, 3),
	(1, 4),
	(1, 15),
	(1, 33),
	(1, 34),
	(1, 35),
	(2, 5),
	(2, 36),
	(3, 6),
	(3, 16),
	(3, 37),
	(4, 7),
	(4, 8),
	(4, 17),
	(4, 18),
	(5, 9),
	(5, 10),
	(5, 19),
	(5, 20),
	(5, 21),
	(5, 22),
	(5, 23),
	(5, 24),
	(5, 25),
	(6, 11),
	(6, 26),
	(6, 27),
	(6, 38),
	(6, 39),
	(7, 12),
	(7, 28),
	(8, 13),
	(8, 29),
	(9, 14),
	(9, 30),
	(9, 31),
	(9, 40),
	(9, 41),
	(9, 42),
	(9, 43),
	(9, 44),
	(10, 7),
	(10, 32),
	(10, 45);

#Создание таблицы с обработанными данными
CREATE TABLE IF NOT EXISTS core_data (
	news_id VARCHAR(100) NOT NULL,
	category_id INTEGER NOT NULL,
	source_id INTEGER NOT NULL,
	pub_date DATETIME NOT NULL,
	link VARCHAR(100) NULL,
	title VARCHAR(100) NULL,
	CONSTRAINT core_data_pk PRIMARY KEY (news_id, category_id),
	CONSTRAINT core_data_fk_categories FOREIGN KEY (category_id) REFERENCES categories(id),
	CONSTRAINT core_data_fk_sources FOREIGN KEY (source_id) REFERENCES sources(id)
);

#Создание таблицы витрины данных со статистиской новостей по категориям
CREATE TABLE mart_data (
	category_id INT NOT NULL,
	category_name VARCHAR(100) NOT NULL,
	source_name VARCHAR(100) NOT NULL,
	number_of_news_by_category INT NULL,
	number_of_news_by_category_and_source INT NULL,
	number_of_news_by_category_last_day INT NULL,
	number_of_news_by_category_and_source_last_day INT NULL,
	avg_number_of_news_by_category_per_day INT NULLL,
	day_with_max_number_of_news_by_category DATE NULL,
	news_count_monday INT NULL,
	news_count_tuesday INT NULL,
	news_count_wednesday INT NULL,
	news_count_thursday INT NULL,
	news_count_friday INT NULL,
	news_count_saturday INT NULL,
	news_count_sunday INT NULL
);
