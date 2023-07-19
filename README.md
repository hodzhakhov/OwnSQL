# OwnSQL

Реляционная база данных в памяти с поддержкой упрощенных sql запросов.

### Поддерживаемый синтаксис

Ключевые слова:

- SELECT
- FROM
- WHERE
- (LEFT|RIGHT|INNER)JOIN
- CREATE TABLE
- DROP TABLE
- AND
- OR
- IS
- NOT
- NULL
- ON
- UPDATE
- INSERT
- VALUES
- DELETE
- PRIMARY KEY
- FOREIGN KEY

Поддерживаемые типы данных:

- bool
- int
- float
- double
- varchar

Ограничения:

- вложенные подзапросы не поддерживаются
- Join только для 2 таблиц
