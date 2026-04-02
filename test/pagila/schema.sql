-- Simplified pagila schema using only INT, TEXT, BOOL types.
-- This is a subset of the pagila dataset adapted for LolaDB's type system.

CREATE TABLE actor (
    actor_id INT,
    first_name TEXT,
    last_name TEXT
);

CREATE TABLE category (
    category_id INT,
    name TEXT
);

CREATE TABLE film (
    film_id INT,
    title TEXT,
    description TEXT,
    release_year INT,
    rental_duration INT,
    rental_rate INT,
    length INT,
    rating TEXT
);

CREATE TABLE film_actor (
    actor_id INT,
    film_id INT
);

CREATE TABLE film_category (
    film_id INT,
    category_id INT
);

CREATE TABLE customer (
    customer_id INT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    active INT
);

CREATE TABLE store (
    store_id INT,
    manager_staff_id INT,
    address TEXT
);

CREATE TABLE inventory (
    inventory_id INT,
    film_id INT,
    store_id INT
);

CREATE TABLE rental (
    rental_id INT,
    inventory_id INT,
    customer_id INT,
    staff_id INT,
    status TEXT
);

CREATE TABLE payment (
    payment_id INT,
    customer_id INT,
    staff_id INT,
    rental_id INT,
    amount INT
);
