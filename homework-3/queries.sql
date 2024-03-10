-- Напишите запросы, которые выводят следующую информацию:
-- 1. Название компании заказчика (company_name из табл. customers) и ФИО сотрудника, работающего над заказом этой компании (см таблицу employees),
-- когда и заказчик и сотрудник зарегистрированы в городе London, а доставку заказа ведет компания United Package (company_name в табл shippers)
SELECT customers.company_name, CONCAT(employees.first_name, ' ', employees.last_name) AS employee_fullname
FROM orders
JOIN shippers ON orders.ship_via=shippers.shipper_id
JOIN customers USING (customer_id)
JOIN employees USING (employee_id)
WHERE orders.ship_via=shippers.shipper_id AND shippers.company_name='United Package'
AND customers.city='London'
AND employees.city='London'

-- 2. Наименование продукта, количество товара (product_name и units_in_stock в табл products),
-- имя поставщика и его телефон (contact_name и phone в табл suppliers) для таких продуктов,
-- которые не сняты с продажи (поле discontinued) и которых меньше 25 и которые в категориях Dairy Products и Condiments.
-- Отсортировать результат по возрастанию количества оставшегося товара.
SELECT product_name, units_in_stock, contact_name, phone
FROM products
JOIN suppliers USING (supplier_id)
JOIN categories USING (category_id)
WHERE discontinued = 0
AND units_in_stock < 25
AND category_name IN ('Dairy Products', 'Condiments')
ORDER BY units_in_stock

-- 3. Список компаний заказчиков (company_name из табл customers), не сделавших ни одного заказа
SELECT company_name
FROM customers
WHERE NOT EXISTS (
	SELECT 1 FROM orders
	WHERE orders.customer_id=customers.customer_id
)

-- 4. уникальные названия продуктов, которых заказано ровно 10 единиц (количество заказанных единиц см в колонке quantity табл order_details)
-- Этот запрос написать именно с использованием подзапроса.
SELECT DISTINCT product_name
FROM products
WHERE EXISTS (
	SELECT 1 FROM order_details
	WHERE order_details.product_id=products.product_id
	AND quantity = 10
)