# Решение продуктовых задач SQL  
В ходе этого учебного проекта выполнялся анализ данных работы интернет-магазина путем написания SQL-запросов и построения дашбордов в Redash.

## Условия задач  
1. Для каждого дня, представленного в таблицах user_actions и courier_actions, рассчитайте следующие показатели:

- Число новых пользователей.
- Число новых курьеров.
- Общее число пользователей на текущий день.
- Общее число курьеров на текущий день.

Колонки с показателями назовите соответственно new_users, new_couriers, total_users, total_couriers. Колонку с датами назовите date. Проследите за тем, чтобы показатели были выражены целыми числами. Результат должен быть отсортирован по возрастанию даты.  
Новыми будем считать тех пользователей и курьеров, которые в данный день совершили своё первое действие в нашем сервисе. Общее число пользователей/курьеров на текущий день — это результат сложения числа новых пользователей/курьеров в текущий день со значениями аналогичного показателя всех предыдущих дней.

2. Дополните запрос из предыдущего задания и теперь для каждого дня, представленного в таблицах user_actions и courier_actions, дополнительно рассчитайте следующие показатели:

- Прирост числа новых пользователей.
- Прирост числа новых курьеров.
- Прирост общего числа пользователей.
- Прирост общего числа курьеров.

Колонки с новыми показателями назовите соответственно new_users_change, new_couriers_change, total_users_growth, total_couriers_growth. Колонку с датами назовите date.

3. Для каждого дня, представленного в таблицах user_actions и courier_actions, рассчитайте следующие показатели:

- Число платящих пользователей.
- Число активных курьеров.
- Долю платящих пользователей в общем числе пользователей на текущий день.
- Долю активных курьеров в общем числе курьеров на текущий день.

Колонки с показателями назовите соответственно paying_users, active_couriers, paying_users_share, active_couriers_share. Колонку с датами назовите date. Проследите за тем, чтобы абсолютные показатели были выражены целыми числами. Все показатели долей необходимо выразить в процентах. При их расчёте округляйте значения до двух знаков после запятой.  
Платящими будем считать тех пользователей, которые в данный день оформили хотя бы один заказ, который в дальнейшем не был отменен.  
Курьеров будем считать активными, если в данный день они приняли хотя бы один заказ, который был доставлен (возможно, уже на следующий день), или доставили любой заказ.

4. Для каждого дня, представленного в таблице user_actions, рассчитайте следующие показатели:

- Долю пользователей, сделавших в этот день всего один заказ, в общем количестве платящих пользователей.
- Долю пользователей, сделавших в этот день несколько заказов, в общем количестве платящих пользователей.

Колонки с показателями назовите соответственно single_order_users_share, several_orders_users_share. Колонку с датами назовите date. Все показатели с долями необходимо выразить в процентах. При расчёте долей округляйте значения до двух знаков после запятой.

5. Для каждого дня, представленного в таблице user_actions, рассчитайте следующие показатели:

- Общее число заказов.
- Число первых заказов (заказов, сделанных пользователями впервые).
- Число заказов новых пользователей (заказов, сделанных пользователями в тот же день, когда они впервые воспользовались сервисом).
- Долю первых заказов в общем числе заказов (долю п.2 в п.1).
- Долю заказов новых пользователей в общем числе заказов (долю п.3 в п.1).

Колонки с показателями назовите соответственно orders, first_orders, new_users_orders, first_orders_share, new_users_orders_share. Колонку с датами назовите date. Проследите за тем, чтобы во всех случаях количество заказов было выражено целым числом. Все показатели с долями необходимо выразить в процентах. При расчёте долей округляйте значения до двух знаков после запятой.

6. На основе данных в таблицах user_actions, courier_actions и orders для каждого дня рассчитайте следующие показатели:

- Число платящих пользователей на одного активного курьера.
- Число заказов на одного активного курьера.

Колонки с показателями назовите соответственно users_per_courier и orders_per_courier. Колонку с датами назовите date. При расчёте показателей округляйте значения до двух знаков после запятой.  
Курьеров считаем активными, если в данный день они приняли хотя бы один заказ, который был доставлен (возможно, уже на следующий день), или доставили любой заказ.

7. На основе данных в таблице courier_actions для каждого дня рассчитайте, за сколько минут в среднем курьеры доставляли свои заказы.  
Колонку с показателем назовите minutes_to_deliver. Колонку с датами назовите date. При расчёте среднего времени доставки округляйте количество минут до целых значений. Учитывайте только доставленные заказы, отменённые заказы не учитывайте.  
Некоторые заказы оформляют в один день, а доставляют уже на следующий. При расчёте среднего времени доставки в качестве дней, для которых считать среднее, используйте дни фактической доставки заказов.

8. На основе данных в таблице orders для каждого часа в сутках рассчитайте следующие показатели:

- Число успешных (доставленных) заказов.
- Число отменённых заказов.
- Долю отменённых заказов в общем числе заказов (cancel rate).

Колонки с показателями назовите соответственно successful_orders, canceled_orders, cancel_rate. Колонку с часом оформления заказа назовите hour. При расчёте доли отменённых заказов округляйте значения до трёх знаков после запятой.



*Дашборды представлены в .jpg файлах*