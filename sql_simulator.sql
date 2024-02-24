--TASK 1
/*Для каждого дня, представленного в таблицах user_actions и courier_actions, рассчитайте следующие показатели:

Число новых пользователей.
Число новых курьеров.
Общее число пользователей на текущий день.
Общее число курьеров на текущий день.*/

SELECT tmp3.date as date,
       count(distinct user_id) as new_users,
       count(distinct courier_id) as new_couriers,
       sum(count(distinct courier_id)) OVER(ORDER BY tmp3.date) as total_couriers,
       sum(count(distinct user_id)) OVER(ORDER BY tmp3.date) as total_users
FROM   (SELECT *
        FROM   (SELECT user_id,
                       time::date as date,
                       row_number() OVER(PARTITION BY user_id
                                         ORDER BY time::date) as num_user
                FROM   user_actions) as tmp1
        WHERE  num_user = 1) as tmp3
    INNER JOIN (SELECT *
                FROM   (SELECT courier_id,
                               time::date as date,
                               row_number() OVER(PARTITION BY courier_id
                                                 ORDER BY time::date) as num_courier
                        FROM   courier_actions) as tmp2
                WHERE  num_courier = 1) as tmp4
        ON tmp3.date = tmp4.date
GROUP BY tmp3.date


--TASK 2
/*Дополните запрос из предыдущего задания и теперь для каждого дня, представленного в таблицах user_actions и courier_actions, дополнительно рассчитайте следующие показатели:

Прирост числа новых пользователей.
Прирост числа новых курьеров.
Прирост общего числа пользователей.
Прирост общего числа курьеров.*/

select date,
    new_users,
    new_couriers,
    total_couriers,
    total_users,
    new_users_change,
    new_couriers_change,
    round(100.0*(last_value(total_couriers) over(order by date rows between 1 preceding and current row) - first_value(total_couriers) over(order by date rows between 1 preceding and current row)) / (first_value(total_couriers) over(order by date rows between 1 preceding and current row)), 2) as total_users_growth,
    round(100.0*(last_value(total_users) over(order by date rows between 1 preceding and current row) - first_value(total_users) over(order by date rows between 1 preceding and current row)) / (first_value(total_users) over(order by date rows between 1 preceding and current row)), 2) as total_couriers_growth
from (
SELECT tmp3.date as date,
       count(distinct user_id) as new_users,
       count(distinct courier_id) as new_couriers,
       sum(count(distinct courier_id)) OVER(ORDER BY tmp3.date) as total_couriers,
       sum(count(distinct user_id)) OVER(ORDER BY tmp3.date) as total_users,
       round(100.0*(last_value(count(distinct user_id)) over(order by tmp3.date rows between 1 preceding and current row) - first_value(count(distinct user_id)) over(order by tmp3.date rows between 1 preceding and current row)) / (first_value(count(distinct user_id)) over(order by tmp3.date rows between 1 preceding and current row)), 2) as new_users_change,
       round(100.0*(last_value(count(distinct courier_id)) over(order by tmp3.date rows between 1 preceding and current row) - first_value(count(distinct courier_id)) over(order by tmp3.date rows between 1 preceding and current row)) / (first_value(count(distinct courier_id)) over(order by tmp3.date rows between 1 preceding and current row)), 2) as new_couriers_change
      --round((last_value(count(distinct user_id)) over(order by tmp3.date rows between 1 preceding and current row)).0 / first_value(count(distinct user_id)) over(order by tmp3.date rows between 1 preceding and current row), 2)
FROM   (SELECT *
        FROM   (SELECT user_id,
                       time::date as date,
                       row_number() OVER(PARTITION BY user_id
                                         ORDER BY time::date) as num_user
                FROM   user_actions) as tmp1
        WHERE  num_user = 1) as tmp3
    INNER JOIN (SELECT *
                FROM   (SELECT courier_id,
                               time::date as date,
                               row_number() OVER(PARTITION BY courier_id
                                                 ORDER BY time::date) as num_courier
                        FROM   courier_actions) as tmp2
                WHERE  num_courier = 1) as tmp4
        ON tmp3.date = tmp4.date
GROUP BY tmp3.date ) tmp


--TASK 3
/*Для каждого дня, представленного в таблицах user_actions и courier_actions, рассчитайте следующие показатели:

Число платящих пользователей.
Число активных курьеров.
Долю платящих пользователей в общем числе пользователей на текущий день.
Долю активных курьеров в общем числе курьеров на текущий день.*/

select
  poluitog1.date,
  paying_users,
  active_couriers,
  round(100.0 * paying_users / total_users) as paying_users_share,
  round(100.0 * active_couriers / total_couriers) as active_couriers_share
from(
    select
      tmp1.date as date,
      u as paying_users,
      c as active_couriers
    from
      (
        select
          count(distinct courier_id) as c,
          - - sum(count(distinct courier_id)) OVER(
            ORDER BY
              time :: date
          ) as total_c,
          time :: date as date
        from
          courier_actions
        where
          order_id in (
            select
              order_id
            from
              courier_actions
            where
              courier_actions.action = 'deliver_order'
          )
        group by
          time :: date
      ) as tmp1
      inner join (
        select
          count(distinct user_id) as u,
          - - sum(count(distinct user_id)) OVER(
            ORDER BY
              time :: date
          ) as total_u,
          time :: date as date
        from
          user_actions
        where
          order_id not in (
            select
              order_id
            from
              user_actions
            where
              user_actions.action = 'cancel_order'
          )
        group by
          time :: date
      ) as tmp2 on tmp1.date = tmp2.date
  ) as poluitog1
  inner join (
    SELECT
      tmp3.date as date,
      sum(count(distinct courier_id)) OVER(
        ORDER BY
          tmp3.date
      ) as total_couriers,
      sum(count(distinct user_id)) OVER(
        ORDER BY
          tmp3.date
      ) as total_users
    FROM
      (
        SELECT
          *
        FROM
          (
            SELECT
              user_id,
              time :: date as date,
              row_number() OVER(
                PARTITION BY user_id
                ORDER BY
                  time :: date
              ) as num_user
            FROM
              user_actions
          ) as tmp1
        WHERE
          num_user = 1
      ) as tmp3
      INNER JOIN (
        SELECT
          *
        FROM
          (
            SELECT
              courier_id,
              time :: date as date,
              row_number() OVER(
                PARTITION BY courier_id
                ORDER BY
                  time :: date
              ) as num_courier
            FROM
              courier_actions
          ) as tmp2
        WHERE
          num_courier = 1
      ) as tmp4 ON tmp3.date = tmp4.date
    GROUP BY
      tmp3.date
  ) as poluitog2 on poluitog1.date = poluitog2.date
  
  
--TASK 4
/*Для каждого дня, представленного в таблице user_actions, рассчитайте следующие показатели:

Долю пользователей, сделавших в этот день всего один заказ, в общем количестве платящих пользователей.
Долю пользователей, сделавших в этот день несколько заказов, в общем количестве платящих пользователей.*/

select
  tmp1.date,
  100.0 * num_users_1 / total_paid_users as single_order_users_share,
  100.0 * num_users / total_paid_users as several_orders_users_share
from
  (
    select
      count(distinct user_id) as total_paid_users,
      time :: date as date
    from
      user_actions
    where
      order_id not in (
        select
          order_id
        from
          user_actions
        where
          user_actions.action = 'cancel_order'
      )
    group by
      time :: date
  ) as tmp1
  left join (
    select
      date,
      count(distinct user_id) as num_users_1
    from
      (
        select
          user_id,
          time :: date as date,
          count(distinct order_id)
        from
          user_actions
        where
          order_id not in (
            select
              order_id
            from
              user_actions
            where
              action = 'cancel_order'
          )
        group by
          time :: date,
          user_id
        having
          count(distinct order_id) = 1
      ) as tmp
    group by
      date
  ) as tmp2 on tmp1.date = tmp2.date
  left join (
    select
      date,
      count(distinct user_id) as num_users
    from
      (
        select
          user_id,
          time :: date as date,
          count(distinct order_id)
        from
          user_actions
        where
          order_id not in (
            select
              order_id
            from
              user_actions
            where
              action = 'cancel_order'
          )
        group by
          time :: date,
          user_id
        having
          count(distinct order_id) > 1
      ) as tmpp
    group by
      date
  ) as tmp3 on tmp2.date = tmp3.date


--TASK 5
/*Для каждого дня, представленного в таблице user_actions, рассчитайте следующие показатели:

Общее число заказов.
Число первых заказов (заказов, сделанных пользователями впервые).
Число заказов новых пользователей (заказов, сделанных пользователями в тот же день, когда они впервые воспользовались сервисом).
Долю первых заказов в общем числе заказов (долю п.2 в п.1).
Долю заказов новых пользователей в общем числе заказов (долю п.3 в п.1).*/


select table1.date,
  orders,
  first_orders,
  new_users_orders,
  round(100 * first_orders :: decimal / orders, 2) as first_orders_share,
  round(100 * new_users_orders :: decimal / orders, 2) as new_users_orders_share
from
  (
    select
      time :: date as date,
      count(distinct order_id) as orders
    from
      user_actions
    where
      order_id not in (
        select
          order_id
        from
          user_actions
        where
          action = 'cancel_order'
      )
    group by
      time :: date
  ) as table1
  left join (
    select
      time :: date as date,
      count(user_id) as first_orders
    from
      (
        select
          row_number() over(
            partition by user_id
            order by
              time
          ) as num_user,
          *
        from
          user_actions
        where
          order_id not in (
            select
              order_id
            from
              user_actions
            where
              action = 'cancel_order'
          )
      ) as tmp
    where
      num_user = 1
    group by
      date
  ) as table2 on table1.date = table2.date
  left join (
    select
      tm1.date as date,
      count(order_id) as new_users_orders
    from
      (
        select
          min(time :: date) as date,
          user_id
        from
          user_actions
        group by
          user_id
      ) as tm1
      left join (
        select
          time :: date as date,
          user_id,
          order_id
        from
          user_actions
        where
          order_id not in (
            select
              order_id
            from
              user_actions
            where
              action = 'cancel_order'
          )
      ) as tm2 on tm1.date = tm2.date
      and tm1.user_id = tm2.user_id
    group by
      tm1.date
    order by
      tm1.date
  ) as table3 on table2.date = table3.date 
  


--TASK 6
/*На основе данных в таблицах user_actions, courier_actions и orders для каждого дня рассчитайте следующие показатели:

Число платящих пользователей на одного активного курьера.
Число заказов на одного активного курьера.*/


select
  tmp1.date,
  round(paying_users :: decimal / active_couriers, 2) as users_per_courier,
  round(orders :: decimal / active_couriers, 2) as orders_per_courier
from
  (
    select
      count(distinct courier_id) as active_couriers,
      - - sum(count(distinct courier_id)) OVER(
        ORDER BY
          time :: date
      ) as total_c,
      time :: date as date
    from
      courier_actions
    where
      order_id in (
        select
          order_id
        from
          courier_actions
        where
          courier_actions.action = 'deliver_order'
      )
    group by
      time :: date
  ) as tmp1
  left join (
    select
      count(distinct user_id) as paying_users,
      - - sum(count(distinct user_id)) OVER(
        ORDER BY
          time :: date
      ) as total_u,
      time :: date as date
    from
      user_actions
    where
      order_id not in (
        select
          order_id
        from
          user_actions
        where
          user_actions.action = 'cancel_order'
      )
    group by
      time :: date
  ) as tmp2 on tmp1.date = tmp2.date
  left join (
    select
      time :: date as date,
      count(distinct order_id) as orders
    from
      user_actions
    where
      order_id not in (
        select
          order_id
        from
          user_actions
        where
          action = 'cancel_order'
      )
    group by
      time :: date
  ) as tmp3 on tmp2.date = tmp3.date
  
  
  
--TASK 7
/*На основе данных в таблице courier_actions для каждого дня рассчитайте, за сколько минут в среднем курьеры доставляли свои заказы.*/


select tmp1.time::date as date,
 round(avg(EXTRACT('epoch' from tmp2.time - tmp1.time) / 60)) minutes_to_deliver
 from
(select order_id,
time
from courier_actions
where order_id in (select order_id
from courier_actions
where action ='deliver_order') and action = 'accept_order') as tmp1
inner join
(select order_id,
time
from courier_actions
where order_id in (select order_id
from courier_actions
where action ='deliver_order') and action = 'deliver_order') as tmp2
on tmp1.order_id = tmp2.order_id
group by tmp1.time::date
order by date



--TASK 8
/*На основе данных в таблице orders для каждого часа в сутках рассчитайте следующие показатели:

Число успешных (доставленных) заказов.
Число отменённых заказов.
Долю отменённых заказов в общем числе заказов (cancel rate).*/

select successful_hour,
successful_orders,
canceled_orders,
 round(canceled_orders::decimal / (successful_orders + canceled_orders), 3) as cancel_rate
 from
(select date_part('hour', creation_time) as successful_hour,
count(distinct order_id) as successful_orders
from orders
where order_id in (select order_id
from courier_actions
where action ='deliver_order')
group by successful_hour) as tmp1
left join
(select date_part('hour', creation_time) as canceled_hour,
count(distinct order_id) as canceled_orders
from orders
where order_id in (select order_id
from user_actions
where action ='cancel_order') 
group by canceled_hour) as tmp2
on tmp1.successful_hour = tmp2.canceled_hour
