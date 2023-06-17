

def creating_general_mart_order(user_dataframe, car_dataframe, order_dataframe):
    user = user_dataframe.withColumnRenamed("id", "user_id_user")
    user = user.drop(user.created_at)
    user = user.drop(user.updated_at)
    car = car_dataframe.withColumnRenamed("id", "car_id_car")
    car = car.drop(car.created_at)
    car = car.drop(car.updated_at)
    user_car = user.join(car, car.user_id == user.user_id_user)
    user_car = user_car.drop(user_car.user_id_user)
    order_user_car = user_car.join(order_dataframe, ((user_car.user_id == order_dataframe.user_id) & (user_car.car_id_car == order_dataframe.car_id)))
    order_user_car = order_user_car.drop(user_car.user_id)
    order_user_car = order_user_car.drop(user_car.car_id_car)
    order_user_car = order_user_car.withColumnRenamed("id", "order_id")
    order_user_car = order_user_car.select('order_id',
                                           'car_id',
                                           'user_id',
                                           'order_status',
                                           'payment_status',
                                           'total_value',
                                           'order_discount',
                                           'first_name',
                                           'last_name',
                                           'cellphone',
                                           'username',
                                           'email',
                                           'gender',
                                           'model',
                                           'fuel',
                                           'manufacturer',
                                           'color',
                                           'created_at',
                                           'updated_at')
    order_user_car = order_user_car.toPandas()
    return order_user_car