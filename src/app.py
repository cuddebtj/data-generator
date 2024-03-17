import datetime
import random
from pathlib import Path
from pprint import pprint

import polars as pl


def end_of_month_date(any_day: datetime.date) -> datetime.date:
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    return next_month - datetime.timedelta(days=next_month.day)


def split_monthly_sales_amt(ran_num: int) -> dict[str, float | int]:
    rand_sales_price = random.randint(55, 70) / 10
    round_qty = round(ran_num / rand_sales_price)
    round_sales = round_qty * rand_sales_price
    hot_ran = random.randint(4, 7)
    cold_ran = 10 - hot_ran
    food = 2
    hot_drink_sales = round(round_sales * (hot_ran / 10), 2)
    hot_drink_qty = round(round_qty * (hot_ran / 10))
    cold_drink_sales = round(round_sales * (cold_ran / 10), 2)
    cold_drink_qty = round(round_qty * (cold_ran / 10))
    food_sales = round(round_sales * (food / 10), 2)
    food_qty = round(round_qty * (food / 10))
    sales = {
        "Hot Drink": hot_drink_sales,
        "hot_drink_qty": hot_drink_qty,
        "Cold Drink": cold_drink_sales,
        "cold_drink_qty": cold_drink_qty,
        "Food": food_sales,
        "food_qty": food_qty,
    }
    return sales


order_per_location_range = (10_000, 14_000)

data_path = Path("./data")
locations = data_path / "DutchBrosLocations20211025.csv"
locations_df = pl.read_csv(locations)

locations_ids_df = pl.DataFrame({"store_id": locations_df["Index"].to_list()})
date_range_df = pl.DataFrame(
    {
        "month": [
            end_of_month_date(datetime.date.today() - datetime.timedelta(days=30 * i))
            for i in range(14)
        ]
    }
)

date_loc_dicts = date_range_df.join(locations_ids_df, how="cross").to_dicts()

for record in date_loc_dicts:
    ran_num = random.randint(
        order_per_location_range[0],
        order_per_location_range[1],
    )
    record.update(split_monthly_sales_amt(ran_num))

df = pl.from_dicts(date_loc_dicts)
df_hot = (
    df.select(
        [
            "month",
            "store_id",
            "Hot Drink",
        ]
    )
    .melt(
        id_vars=[
            "month",
            "store_id",
        ],
        variable_name="product_category",
        value_name="monthly_sales_amt",
    )
    .join(
        (
            df.select(
                [
                    "month",
                    "store_id",
                    "hot_drink_qty",
                ]
            )
            .melt(
                id_vars=[
                    "month",
                    "store_id",
                ],
                variable_name="product_category",
                value_name="monthly_order_qty",
            )
            .select(
                [
                    "month",
                    "store_id",
                    "monthly_order_qty",
                ]
            )
        ),
        ["month", "store_id"],
    )
)

df_cold = (
    df.select(
        [
            "month",
            "store_id",
            "Cold Drink",
        ]
    )
    .melt(
        id_vars=[
            "month",
            "store_id",
        ],
        variable_name="product_category",
        value_name="monthly_sales_amt",
    )
    .join(
        (
            df.select(
                [
                    "month",
                    "store_id",
                    "cold_drink_qty",
                ]
            )
            .melt(
                id_vars=[
                    "month",
                    "store_id",
                ],
                variable_name="product_category",
                value_name="monthly_order_qty",
            )
            .select(
                [
                    "month",
                    "store_id",
                    "monthly_order_qty",
                ]
            )
        ),
        ["month", "store_id"],
    )
)

df_food = (
    df.select(
        [
            "month",
            "store_id",
            "Food",
        ]
    )
    .melt(
        id_vars=[
            "month",
            "store_id",
        ],
        variable_name="product_category",
        value_name="monthly_sales_amt",
    )
    .join(
        (
            df.select(
                [
                    "month",
                    "store_id",
                    "food_qty",
                ]
            )
            .melt(
                id_vars=[
                    "month",
                    "store_id",
                ],
                variable_name="product_category",
                value_name="monthly_order_qty",
            )
            .select(
                [
                    "month",
                    "store_id",
                    "monthly_order_qty",
                ]
            )
        ),
        ["month", "store_id"],
    )
)

final_df = pl.concat([df_hot, df_cold, df_food], how="vertical")
final_df = final_df.join(
    locations_df, left_on="store_id", right_on="Index", how="outer"
)

final_df.drop(["index", "Index"]).write_csv("data/test.csv")
