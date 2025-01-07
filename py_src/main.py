import polars as pl
from datetime import datetime, timedelta
from time import perf_counter


def fmt_time(t: float) -> str:
    return str(timedelta(seconds=t))


def create_df(rows: int, dt_start: str, dt_end: str) -> pl.DataFrame:
    dt_start_dt = datetime.strptime(dt_start, "%Y-%m-%d")
    dt_end_dt = datetime.strptime(dt_end, "%Y-%m-%d")
    # uniformly choose dates between dt_start and dt_end
    total_days = (dt_end_dt - dt_start_dt).days
    dates = [
        dt_start_dt + timedelta(days=(i * total_days) // rows) for i in range(rows)
    ]
    df = pl.DataFrame(
        {
            "date": dates,
            "value": [i for i in range(rows)],
        }
    )
    return df


def main():
    st = perf_counter()
    df = create_df(1_000_000, "2021-01-01", "2021-12-31")
    print(
        f"Time taken to create DataFrame ({len(df)}): ", fmt_time(perf_counter() - st)
    )
    print(df)

    dff = df.filter(pl.col("date") == pl.lit("2021-01-01").str.to_date())
    print(len(dff))


if __name__ == "__main__":
    main()
