import pytest
import subprocess


def run_command(command) -> tuple[int, str, str]:
    try:
        # Run the command and capture output
        result = subprocess.run(command, capture_output=True, text=True)
        return result.returncode, result.stdout, result.stderr
    except subprocess.CalledProcessError as e:
        return e.returncode, e.stdout, e.stderr


@pytest.mark.skip(reason="Not needed.")
@pytest.mark.benchmark(group="create_df")
@pytest.mark.parametrize("pl_version", ["0.20.3", "1.16.0", "1.19.0"])
def test_create_df(benchmark, pl_version):
    exit, stdout, stderr = run_command(
        ["uv", "pip", "install", f"polars=={pl_version}"]
    )
    if exit == 0:
        exit, stdout, stderr = run_command(["uv", "sync"])
    if exit != 0:
        print(stderr)
        assert False

    import polars as pl
    from main import create_df

    df = benchmark.pedantic(
        create_df, args=(1_000_000, "2021-01-01", "2021-12-31"), rounds=3, iterations=3
    )
    assert len(df) == 1_000_000


@pytest.mark.benchmark(group="filter_df")
@pytest.mark.parametrize("pl_version", ["0.20.3", "1.16.0", "1.19.0"])
def test_filter_df(benchmark, pl_version):
    exit, stdout, stderr = run_command(
        ["uv", "pip", "install", f"polars=={pl_version}"]
    )
    if exit == 0:
        exit, stdout, stderr = run_command(["uv", "sync"])
    if exit != 0:
        print(stderr)
        assert False

    import polars as pl
    from main import create_df

    df = create_df(1_000_000, "2021-01-01", "2021-12-31")
    dff = benchmark.pedantic(
        df.filter,
        args=(pl.col("date") == pl.lit("2021-01-01").str.to_date(),),
        rounds=10,
        iterations=1_000,
    )
    assert len(dff) > 0
