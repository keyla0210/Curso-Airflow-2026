from pathlib import Path

def build_dim_time(output_path: Path | str) -> str:
    import pandas as pd
    from datetime import datetime

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ----------------------------
    # Definir rango dinámico
    # ----------------------------
    start_date = "2000-01-01"
    current_year = datetime.today().year
    end_year = current_year + 5
    end_date = f"{end_year}-12-31"

    # ----------------------------
    # Generar calendario
    # ----------------------------
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")

    dim = pd.DataFrame({"date": date_range})

    # ----------------------------
    # Atributos
    # ----------------------------
    dim["date_key"] = dim["date"].dt.strftime("%Y%m%d").astype(int)
    dim["date_iso"] = dim["date"].dt.strftime("%Y-%m-%d")

    dim["year"] = dim["date"].dt.year
    dim["quarter"] = dim["date"].dt.quarter
    dim["month"] = dim["date"].dt.month
    dim["month_name"] = dim["date"].dt.month_name()
    dim["day"] = dim["date"].dt.day
    dim["day_name"] = dim["date"].dt.day_name()
    dim["week_of_year"] = dim["date"].dt.isocalendar().week.astype(int)

    dim["is_weekend"] = dim["day_name"].isin(["Saturday", "Sunday"])

    # Extras útiles en BI
    dim["year_month"] = dim["date"].dt.strftime("%Y-%m")
    dim["year_week"] = dim["year"].astype(str) + "-W" + dim["week_of_year"].astype(str).str.zfill(2)

    # ----------------------------
    # Orden tipo dimensional
    # ----------------------------
    dim = dim[
        [
            "date_key",
            "date_iso",
            "date",
            "year",
            "quarter",
            "month",
            "month_name",
            "year_month",
            "week_of_year",
            "year_week",
            "day",
            "day_name",
            "is_weekend",
        ]
    ]

    # ----------------------------
    # Guardar
    # ----------------------------
    dim.to_parquet(output_path, index=False)

    return str(output_path)