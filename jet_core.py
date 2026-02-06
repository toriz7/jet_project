import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd
import holidays  # 한국 공휴일 계산용 (pip install holidays)
import logging

TABLE_NAME = "journal_entries"

# ===============================
# 로거 설정 (jet_app.jet_core)
# ===============================
logger = logging.getLogger("jet_app.jet_core")
# 상위(root) 로거에 핸들러가 하나도 없을 때만 기본 설정
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

# ===============================
# 필수 컬럼 (NULL 허용, 존재 여부만 체크)
# ===============================
REQUIRED_COLUMNS = [
    "회계연도",
    "전기일",
    "전표번호",
    "전표라인번호",
    "계정코드",
    "계정명",
    "차대변지시자",
    "현지통화금액",
    "라인적요",
    "거래처",
    "거래처명",
    "사용자명",
]


def summarize_csv(df: pd.DataFrame) -> Dict:
    """CSV 기본 정보 요약"""
    summary = {
        "total_rows": df.shape[0],
        "total_cols": df.shape[1],
        "columns": list(df.columns),
        "missing_required_columns": [
            c for c in REQUIRED_COLUMNS if c not in df.columns
        ],
        "null_counts": {},
    }

    # NULL 개수는 정보 제공용 (오류 처리 X)
    for col in REQUIRED_COLUMNS:
        if col in df.columns:
            summary["null_counts"][col] = int(df[col].isna().sum())

    return summary


def load_csv_to_sqlite(
    file_obj,
    encoding: str = "utf-8-sig",
) -> Tuple[Optional[sqlite3.Connection], Dict]:
    """
    CSV → DataFrame → in-memory SQLite(journal_entries) 적재 + 요약 정보 반환
    오류가 있으면 conn 대신 None을 반환
    """
    logger.info("[load_csv_to_sqlite] start (encoding=%s)", encoding)

    df = pd.read_csv(file_obj, encoding=encoding, low_memory=False)
    df.columns = [c.strip() for c in df.columns]

    logger.info(
        "[load_csv_to_sqlite] CSV loaded: rows=%d, cols=%d, columns=%s",
        df.shape[0],
        df.shape[1],
        list(df.columns),
    )

    # 날짜 정규화
    if "전기일" in df.columns:
        df["전기일"] = (
            df["전기일"].astype(str)
            .str.replace(r"\D", "", regex=True)
            .str[-8:]
        )

    # 금액 정규화
    if "현지통화금액" in df.columns:
        df["현지통화금액"] = (
            df["현지통화금액"]
            .astype(str)
            .str.replace(",", "")
            .str.replace(" ", "")
        )
        df["현지통화금액"] = pd.to_numeric(df["현지통화금액"], errors="coerce")

    # CSV 요약
    summary = summarize_csv(df)
    errors: List[str] = []

    # 1) 행 0개 에러
    if summary["total_rows"] == 0:
        errors.append("CSV 데이터가 0행입니다.")

    # 2) 필수 컬럼 존재 여부 체크 (NULL은 허용)
    if summary["missing_required_columns"]:
        miss = ", ".join(summary["missing_required_columns"])
        errors.append(f"누락된 필수 컬럼: {miss}")

    summary["errors"] = errors

    if errors:
        logger.error(
            "[load_csv_to_sqlite] validation failed: %s",
            " | ".join(errors),
        )
        return None, summary

    conn = sqlite3.connect(":memory:")
    df.to_sql(TABLE_NAME, conn, if_exists="replace", index=False)

    logger.info(
        "[load_csv_to_sqlite] success: in-memory DB created, table=%s, rows=%d",
        TABLE_NAME,
        df.shape[0],
    )

    return conn, summary


# ===============================
# 연도 파생
# ===============================


def derive_years_from_db(conn: sqlite3.Connection) -> List[int]:
    """
    DB에 적재된 분개 데이터에서 분석 대상 연도 리스트를 도출한다.
    우선순위:
      1) 회계연도 컬럼이 있으면 숫자로 변환해 사용
      2) 없거나 전부 결측이면 전기일 앞 4자리로 연도 추출
    """
    years: List[int] = []

    # 1) 회계연도 우선
    try:
        df_year = pd.read_sql_query(
            f"SELECT DISTINCT 회계연도 FROM {TABLE_NAME}", conn
        )
        if not df_year.empty:
            years = (
                pd.to_numeric(df_year["회계연도"], errors="coerce")
                .dropna()
                .astype(int)
                .tolist()
            )
    except Exception:
        # 회계연도 컬럼이 없을 수도 있으므로 무시
        pass

    # 2) 전기일 앞 4자리 보조
    if not years:
        try:
            df_dt = pd.read_sql_query(
                f"SELECT DISTINCT 전기일 FROM {TABLE_NAME}", conn
            )
            if not df_dt.empty:
                years = (
                    df_dt["전기일"]
                    .astype(str)
                    .str[:4]
                    .pipe(pd.to_numeric, errors="coerce")
                    .dropna()
                    .astype(int)
                    .tolist()
                )
        except Exception:
            pass

    if not years:
        raise ValueError("회계연도 또는 전기일에서 연도를 추출할 수 없습니다.")

    min_y, max_y = min(years), max(years)
    full_range = list(range(min_y, max_y + 1))

    logger.info(
        "[derive_years_from_db] detected years: raw=%s -> range=%s",
        years,
        full_range,
    )

    return full_range


# ===============================


def create_korean_holiday_table(conn: sqlite3.Connection, year: int) -> str:
    """연도별 토/일 + 공휴일 테이블 생성"""
    table_name = f"holiday_{year}"
    logger.info(
        "[create_korean_holiday_table] building holiday table for year=%d -> %s",
        year,
        table_name,
    )

    kr_holidays = holidays.KR(years=[year])
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)

    dates: List[str] = []
    for i in range((end_date - start_date).days + 1):
        d = start_date + timedelta(days=i)
        dt = d.strftime("%Y%m%d")

        if d.weekday() >= 5 or (d in kr_holidays):  # 토/일 또는 공휴일
            dates.append(dt)

    pd.DataFrame({"dt": dates}).to_sql(
        table_name, conn, if_exists="replace", index=False
    )

    logger.info(
        "[create_korean_holiday_table] table=%s created (rows=%d)",
        table_name,
        len(dates),
    )

    return table_name


def build_quarter_last_two_weeks_condition(year: int, field: str = "dt") -> str:
    """
    분기 마지막 달(3,6,9,12월)의 말일 기준으로 '2주 전부터 말일 이후 2주까지' 범위를 OR 조건으로 생성.
    - 3월: 3월 17일 ~ 4월 14일
    - 6월: 6월 16일 ~ 7월 14일
    - 9월: 9월 16일 ~ 10월 14일
    - 12월: 12월 17일 ~ 다음해 1월 14일
    """
    from datetime import datetime, timedelta
    
    ranges = []
    # (month, last_day)
    for month, last_day in [(3, 31), (6, 30), (9, 30), (12, 31)]:
        # 분기 말일
        quarter_end = datetime(year, month, last_day)
        # 2주 전 (14일 전)
        start_date = quarter_end - timedelta(days=14)
        # 말일 이후 2주 (14일 후)
        end_date = quarter_end + timedelta(days=14)
        
        start = start_date.strftime("%Y%m%d")
        end = end_date.strftime("%Y%m%d")
        ranges.append(f"({field} BETWEEN '{start}' AND '{end}')")
    return " OR ".join(ranges)

# ===============================
# JET ①~⑨(+⑩) 쿼리
# ===============================


def get_jet_queries(
    year: int,
    holiday_table: str,
    person_names: Optional[List[str]] = None,
    min_amount: float = 1000000000.0,
    min_count_account: int = 3,
    min_count_user: int = 3,
    min_count_customer: int = 3,
) -> Dict[str, str]:
    logger.info(
        "[get_jet_queries] preparing queries for year=%d, holiday_table=%s, person_names=%s, min_amount=%.0f, min_count_account=%d, min_count_user=%d, min_count_customer=%d",
        year,
        holiday_table,
        person_names,
        min_amount,
        min_count_account,
        min_count_user,
        min_count_customer,
    )

    queries: Dict[str, str] = {}

    quarter_last_two_weeks = build_quarter_last_two_weeks_condition(
        year, field="dt"
    )

    # ① 휴일(분기말) + 지정 금액 이상
    queries["① 분기말_휴일_지정금액이상_분개"] = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE 전기일 IN (
            SELECT dt FROM {holiday_table}
            WHERE {quarter_last_two_weeks}
        )
        AND ABS(현지통화금액) >= {min_amount}
        ORDER BY 전표번호, 전표라인번호
    """

    # ② 적요 '조정' 포함
    queries["② 적요_조정_포함"] = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE 라인적요 LIKE '%조정%'
    """

    # ③ 분기말(2주 전~분기 후 2주) 적요 '수정' 포함
    quarter_period = build_quarter_last_two_weeks_condition(year, field="전기일")
    queries["③ 분기말_수정_적요"] = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE 라인적요 LIKE '%수정%'
          AND ({quarter_period})
        ORDER BY 전기일, 전표번호, 전표라인번호
    """

    # ④ 대차불일치 전표 세트
    queries["④ 대차불일치_전표세트"] = f"""
        SELECT 
            전표번호,
            SUM(현지통화금액) AS 전표별_합계,
            SUM(CASE WHEN 차대변지시자 = 'D' THEN 현지통화금액 ELSE 0 END) AS 차변합,
            SUM(CASE WHEN 차대변지시자 = 'C' THEN 현지통화금액 ELSE 0 END) AS 대변합,
            COUNT(*) AS 전표라인수
        FROM {TABLE_NAME}
        GROUP BY 전표번호
        HAVING ABS(차변합) <> ABS(대변합);
    """

    # ⑤ 누락된 전표번호
    queries["⑤ 누락된_전표번호"] = f"""
        WITH numeric_vouchers AS (
            SELECT DISTINCT 회계연도, CAST(전표번호 AS INTEGER) AS n
            FROM {TABLE_NAME}
            WHERE 전표번호 GLOB '[0-9]*'
              AND 전표번호 NOT GLOB '*[^0-9]*'
              AND 전표번호 != ''
        ),
        ordered AS (
            SELECT
                회계연도,
                n,
                LEAD(n) OVER (PARTITION BY 회계연도 ORDER BY n) AS next_n
            FROM numeric_vouchers
        ),
        gaps AS (
            SELECT
                회계연도,
                n + 1 AS start_missing,
                next_n - 1 AS end_missing
            FROM ordered
            WHERE next_n IS NOT NULL
              AND next_n > n + 1
        )
        SELECT
            회계연도,
            start_missing AS 누락시작전표번호,
            end_missing   AS 누락끝전표번호,
            (end_missing - start_missing + 1) AS 누락갯수
        FROM gaps
        ORDER BY 회계연도, start_missing
    """

    # ⑥ 특정 계정이 N회 이하
    queries["⑥ 계정별_N회이하"] = f"""
        WITH low_count_accounts AS (
            SELECT 회계연도, 계정명
            FROM {TABLE_NAME}
            GROUP BY 계정명, 회계연도
            HAVING COUNT(*) <= {min_count_account}
        )
        SELECT j.*
        FROM {TABLE_NAME} j
        INNER JOIN low_count_accounts l
          ON j.계정명 = l.계정명
          AND j.회계연도 = l.회계연도
        ORDER BY j.회계연도, j.계정명, j.전표번호, j.전표라인번호
    """

    # ⑦ 사용자별 N회 이하
    queries["⑦ 사용자별_N회이하"] = f"""
        WITH low_count_users AS (
            SELECT 회계연도, 사용자명
            FROM {TABLE_NAME}
            GROUP BY 사용자명, 회계연도
            HAVING COUNT(*) <= {min_count_user}
        )
        SELECT j.*
        FROM {TABLE_NAME} j
        INNER JOIN low_count_users l
          ON j.사용자명 = l.사용자명
          AND j.회계연도 = l.회계연도
        ORDER BY j.회계연도, j.사용자명, j.전표번호, j.전표라인번호
    """


    # ⑧ 거래처가 N회 이하
    queries["⑧ 거래처별_N회이하"] = f"""
        WITH low_count_customers AS (
            SELECT 회계연도, 거래처명
            FROM {TABLE_NAME}
            GROUP BY 거래처명, 회계연도
            HAVING COUNT(*) <= {min_count_customer}
        )
        SELECT j.*
        FROM {TABLE_NAME} j
        INNER JOIN low_count_customers l
          ON j.거래처명 = l.거래처명
          AND j.회계연도 = l.회계연도
        ORDER BY j.회계연도, j.거래처명, j.전표번호, j.전표라인번호
    """

    # ⑨ 금액의 천원단위가 0000 (10,000원 단위 배수)
    queries["⑨ 금액_만원단위_0000"] = f"""
        SELECT * FROM {TABLE_NAME}
        WHERE 현지통화금액 IS NOT NULL
          AND 현지통화금액 != 0
          AND 현지통화금액 % 10000 = 0
    """

    # ⑩ 주요 인물 이름이 거래처명에 포함되는 분개
    # 항상 ⑩을 생성하되, person_names가 없으면 빈 결과를 반환하는 쿼리 사용
    if person_names:
        cleaned = [n.strip() for n in person_names if n and n.strip()]
        if cleaned:
            like_conditions = []
            for name in cleaned:
                safe = name.replace("'", "''")  # 작은따옴표 이스케이프
                like_conditions.append(f"거래처명 LIKE '%{safe}%'")

            where_clause = " OR ".join(like_conditions)

            queries["⑩ 주요인물_거래처명_매칭"] = f"""
                SELECT *
                FROM {TABLE_NAME}
                WHERE 거래처명 IS NOT NULL
                  AND ({where_clause})
                ORDER BY 전기일, 전표번호, 전표라인번호
            """
        else:
            # person_names가 비어있으면 빈 결과 반환
            queries["⑩ 주요인물_거래처명_매칭"] = f"""
                SELECT *
                FROM {TABLE_NAME}
                WHERE 1=0
            """
    else:
        # person_names가 None이면 빈 결과 반환
        queries["⑩ 주요인물_거래처명_매칭"] = f"""
            SELECT *
            FROM {TABLE_NAME}
            WHERE 1=0
        """

    # ⑪ 마이너스(-) 금액 분개
    queries["⑪ 마이너스_금액_분개"] = f"""
        SELECT *
        FROM {TABLE_NAME}
        WHERE 현지통화금액 < 0
        ORDER BY 전표번호, 전표라인번호
    """

    logger.info("[get_jet_queries] %d queries prepared", len(queries))
    return queries


# ===============================
# JET 실행 + 디버깅용 중간 테이블 생성
# ===============================


def run_jet_tests(
    conn: sqlite3.Connection,
    years: List[int],
    person_names: Optional[List[str]] = None,
    person_birthdates: Optional[List[str]] = None,
    min_amount: float = 1000000000.0,
    min_count_account: int = 3,
    min_count_user: int = 3,
    min_count_customer: int = 3,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str], Dict[str, pd.DataFrame]]:
    """
    여러 연도에 대해 휴일 테이블을 생성 후 ①~⑪ 쿼리를 실행한다.
    연도별 결과를 하나로 모아(qid 단위) 반환한다.
    min_amount: ①번 쿼리에서 사용할 최소 금액 (기본값: 10억원)
    min_count_account: ⑥번 쿼리에서 사용할 계정별 최소 횟수 (기본값: 3회)
    min_count_user: ⑦번 쿼리에서 사용할 사용자별 최소 횟수 (기본값: 3회)
    min_count_customer: ⑧번 쿼리에서 사용할 거래처별 최소 횟수 (기본값: 3회)
    """
    logger.info(
        "[run_jet_tests] start (years=%s, person_names=%s, person_birthdates=%s, min_amount=%.0f, min_count_account=%d, min_count_user=%d, min_count_customer=%d)",
        years,
        person_names,
        person_birthdates,
        min_amount,
        min_count_account,
        min_count_user,
        min_count_customer,
    )

    if not years:
        raise ValueError("JET 실행 대상 연도가 비어 있습니다.")

    debug_info: Dict[str, pd.DataFrame] = {}

    # 1) 입력된 주요 인물 정보 (연도와 무관) - 1회 저장
    if person_names:
        people_df = pd.DataFrame({"이름": person_names})
        if person_birthdates:
            dob_list = (person_birthdates + [""] * len(person_names))[
                : len(person_names)
            ]
            people_df["생년월일"] = dob_list
        debug_info["person_info"] = people_df

    # qid별로 연도 결과를 누적 후 concat
    results_acc: Dict[str, List[pd.DataFrame]] = {}
    # SQL은 qid 단위로 연도별 SQL을 주석과 함께 묶어서 보관
    queries: Dict[str, str] = {}

    # 연도별로 휴일 테이블 생성 후 JET 실행
    for year in years:
        logger.info("[run_jet_tests] processing year=%d", year)

        holiday_table = create_korean_holiday_table(conn, year)

        # 디버깅용 중간 테이블들 (연도 prefix)
        prefix = f"[{year}] "
        logger.info(
            "[run_jet_tests] building debug info for year=%d (prefix=%s)",
            year,
            prefix,
        )

        quarter_condition = (
            f"dt LIKE '{year}03%' OR dt LIKE '{year}06%' "
            f"OR dt LIKE '{year}09%' OR dt LIKE '{year}12%'"
        )

        debug_info[prefix + "holiday_all"] = pd.read_sql_query(
            f"SELECT dt FROM {holiday_table} ORDER BY dt",
            conn,
        )
        debug_info[prefix + "holiday_quarter"] = pd.read_sql_query(
            f"""
            SELECT dt
            FROM {holiday_table}
            WHERE {quarter_condition}
            ORDER BY dt
            """,
            conn,
        )
        debug_info[prefix + "journal_holiday_dates"] = pd.read_sql_query(
            f"""
            SELECT DISTINCT j.전기일 AS dt
            FROM {TABLE_NAME} j
            JOIN {holiday_table} h
              ON j.전기일 = h.dt
            ORDER BY dt
            """,
            conn,
        )

        # 연도별 JET 쿼리 실행
        year_queries = get_jet_queries(year, holiday_table, person_names=person_names, min_amount=min_amount, min_count_account=min_count_account, min_count_user=min_count_user, min_count_customer=min_count_customer)
        for qid, sql in year_queries.items():
            logger.info("[run_jet_tests] running query '%s' (year=%d)", qid, year)
            df = pd.read_sql_query(sql, conn)

            if qid not in results_acc:
                results_acc[qid] = []
            results_acc[qid].append(df)

            # SQL 누적 (연도별로 구분)
            combined_sql = queries.get(qid, "")
            if combined_sql:
                combined_sql += "\n\n"
            combined_sql += f"-- year={year}\n{sql.strip()}"
            queries[qid] = combined_sql

    # 연도별 누적분 concat
    results: Dict[str, pd.DataFrame] = {
        qid: pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]
        for qid, dfs in results_acc.items()
    }

    logger.info("[run_jet_tests] all queries finished (count=%d)", len(queries))

    return results, queries, debug_info
