import io
import zipfile
import gc  # 메모리 회수 유도용
import logging
import os

import pandas as pd
import streamlit as st

from jet_core import derive_years_from_db, load_csv_to_sqlite, run_jet_tests

# ================== 로거 설정 ==================


def setup_logger() -> logging.Logger:
    """
    jet_app 전역 로거 설정:
      - 콘솔(커맨드창) 출력
      - 파일: jet_app.log
    Streamlit가 스크립트를 여러 번 재실행해도
    핸들러가 중복으로 붙지 않도록 보호.
    """
    logger = logging.getLogger("jet_app")
    if logger.handlers:
        return logger  # 이미 설정됨

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )

    # 콘솔 핸들러
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # 파일 핸들러 (스크립트와 같은 폴더에 jet_app.log)
    log_path = os.path.join(os.path.dirname(__file__), "jet_app.log")
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    logger.info("=== JET Streamlit app logging initialized ===")
    logger.info("Log file: %s", log_path)

    return logger


logger = setup_logger()

# ================== Streamlit UI 기본 설정 ==================

st.set_page_config(page_title="JET (Journal Entry Test)", layout="wide")

st.title("📘 Journal Entry Test (JET) 자동 분석")

st.markdown(
    """
    1. **분개장 CSV(UTF-8)** 를 업로드합니다.  
    2. CSV에서 회계연도(없으면 전기일)를 자동으로 읽어 **모든 연도**에 대해 JET 쿼리(①~⑪)를 수행합니다.  
    3. 결과는 탭에서 확인하고, **엑셀(.xlsx)** 파일로 다운로드할 수 있습니다.  
    4. 탭 아래 **교집합 선택 체크박스**로 여러 테스트(2개 이상)를 선택한 뒤, **🔍 교집합 분석 실행** 버튼을 눌러야만 실제 교집합 계산이 시작됩니다.  
    5. 상단의 **대표이사/주요 인물 정보 입력**에 이름을 넣으면, 거래처명에 해당 이름이 포함된 분개를 **⑩ 주요인물_거래처명_매칭**으로 조회합니다.  
    6. 하단의 **🐞 디버깅 정보**에서 휴일 테이블·매칭 날짜 및 주요 인물 정보를 확인할 수 있습니다.
    """
)

# ================== 결과 출력 공통 함수 ==================


def render_results(results, queries, debug_info, jet_years: list[int]):
    """JET 결과 + 디버깅 로그를 화면에 그리는 공통 함수"""
    logger.info(
        "[render_results] rendering results for years=%s (result_sets=%d)",
        jet_years,
        len(results),
    )

    year_label = (
        f"{min(jet_years)}~{max(jet_years)}"
        if jet_years and len(set(jet_years)) > 1
        else (str(jet_years[0]) if jet_years else "N/A")
    )
    year_range_label = (
        f"{min(jet_years)}-{max(jet_years)}"
        if jet_years and len(set(jet_years)) > 1
        else (str(jet_years[0]) if jet_years else "N/A")
    )
    st.markdown(f"### 📅 분석 연도 범위 (모두 합산): **{year_label}**")

    # 전체 결과 ZIP (JET 실행 시 미리 만들어 둔 바이트 사용)
    with st.expander("📦 전체 테스트 결과 ZIP 다운로드 (.xlsx 묶음)"):
        zip_bytes = st.session_state.get("jet_zip_bytes")
        if zip_bytes is None:
            # 예외적으로 과거 세션에서 zip이 없는 경우: 안전하게 재생성
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                for qid, df in results.items():
                    xlsx_io = io.BytesIO()
                    with pd.ExcelWriter(xlsx_io, engine="openpyxl") as writer:
                        df.to_excel(writer, index=False, sheet_name="JET_RESULT")
                    xlsx_io.seek(0)
                    zf.writestr(f"{qid}.xlsx", xlsx_io.getvalue())
            zip_buffer.seek(0)
            zip_bytes = zip_buffer.getvalue()
            st.session_state["jet_zip_bytes"] = zip_bytes

        st.download_button(
            label="📥 전체 ZIP 다운로드 (.xlsx)",
            data=zip_bytes,
            file_name=f"JET_results_all_{year_range_label}.zip",
            mime="application/zip",
        )

    # --- JET 결과 탭 ---
    tab_labels = list(results.keys())
    tabs = st.tabs(tab_labels)

    for tab, qid in zip(tabs, tab_labels):
        with tab:
            st.subheader(f"결과: {qid}")

            df = results[qid]
            show_df = df.copy()
            show_df.index = range(1, len(show_df) + 1)

            if show_df.empty:
                st.info("해당 테스트 결과가 없습니다 (0건)")
            else:
                st.dataframe(show_df, use_container_width=True)
                
                # ④번 대차불일치_전표세트의 경우 전표번호 상세 조회 기능 추가
                if qid == "④ 대차불일치_전표세트" and "전표번호" in df.columns:
                    st.markdown("---")
                    st.markdown("#### 📋 전표번호별 상세 거래 내역 조회")
                    
                    # 전표번호 선택
                    voucher_numbers = df["전표번호"].astype(str).unique().tolist()
                    selected_voucher = st.selectbox(
                        "전표번호를 선택하세요:",
                        options=["선택하세요"] + sorted(voucher_numbers, key=lambda x: (len(x), x)),
                        key=f"voucher_select_{qid}",
                    )
                    
                    if selected_voucher and selected_voucher != "선택하세요":
                        # 미리 준비된 상세 데이터 사용
                        voucher_details = st.session_state.get("voucher_details", {})
                        detail_df = voucher_details.get(selected_voucher)
                        
                        if detail_df is not None and not detail_df.empty:
                            st.markdown(f"**전표번호: {selected_voucher}** 상세 내역 ({len(detail_df)}건)")
                            detail_show = detail_df.copy()
                            detail_show.index = range(1, len(detail_show) + 1)
                            st.dataframe(detail_show, use_container_width=True)
                        else:
                            st.info(f"전표번호 {selected_voucher}에 해당하는 거래가 없습니다.")

            with st.expander("🔍 사용된 SQL 보기 (연도별 실행 포함)"):
                st.code(
                    queries.get(qid, "-- 정의되지 않은 쿼리입니다."),
                    language="sql",
                )

    # ============================
    #  여러 테스트 교집합 보기
    # ============================
    intersection_container = st.container()
    with intersection_container:
        st.markdown("### 🔗 여러 테스트 교집합 보기")

        if len(results) >= 2:
            st.write(
                "교집합을 계산할 테스트를 선택한 뒤, "
                "**🔍 교집합 분석 실행** 버튼을 눌러주세요 "
                "(2개 이상, 여러 개 선택 가능)."
            )

            # 체크박스는 "선택 상태"만 담당 (연도 합산 기준 하나의 리스트)
            cols = st.columns(len(tab_labels))
            selected_flags = {}
            for col, qid in zip(cols, tab_labels):
                selected_flags[qid] = col.checkbox(
                    qid,
                    key=f"intersect_{qid}",
                )

            raw_selected_tests = [
                qid for qid, v in selected_flags.items() if v
            ]

            def _base_qid(qid: str) -> str:
                if qid.startswith("[") and "] " in qid:
                    return qid.split("] ", 1)[1]
                return qid

            # ④, ⑤(집계형) 테스트는 교집합 대상에서 제외
            EXCLUDE_FROM_INTERSECTION = ["④ 대차불일치_전표세트","⑤ 누락된_전표번호"]
            excluded_tests = [
                qid
                for qid in raw_selected_tests
                if _base_qid(qid) in EXCLUDE_FROM_INTERSECTION
            ]
            selected_tests = [
                qid
                for qid in raw_selected_tests
                if _base_qid(qid) not in EXCLUDE_FROM_INTERSECTION
            ]

            if excluded_tests:
                st.info(
                    "다음 테스트는 **전표 집계 수준 결과**이므로 "
                    "교집합 분석 대상에서 제외했습니다: "
                    + ", ".join(excluded_tests)
                )

            # 버튼을 눌렀을 때만 실제 교집합 계산
            analyze_btn = st.button("🔍 교집합 분석 실행")

            if analyze_btn:
                if len(selected_tests) < 2:
                    st.warning(
                        "교집합 계산을 위해서는 **2개 이상의 테스트**를 선택해야 합니다.\n"
                        "(전표 집계 수준 테스트는 자동으로 제외됩니다.)"
                    )
                    st.session_state["intersection_result"] = None
                    st.session_state["intersection_selected_tests"] = []
                else:
                    # 교집합 전용 상황 안내 (이 구간만 잠깐 로딩)
                    inter_progress = st.progress(0)
                    inter_status = st.empty()

                    def inter_update(step: int, total: int, msg: str):
                        ratio = max(0, min(1, step / total))
                        inter_progress.progress(ratio)
                        inter_status.write(f"**[{step}/{total}] {msg}**")

                    total_steps = 3
                    inter_update(
                        1, total_steps, "선택된 테스트 목록을 확인하는 중입니다..."
                    )

                    # 각 선택된 결과 DataFrame 복사 (원본 손상 방지)
                    dfs = [results[qid].copy() for qid in selected_tests]

                    # ✅ 전체 행 데이터 기준으로 교집합 계산
                    inter_update(
                        2,
                        total_steps,
                        "공통 컬럼을 확인하고 전체 행 데이터 기준으로 교집합을 계산하는 중입니다...",
                    )

                    # 모든 DataFrame의 공통 컬럼 자동 찾기
                    if not dfs:
                        key_cols = []
                    else:
                        # 첫 번째 DataFrame의 컬럼부터 시작
                        key_cols = list(dfs[0].columns)
                        # 나머지 DataFrame들과 공통 컬럼만 유지
                        for df in dfs[1:]:
                            key_cols = [col for col in key_cols if col in df.columns]

                    if not key_cols:
                        inter_progress.empty()
                        inter_status.empty()
                        st.warning(
                            "선택된 테스트들에 공통으로 존재하는 컬럼이 없습니다.\n"
                            "교집합을 계산할 수 없습니다."
                        )
                        st.session_state["intersection_result"] = None
                        st.session_state[
                            "intersection_selected_tests"
                        ] = selected_tests
                    else:
                        # 3단계: 실제 교집합 계산 (전체 행 데이터 기준)
                        inter_update(
                            3,
                            total_steps,
                            "전체 행 데이터 기준으로 교집합 행을 계산하는 중입니다...",
                        )

                        # dtype 통일: 공통 컬럼은 모두 문자열(str)로 변환하여 비교
                        for i in range(len(dfs)):
                            for col in key_cols:
                                if col in dfs[i].columns:
                                    dfs[i][col] = dfs[i][col].astype(str).fillna("")

                        # 공통 컬럼 기준으로 중복 제거 후 교집합 계산
                        # key_cols는 이미 첫 번째 DataFrame의 원본 컬럼 순서를 유지하고 있음
                        
                        inter_df = dfs[0][key_cols].drop_duplicates()
                        for df in dfs[1:]:
                            inter_df = inter_df.merge(
                                df[key_cols].drop_duplicates(),
                                on=key_cols,
                                how="inner",
                            )

                        inter_df = inter_df.drop_duplicates()
                        
                        # 결과 DataFrame의 컬럼 순서를 첫 번째 테스트 결과와 동일하게 정렬
                        # (merge 후에도 원본 순서 유지)
                        inter_df = inter_df[key_cols]

                        inter_progress.empty()
                        inter_status.empty()

                        st.session_state["intersection_result"] = inter_df
                        st.session_state[
                            "intersection_selected_tests"
                        ] = selected_tests

                        st.success(
                            "✅ 교집합 계산이 완료되었습니다. "
                            f"(전체 행 데이터 기준, 선택된 테스트: {', '.join(selected_tests)})"
                        )

            # --- 마지막으로 계산된 교집합 결과 표시 ---
            inter_df = st.session_state.get("intersection_result")
            last_selected = st.session_state.get(
                "intersection_selected_tests", []
            )

            if inter_df is not None and last_selected:
                st.write(
                    f"마지막으로 분석한 교집합 "
                    f"({', '.join(last_selected)}) 결과: "
                    f"**{len(inter_df)}건**"
                )

                if inter_df.empty:
                    st.info("교집합 결과가 없습니다.")
                else:
                    show_inter = inter_df.copy()
                    show_inter.index = range(1, len(show_inter) + 1)
                    st.dataframe(show_inter, use_container_width=True)
            else:
                st.info(
                    "교집합을 보려면 테스트를 선택한 뒤 "
                    "**'🔍 교집합 분석 실행'** 버튼을 클릭하세요."
                )
        else:
            st.info("교집합 계산을 위해서는 2개 이상의 테스트 결과가 필요합니다.")

    # --- 디버깅 정보 ---
    with st.expander("🐞 디버깅용 로그 / 중간 데이터 확인", expanded=False):
        if not debug_info:
            st.info("디버깅 데이터가 없습니다.")
        else:
            for key in sorted(debug_info.keys()):
                df = debug_info[key].copy()
                df.index = range(1, len(df) + 1)
                st.markdown(f"#### {key}")
                st.dataframe(df, use_container_width=True)

    #  여기까지 오면 결과 렌더링이 모두 끝난 상태이므로,
    #    JET 실행 중에 띄웠던 상단 진행바/상태 텍스트를 제거한다.
    pb = st.session_state.get("jet_progress_bar")
    stx = st.session_state.get("jet_status_text")
    if pb is not None:
        try:
            pb.empty()
        except Exception:
            pass
        st.session_state["jet_progress_bar"] = None
    if stx is not None:
        try:
            stx.empty()
        except Exception:
            pass
        st.session_state["jet_status_text"] = None


# ================== 입력 UI ==================

uploaded_file = st.file_uploader("분개장 CSV 파일을 업로드하세요", type=["csv"])

# ①번 쿼리 최소 금액 입력
min_amount = st.number_input(
    "①번 테스트 최소 금액 입력 (원 단위)",
    min_value=0.0,
    value=1000000000.0,
    step=10000000.0,
    format="%.0f",
    help="①번 테스트(휴일 분기말 지정금액 이상 분개)에서 사용할 최소 금액을 입력하세요. 기본값: 10억원",
)

# 대표이사 / 주요 인물 정보 입력 (동적 + 버튼)
if "person_rows" not in st.session_state:
    st.session_state.person_rows = 1

with st.expander("👤 대표이사 / 주요 인물 정보 입력 (선택 사항)", expanded=False):
    st.markdown(
        """
        분석 대상 회사의 **대표이사·주요 인물 이름 및 생년월일**을 입력하면,  
        거래처명에 해당 인물명이 포함된 분개를 **⑩ 주요인물_거래처명_매칭** 테스트로 조회합니다.  
        필요 시 **➕ 인물 추가** 버튼으로 인원을 늘릴 수 있습니다.
        """
    )

    add_col, _ = st.columns([1, 3])
    if add_col.button("➕ 인물 추가", key="add_person_row"):
        st.session_state.person_rows += 1

    person_names = []
    person_dobs = []

    for i in range(st.session_state.person_rows):
        col1, col2 = st.columns(2)
        name = col1.text_input(f"이름 {i+1}", key=f"person_name_{i}")
        dob = col2.text_input(
            f"생년월일 {i+1} (YYYYMMDD, 선택)", key=f"person_dob_{i}"
        )

        if name.strip():
            person_names.append(name.strip())
            if dob.strip():
                person_dobs.append(dob.strip())

run_button = st.button("🚀 JET 실행")

# ================== 실행 버튼 클릭 시 (상황 안내 포함) ==================

if run_button:
    if uploaded_file is None:
        st.warning("먼저 CSV 파일을 업로드해주세요.")
        logger.warning("[UI] run_button clicked without file upload")
    else:
        # 진행률 바 + 상태 텍스트
        progress_bar = st.progress(0)
        status_text = st.empty()

        # 렌더링 완료 후에 지우기 위해 session_state에 보관
        st.session_state["jet_progress_bar"] = progress_bar
        st.session_state["jet_status_text"] = status_text

        total_steps = 7  # 연도 파생 및 ZIP/엑셀 사전 생성 단계까지 포함
        current_step = 0

        def update_progress(step: int, message: str):
            ratio = step / total_steps
            ratio = max(0, min(1, ratio))
            progress_bar.progress(ratio)
            status_text.write(f"**[{step}/{total_steps}] {message}**")

        file_name = getattr(uploaded_file, "name", "unknown")
        file_size = getattr(uploaded_file, "size", "unknown")

        logger.info(
            "[UI] JET run requested (file=%s, size=%s bytes, person_names=%s)",
            file_name,
            file_size,
            person_names,
        )

        with st.spinner(
            "⚙️ CSV 분석부터 휴일 테이블 생성, JET 테스트 실행, "
            "엑셀/ZIP 생성 및 화면 표시 준비까지 처리 중입니다..."
        ):
            conn = None

            try:
                # 1단계: CSV → SQLite 적재
                current_step = 1
                update_progress(
                    current_step,
                    "CSV 파일 로딩 및 전처리, SQLite 메모리 DB 적재 중입니다...",
                )
                logger.info("[UI] Step 1: load_csv_to_sqlite 시작")
                conn, summary = load_csv_to_sqlite(
                    uploaded_file, encoding="utf-8-sig"
                )
                logger.info(
                    "[UI] Step 1 완료: rows=%s, cols=%s, missing_required=%s, errors=%s",
                    summary.get("total_rows"),
                    summary.get("total_cols"),
                    summary.get("missing_required_columns"),
                    summary.get("errors"),
                )

                # 2단계: CSV 요약/검증 정보 표시
                current_step = 2
                update_progress(
                    current_step,
                    "CSV 구조 검증 및 요약 정보를 화면에 표시하는 중입니다...",
                )
                with st.expander("📊 CSV 검증 및 요약 정보", expanded=True):
                    st.write(f"- 총 행(row) 수: **{summary['total_rows']}**")
                    st.write(f"- 총 열(column) 수: **{summary['total_cols']}**")

                    st.write("### 컬럼 목록")
                    st.write(", ".join(summary["columns"]))

                    st.write("### 필수 컬럼별 NULL 개수 (정보용)")
                    null_df = pd.DataFrame(
                        list(summary["null_counts"].items()),
                        columns=["컬럼명", "NULL 개수"],
                    )
                    null_df.index = range(1, len(null_df) + 1)
                    st.dataframe(null_df, use_container_width=True)

                # 3단계: CSV 구조 오류 점검
                current_step = 3
                update_progress(
                    current_step,
                    "CSV 구조에 치명적인 오류가 있는지 점검하는 중입니다...",
                )
                if summary.get("errors"):
                    logger.error(
                        "[UI] CSV 구조 오류로 JET 실행 중단: %s",
                        " | ".join(summary["errors"]),
                    )
                    st.error(
                        "CSV 구조 오류로 JET 실행이 중단되었습니다:\n\n"
                        + "\n".join(f"- {msg}" for msg in summary["errors"])
                    )
                    status_text.write(
                        "❌ CSV 구조 오류로 인해 JET 실행이 중단되었습니다."
                    )
                    # 오류 시에는 안내바 제거 후 즉시 종료
                    try:
                        progress_bar.empty()
                        status_text.empty()
                    except Exception:
                        pass
                    st.session_state["jet_progress_bar"] = None
                    st.session_state["jet_status_text"] = None

                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
                    gc.collect()
                    st.stop()

                # 4단계: 분석 대상 연도 파생
                current_step = 4
                update_progress(
                    current_step,
                    "CSV에서 분석 대상 연도(회계연도 또는 전기일 기준)를 계산하는 중입니다...",
                )
                try:
                    detected_years = derive_years_from_db(conn)
                except Exception as e:
                    logger.exception("[UI] 연도 파생 실패")
                    st.error(f"연도 정보를 추출할 수 없습니다: {e}")
                    status_text.write("❌ 연도 정보를 추출할 수 없습니다.")
                    try:
                        progress_bar.empty()
                        status_text.empty()
                    except Exception:
                        pass
                    st.session_state["jet_progress_bar"] = None
                    st.session_state["jet_status_text"] = None
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
                    gc.collect()
                    st.stop()

                st.info(
                    f"분석 대상 연도 범위: {min(detected_years)}~{max(detected_years)} "
                    f"(총 {len(detected_years)}개 연도)"
                )

                # 5단계: JET 실행 (휴일테이블 + 쿼리 + 디버깅 로그)
                current_step = 5
                update_progress(
                    current_step,
                    "휴일(토/일+공휴일) 테이블 생성 및 JET 쿼리, "
                    "디버깅용 로그를 생성하는 중입니다...",
                )
                logger.info(
                    "[UI] Step 5: run_jet_tests 시작 (years=%s, person_names=%s, min_amount=%.0f)",
                    detected_years,
                    person_names,
                    min_amount,
                )
                results, queries, debug_info = run_jet_tests(
                    conn,
                    detected_years,
                    person_names or None,
                    person_dobs or None,
                    min_amount=min_amount,
                )
                logger.info(
                    "[UI] Step 5 완료: result_sets=%d",
                    len(results) if results is not None else 0,
                )

                # 6단계: 결과 ZIP 사전 생성
                current_step = 6
                update_progress(
                    current_step,
                    "전체 ZIP 파일을 생성하는 중입니다...",
                )
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(
                    zip_buffer, "w", zipfile.ZIP_DEFLATED
                ) as zf:
                    for qid, df in results.items():
                        buf = io.BytesIO()
                        with pd.ExcelWriter(
                            buf, engine="openpyxl"
                        ) as writer:
                            df.to_excel(
                                writer, index=False, sheet_name="JET_RESULT"
                            )
                        buf.seek(0)
                        data_bytes = buf.getvalue()
                        zf.writestr(f"{qid}.xlsx", data_bytes)
                zip_buffer.seek(0)

                # ④번 결과의 전표번호별 상세 데이터 미리 준비
                voucher_details = {}
                if "④ 대차불일치_전표세트" in results:
                    mismatch_df = results["④ 대차불일치_전표세트"]
                    if not mismatch_df.empty and "전표번호" in mismatch_df.columns:
                        logger.info("[UI] ④번 전표번호별 상세 데이터 준비 중...")
                        for voucher_num in mismatch_df["전표번호"].astype(str).unique():
                            try:
                                detail_query = f"""
                                    SELECT *
                                    FROM journal_entries
                                    WHERE 전표번호 = ?
                                    ORDER BY 전표라인번호
                                """
                                detail_df = pd.read_sql_query(
                                    detail_query, conn, params=(voucher_num,)
                                )
                                voucher_details[str(voucher_num)] = detail_df
                            except Exception as e:
                                logger.warning(
                                    f"[UI] 전표번호 {voucher_num} 상세 조회 실패: {e}"
                                )
                        logger.info(
                            f"[UI] ④번 전표번호별 상세 데이터 준비 완료: {len(voucher_details)}개"
                        )

                # session_state에 저장
                st.session_state["jet_results"] = results
                st.session_state["jet_queries"] = queries
                st.session_state["jet_debug_info"] = debug_info
                st.session_state["jet_years"] = detected_years
                st.session_state["jet_zip_bytes"] = zip_buffer.getvalue()
                st.session_state["voucher_details"] = voucher_details

                # 이전 교집합 결과 초기화
                st.session_state["intersection_result"] = None
                st.session_state["intersection_selected_tests"] = []

                # 7단계: 표시 준비 완료
                current_step = 7
                update_progress(
                    current_step,
                    "모든 분석 및 파일 생성이 완료되었습니다. "
                    "잠시 후 아래 결과 탭과 교집합 영역에 데이터가 표시됩니다.",
                )

            except Exception as e:
                status_text.write("❌ 처리 중 오류가 발생했습니다.")
                st.error(f"처리 중 오류가 발생했습니다: {e}")
                logger.exception("[UI] 처리 중 예외 발생")

                # 예외 시에도 진행바/상태 텍스트 제거
                try:
                    progress_bar.empty()
                    status_text.empty()
                except Exception:
                    pass
                st.session_state["jet_progress_bar"] = None
                st.session_state["jet_status_text"] = None

            finally:
                # conn은 session_state에 저장되어 있으므로 닫지 않음
                # (④번 전표번호 상세 조회에 필요)
                # 새로운 JET 실행 시에는 새로운 conn이 생성됨
                gc.collect()

                logger.info(
                    "[UI] JET run finished (file=%s)", file_name
                )

            st.success(
                "✅ JET 테스트가 완료되었습니다! "
                "아래 탭과 교집합 보기 영역에서 결과를 확인하세요."
            )

# ================== 저장된 결과가 있으면 항상 렌더링 ==================

if "jet_results" in st.session_state:
    render_results(
        st.session_state["jet_results"],
        st.session_state["jet_queries"],
        st.session_state["jet_debug_info"],
        st.session_state.get("jet_years", []),
    )
else:
    st.info("CSV 업로드 후 **JET 실행** 버튼을 눌러주세요.")
