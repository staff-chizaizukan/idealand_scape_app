import streamlit as st
import nomic
from nomic import AtlasDataset
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import sheet_module
import nomic_module

import re
import json

# ===================================
# 関数
# ===================================


# ===================================
# ページ設定
# ===================================
st.set_page_config(page_title="Nomic Map to Sheet", layout="wide")

# ===================================
# 初期ページ設定
# ===================================
if "page" not in st.session_state:
    st.session_state.page = "nomic"

# ===================================
# 初期変数（入力データの初期化）
# ===================================
default_state = {
    "nomic_api_token": st.secrets.get("NOMIC_TOKEN", ""),
    "nomic_domain": "atlas.nomic.ai",
    "nomic_map_url": "chizai-capcom-from-500",
    "output_sheet_url": "1pt9jeFguPEjw_aWGpHoGVPx4YV49Qp_ngURRK17926M",
    "output_sheet_name": "シート1",
    "design_sheet_id": "",
    "design_sheet_name": "",
    "setting_category_col": "",
    "novelty_score":"novelty_score",
    "feasibility_score":"feasibility_score",
    "marketability_score":"marketability_score"

}

for key, value in default_state.items():
    if key not in st.session_state:
        st.session_state[key] = value


# ===================================
# ヘッダー
# ===================================
logo_url = "https://prcdn.freetls.fastly.net/release_image/52909/36/52909-36-dd1d67cb4052a579b0c29e32c84fa9bf-2723x945.png?width=1950&height=1350&quality=85%2C65&format=jpeg&auto=webp&fit=bounds&bg-color=fff"

st.markdown(f"""
    <div class="header">
        <div class="header-left">
            <img src="{logo_url}" class="logo" alt="App Logo">
            <span class="title">Nomic Map to Sheet</span>
        </div>
    </div>
""", unsafe_allow_html=True)


# ===================================
# サイドメニュー
# ===================================
tabs = {
    "nomic": "Nomic",
    "output": "Output",
    "setting": "Setting"
}

spacer1, col1, spacer2, col2, spacer3 = st.columns([0.5, 1, 0.1, 3, 0.5])

with col1:
    st.markdown("<div class='side-menu'>", unsafe_allow_html=True)
    for key, label in tabs.items():
        if st.button(label, key=f"tab_{key}", use_container_width=True):
            st.session_state.page = key
    st.markdown("</div>", unsafe_allow_html=True)


# ===================================
# メインコンテンツ
# ===================================
with col2:
    st.markdown("<div class='content'>", unsafe_allow_html=True)
    page = st.session_state.page

    # ---- Nomicタブ ----
    if page == "nomic":
        st.markdown("<h2>Nomic</h2>", unsafe_allow_html=True)
        st.session_state.nomic_api_token = st.text_input("API Token", value=st.session_state.nomic_api_token)
        st.session_state.nomic_domain = st.text_input("Domain", value=st.session_state.nomic_domain)
        st.session_state.nomic_map_url = st.text_input("Map URL", value=st.session_state.nomic_map_url)

        if st.button("Download data"):
            # --- Nomicデータ取得 ---
            df_meta, df_topics, df_data, err = nomic_module.get_data(
                st.session_state.nomic_api_token,
                st.session_state.nomic_domain,
                st.session_state.nomic_map_url
            )

            if err or df_meta is None:
                st.error(f"❌ Failed to fetch Nomic data: {err}")
            else:
                st.success(f"✅ Data fetched successfully from '{st.session_state.nomic_map_url}'")

                # --- セッションに保存（再ダウンロード対応） ---
                st.session_state.df_meta = df_meta
                st.session_state.df_topics = df_topics
                st.session_state.df_data = df_data

        # --- ダウンロードボタン群 ---
        if (
            "df_meta" in st.session_state
            and st.session_state.df_meta is not None
            and "df_topics" in st.session_state
            and st.session_state.df_topics is not None
            and "df_data" in st.session_state
            and st.session_state.df_data is not None
        ):

            col1, col2, col3 = st.columns(3)

            with col1:
                st.download_button(
                    label="Meta CSV",
                    data=st.session_state.df_meta.to_csv(index=False).encode("utf-8-sig"),
                    file_name="meta.csv",
                    mime="text/csv",
                )

            with col2:
                st.download_button(
                    label="Topics CSV",
                    data=st.session_state.df_meta.to_csv(index=False).encode("utf-8-sig"),
                    file_name="topics.csv",
                    mime="text/csv",
                )

            with col3:
                st.download_button(
                    label="Data CSV",
                    data=st.session_state.df_meta.to_csv(index=False).encode("utf-8-sig"),
                    file_name="data.csv",
                    mime="text/csv",
                )

    # ---- Outputタブ ----
    elif page == "output":
        st.markdown("<h2>Output</h2>", unsafe_allow_html=True)
        service_email = "idealand-scape-app@idealand-scape.iam.gserviceaccount.com"

        st.markdown(
            f"""
            <div style="padding:12px; border-radius:8px; background:#333; margin-bottom:16px;">
                <p style="margin-bottom:8px;">
                    出力を行う前に、以下のメールアドレスをスプレッドシートの共有設定に追加し、<b>編集者権限</b>を付与してください。
                </p>
                <div style="display:flex; align-items:center; gap:8px;">
                    <input type="text" value="{service_email}" id="svcMail" readonly
                        style="flex:1; padding:6px 10px; border:1px solid #ccc; border-radius:6px; background:white;">
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.session_state.output_sheet_url = st.text_input("Sheet URL", value=st.session_state.output_sheet_url)
        st.session_state.output_sheet_name = st.text_input("Sheet Name", value=st.session_state.output_sheet_name)

        # Run button
        if st.button("Run Output"):
            # --- Nomicデータ取得 ---
            df_master, err = nomic_module.create_nomic_dataset(
                st.session_state.nomic_api_token,
                st.session_state.nomic_domain,
                st.session_state.nomic_map_url,
                st.session_state.novelty_score,
                st.session_state.feasibility_score,
                st.session_state.marketability_score
            )

            with open("./design/defalte.json", "r", encoding="utf-8") as f:
                style_config = json.load(f)

            if err or df_master is None:
                st.error(f"❌ Failed to fetch Nomic data: {err}")
            else:
                # --- Google Sheets 書き込み ---
                service_account_info = json.loads(st.secrets["google_service_account"]["value"])
                sheet_url, sheet_err = sheet_module.write_sheet(
                    st.session_state.output_sheet_url,
                    st.session_state.output_sheet_name,
                    service_account_info,
                    df_master,
                    style_config
                )

                if sheet_err:
                    st.error(f"❌ Failed to export to Google Sheets: {sheet_err}")
                else:
                    st.session_state.df_master = df_master
                    st.success(f"✅ Data exported to '{st.session_state.output_sheet_name or 'unspecified sheet'}'")


            # --- データプレビュー ---
        if "df_master" in st.session_state and st.session_state.df_master is not None:
            st.dataframe(st.session_state.df_master.head(20))

    elif page == "setting":
            st.markdown("<h2>Setting</h2>", unsafe_allow_html=True)
            st.session_state.novelty_score = st.text_input("Novelty score", value=st.session_state.novelty_score)
            st.session_state.feasibility_score = st.text_input("Feasibility score", value=st.session_state.feasibility_score)
            st.session_state.marketability_score = st.text_input("Marketability score", value=st.session_state.marketability_score)

# ===================================
# 外部CSSを読み込む
# ===================================
def local_css(file_name):
    with open(file_name, encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

local_css("style.css")
